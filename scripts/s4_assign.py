"""
ÉTAPE 4 - Fankai Episode Assignor
===================================
Lit les torrents déjà matchés (data/torrents_matched.json), construit les
structures de séries depuis l'API et place chaque torrent dans les bons
épisodes. Détecte et remonte les packs intégraux / saisons.

Input  : data/torrents_matched.json, data/torrents_unmatched.json
Output : series/{id}.json, data/unmatched_torrents.json
"""

import re
import json
import time
import unicodedata
import requests
from pathlib import Path
from collections import Counter

METADATA_BASE  = "https://metadata.fankai.fr"
MATCHED_FILE   = "data/torrents_matched.json"
UNMATCHED_FILE = "data/torrents_unmatched.json"   # produit par s3_match.py
REPORT_FILE    = "data/unmatched_torrents.json"   # rapport final (score + assign)
OUTPUT_DIR     = Path("series")
DELAY          = 0.3

# ── Épisodes à ignorer (erreurs API, doublons, etc.) ─────────────────────────
# Ajouter ici les IDs d'épisodes à exclure de toute assignation.
IGNORED_EPISODE_IDS: set[int] = {
    1270,  # erreur API temporaire
}

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
API_CACHE_DIR = Path("data/api_cache")
API_CACHE_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "fankarr-matcher/1.0"})


# ── API avec cache ────────────────────────────────────────────────────────────

def _cache_key(url):
    key = re.sub(r"https?://[^/]+", "", url)
    key = re.sub(r"[^\w/]", "_", key).strip("_/")
    return API_CACHE_DIR / (key + ".json")

def api_get(url, force=False):
    cache_file = _cache_key(url)
    if not force and cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))
    try:
        r = SESSION.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        time.sleep(DELAY)
        return data
    except Exception as e:
        print(f"  [!] GET {url} → {e}")
        return None


# ── Construction des structures de séries ─────────────────────────────────────

def fetch_seasons(serie_id):
    data = api_get(f"{METADATA_BASE}/series/{serie_id}/seasons")
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for key in ("seasons", "data", "results"):
            if isinstance(data.get(key), list): return data[key]
    return []

def fetch_episodes(season_id):
    data = api_get(f"{METADATA_BASE}/seasons/{season_id}/episodes")
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for key in ("episodes", "data", "results"):
            if isinstance(data.get(key), list): return data[key]
    return []

def fix_encoding(s):
    if not s or not isinstance(s, str): return s
    try: return s.encode('cp1252').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        try: return s.encode('latin-1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError): return s

def build_structure(serie, seasons_raw):
    seasons_raw = [s for s in (seasons_raw or []) if isinstance(s, dict)]
    seasons_out = []
    for season in sorted(seasons_raw, key=lambda s: s.get("season_number", 0)):
        eps_raw = [e for e in fetch_episodes(season["id"]) if isinstance(e, dict)]
        time.sleep(DELAY)
        episodes_out = [
            {
                "id":                ep["id"],
                "episode_number":    ep.get("episode_number"),
                "title":             fix_encoding(ep.get("title")),
                "aired":             ep.get("aired") or None,
                "original_filename": ep.get("original_filename"),
                "formatted_name":    ep.get("formatted_name"),
                "nfo_filename":      ep.get("nfo_filename"),
                "torrents":          [],
                "paths":             [],
            }
            for ep in sorted(eps_raw, key=lambda e: e.get("episode_number", 0))
            if ep.get("id") not in IGNORED_EPISODE_IDS
        ]
        seasons_out.append({
            "id":            season["id"],
            "season_number": season.get("season_number"),
            "title":         fix_encoding(season.get("title") or season.get("name")),
            "torrents":      [],
            "episodes":      episodes_out,
        })
    return {
        "id":         serie["id"],
        "title":      fix_encoding(serie.get("title")),
        "show_title": fix_encoding(serie.get("show_title")),
        "torrents":   [],
        "seasons":    seasons_out,
    }

def make_ref(t):
    return {
        "nyaa_id":      t.get("nyaa_id"),
        "nyaa_url":     t.get("nyaa_url"),
        "title":        t.get("title"),
        "torrent_name": t.get("torrent_name"),
        "torrent_url":  t.get("torrent_url"),
        "magnet":       t.get("magnet"),
        "infohash":     t.get("infohash"),
        "size":         t.get("size"),
        "pub_date":     t.get("pub_date"),
        "seeders":      t.get("seeders"),
        "fankai":       t.get("fankai", True),
    }


# ── Index des fichiers du torrent (chemin → position) ────────────────────────

def build_file_index(torrent: dict) -> dict:
    """Retourne {chemin → index_réel 0-based dans le .torrent}.

    Préfère torrent["file_indices"] (ordre original du .torrent, produit par
    s2_enrich / manual_add). Si absent, fallback sur la position dans files[]
    (qui peut être alphabétique → index approximatif).
    Les deux variantes de séparateur (/ et \\) sont stockées pour un lookup robuste.
    """
    source = torrent.get("file_indices") or {}
    if not source:
        # fallback : position dans la liste triée
        source = {f: i for i, f in enumerate(torrent.get("files") or [])}
    result = {}
    for path, idx in source.items():
        result[path] = idx
        result[path.replace("\\", "/")] = idx
        result[path.replace("/", "\\")] = idx
    return result

def file_index_of(path: str | None, file_index: dict) -> int | None:
    """Retourne l'index du fichier dans le torrent, ou None si introuvable."""
    if not path or not file_index:
        return None
    return file_index.get(path)


# ── Index de chemins depuis les fichiers du torrent ───────────────────────────

_VIDEO_EXT = {'.mkv', '.mp4', '.avi', '.m4v', '.mov'}

def _extract_ep_video(fname):
    if Path(fname).suffix.lower() not in _VIDEO_EXT: return None
    stem = Path(fname).stem
    stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()
    if re.search(r'\b\d+[,.]\d+\b', stem): return None
    if re.search(r'\bbonus\b', stem, re.IGNORECASE): return None
    m = re.search(r'\bS\d{1,2}E(\d{2,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b\d{1,2}x(\d{2,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b(?:henshu|henshū|henshû|hensh|kaï|kai|yabai|yabaï|kyodai|film)\s+(\d{1,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b(\d{1,3})\s*\(fan-?ka[iï]\)', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\(fan-?ka[iï]\)\s+(\d{1,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'[-–]\s*0*(\d{1,3})\s*[-–]', stem)
    if m:
        num = int(m.group(1))
        if num <= 999: return num
    return None

def _stem_title(s):
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(w for w in s.split() if len(w) >= 3)

def _norm_folder(s):
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\b(?:partie|part|saison|season|arc)\s*(\d+)\b", r"\1", s, flags=re.IGNORECASE)
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).lower().strip()

def build_ep_path_index(torrent):
    ep_numbers = torrent.get("ep_numbers") or []
    raw_index  = {}
    for f in torrent.get("files") or []:
        num = _extract_ep_video(re.split(r'[/\\]', f)[-1])
        if num is not None and num not in raw_index:
            raw_index[num] = f
    video_files = sorted([
        f for f in (torrent.get("files") or [])
        if Path(re.split(r'[/\\]', f)[-1]).suffix.lower() in _VIDEO_EXT
        and _extract_ep_video(re.split(r'[/\\]', f)[-1]) is not None
        and not re.split(r'[/\\]', f)[-1].lower().endswith(('.png', '.jpg', '.nfo', '.zip'))
    ])
    if ep_numbers and len(ep_numbers) == len(video_files):
        return {ep_num: f for ep_num, f in zip(sorted(ep_numbers), video_files)}
    return raw_index

def build_title_path_index(torrent):
    result = []
    _NOISE_PARTS = {'film', 'bonus', 'ova', 'special', 'sp'}
    for f in torrent.get("files") or []:
        fname = re.split(r'[/\\]', f)[-1]
        if Path(fname).suffix.lower() not in _VIDEO_EXT: continue
        stem = Path(fname).stem
        stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()
        stem = re.sub(r'[-–]?\s*\d{3,4}p.*$', '', stem, flags=re.IGNORECASE).strip()
        stem = re.sub(r'\s*[-–]\s*(?:dvd|blu.?ray|bdrip|web|hdtv|multi).*$', '', stem, flags=re.IGNORECASE).strip()
        parts = re.split(r'\s*[-–]\s*', stem)
        title_part = None
        for part in reversed(parts):
            p = part.strip()
            if p and not re.fullmatch(r'[\d,.\s]+', p) and p.lower() not in _NOISE_PARTS:
                title_part = p; break
        if not title_part: continue
        title_norm = _stem_title(title_part)
        if title_norm: result.append((title_norm, f))
    return result

def build_folder_ep_index(torrent):
    index = {}
    for f in torrent.get("files") or []:
        parts = re.split(r'[/\\]', f)
        if len(parts) < 2: continue
        if Path(parts[-1]).suffix.lower() not in _VIDEO_EXT: continue
        folder_key = _norm_folder(parts[-2])
        ep_num     = _extract_ep_video(parts[-1])
        if ep_num is None: continue
        if folder_key not in index: index[folder_key] = {}
        if ep_num not in index[folder_key]: index[folder_key][ep_num] = f
    return index

def find_best_folder(season_title, folder_index):
    season_norm   = _norm_folder(season_title)
    season_tokens = set(season_norm.split())
    if not season_tokens: return None
    best_key, best_score = None, 0
    for folder_key in folder_index:
        folder_tokens = set(folder_key.split())
        if not folder_tokens: continue
        inter = len(season_tokens & folder_tokens)
        score = inter / max(len(season_tokens), len(folder_tokens))
        if score > best_score: best_score = score; best_key = folder_key
    return best_key if best_score >= 0.3 else None

def match_title_to_path(ep_title, title_path_index):
    ep_norm   = _stem_title(ep_title)
    if not ep_norm: return None
    ep_tokens = set(ep_norm.split())
    best_path, best_score = None, 0
    for title_norm, fpath in title_path_index:
        file_tokens = set(title_norm.split())
        if not file_tokens: continue
        inter = ep_tokens & file_tokens
        score = len(inter) / max(len(ep_tokens), len(file_tokens))
        if score > best_score: best_score = score; best_path = fpath
    return best_path if best_score >= 0.5 else None


# ── Assignment d'un torrent dans une structure ────────────────────────────────

def _torrent_title_to_path(torrent_title):
    if not torrent_title: return None
    t = re.sub(r'^\[[^\]]*\]\s*', '', torrent_title).strip()
    t = re.sub(r'\s*[-–]\s*\d{3,4}p.*$', '', t, flags=re.IGNORECASE).strip()
    t = re.sub(r'\s*\(.*?\)\s*$', '', t).strip()
    return t + ".mkv" if t else None

def _compute_path(ep, n, is_specials, folder_key, season_path_idx, path_idx, title_path_idx, nb_seasons, strict=False):
    if is_specials:
        return match_title_to_path(ep.get("title", ""), title_path_idx)
    if season_path_idx.get(n):
        return season_path_idx[n]
    if folder_key is not None:
        return path_idx.get(n) or match_title_to_path(ep.get("title", ""), title_path_idx)
    if nb_seasons <= 1:
        return path_idx.get(n) or match_title_to_path(ep.get("title", ""), title_path_idx)
    if path_idx.get(n):
        return path_idx[n]
    if strict:
        return None
    return match_title_to_path(ep.get("title", ""), title_path_idx)

def _add_torrent_to_structure(structure, ref):
    existing = {t.get("nyaa_id") or t.get("infohash") for t in structure["torrents"]}
    key = ref.get("nyaa_id") or ref.get("infohash")
    if key not in existing:
        structure["torrents"].append(ref)

def assign(structure, torrent, ttype):
    ref          = make_ref(torrent)
    t            = ttype["type"]
    fidx         = build_file_index(torrent)   # chemin → index dans torrent["files"]
    ihash        = ref.get("infohash")

    def _path_obj(p):
        return {"infohash": ihash, "path": p, "file_index": file_index_of(p, fidx)}

    if t == "integral":
        _add_torrent_to_structure(structure, ref)
        return True

    if t == "season":
        season_num      = ttype.get("season")
        ep_numbers_list = torrent.get("ep_numbers") or []
        path_idx        = build_ep_path_index(torrent)
        title_path_idx  = build_title_path_index(torrent)
        folder_idx      = build_folder_ep_index(torrent)
        nb_seasons_loc  = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])
        assigned = False
        for season in structure["seasons"]:
            sn = season.get("season_number", 0)
            if season_num is not None and sn != 0 and sn != season_num:
                continue
            is_specials     = sn == 0
            folder_key      = find_best_folder(season.get("title", ""), folder_idx)
            season_path_idx = folder_idx.get(folder_key, {}) if folder_key else {}
            for ep in season["episodes"]:
                n = ep.get("episode_number")
                if ep_numbers_list and n not in ep_numbers_list:
                    continue
                existing = {t2.get("nyaa_id") or t2.get("infohash") for t2 in ep["torrents"]}
                if ihash not in existing and (ref.get("nyaa_id") or ihash) not in existing:
                    ep["torrents"].append(ref)
                    p = _compute_path(ep, n, is_specials, folder_key, season_path_idx,
                                      path_idx, title_path_idx, nb_seasons_loc, strict=True)
                    if p is None:
                        p = _torrent_title_to_path(torrent.get("title"))
                    ep["paths"].append(_path_obj(p))
                assigned = True
        return assigned

    nb_seasons = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])

    if t == "episode_range":
        ep_from        = ttype["ep_from"]; ep_to = ttype["ep_to"]
        path_idx       = build_ep_path_index(torrent)
        title_path_idx = build_title_path_index(torrent)
        folder_idx     = build_folder_ep_index(torrent)
        assigned = False
        for season in structure["seasons"]:
            is_specials     = season.get("season_number") == 0
            folder_key      = find_best_folder(season.get("title", ""), folder_idx)
            season_path_idx = folder_idx.get(folder_key, {}) if folder_key else {}
            for ep in season["episodes"]:
                n = ep.get("episode_number")
                if n is not None and ep_from <= n <= ep_to:
                    existing = {t2.get("nyaa_id") or t2.get("infohash") for t2 in ep["torrents"]}
                    if ihash not in existing and (ref.get("nyaa_id") or ihash) not in existing:
                        ep["torrents"].append(ref)
                        p = _compute_path(ep, n, is_specials, folder_key, season_path_idx,
                                          path_idx, title_path_idx, nb_seasons, strict=True)
                        if p is None:
                            p = _torrent_title_to_path(torrent.get("title"))
                        ep["paths"].append(_path_obj(p))
                    assigned = True
        return assigned

    if t == "episode":
        ep_num = ttype["episode"]
        if ep_num is None: return False
        path_idx       = build_ep_path_index(torrent)
        title_path_idx = build_title_path_index(torrent)
        folder_idx     = build_folder_ep_index(torrent)
        force_season   = torrent.get("force_season")
        force_path     = torrent.get("force_path")
        if force_season is None and ttype.get("season") == 0:
            force_season = 0
        for season in structure["seasons"]:
            sn = season.get("season_number", 0)
            if force_season is not None and sn != force_season:
                continue
            if force_season is None and sn == 0:
                continue
            is_specials     = sn == 0
            folder_key      = find_best_folder(season.get("title", ""), folder_idx)
            season_path_idx = folder_idx.get(folder_key, {}) if folder_key else {}
            for ep in season["episodes"]:
                if ep.get("episode_number") == ep_num:
                    existing = {t2.get("nyaa_id") or t2.get("infohash") for t2 in ep["torrents"]}
                    if ihash not in existing and (ref.get("nyaa_id") or ihash) not in existing:
                        ep["torrents"].append(ref)
                        if force_path:
                            p = force_path
                        else:
                            p = _compute_path(ep, ep_num, is_specials, folder_key, season_path_idx,
                                              path_idx, title_path_idx, nb_seasons, strict=True)
                            if p is None:
                                p = _torrent_title_to_path(torrent.get("title"))
                        ep["paths"].append(_path_obj(p))
                    return True
        return False

    return False


# ── Consolidation des packs ───────────────────────────────────────────────────

def _torrent_key(t):
    return t.get("nyaa_id") or t.get("infohash")

def _populate_paths_from_torrent(structure, pack_raw, append=False):
    path_idx        = build_ep_path_index(pack_raw)
    title_path_idx  = build_title_path_index(pack_raw)
    folder_idx_c    = build_folder_ep_index(pack_raw)
    nb_seasons      = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])
    pack_infohash   = pack_raw.get("infohash")
    fidx            = build_file_index(pack_raw)

    def _mk(p, ih=None):
        return {"infohash": ih or pack_infohash, "path": p, "file_index": file_index_of(p, fidx)}

    for season in structure["seasons"]:
        is_specials     = season.get("season_number") == 0
        folder_key      = find_best_folder(season.get("title", ""), folder_idx_c)
        season_path_idx = folder_idx_c.get(folder_key, {}) if folder_key else {}
        for ep in season["episodes"]:
            n = ep.get("episode_number")
            def _get_path():
                if is_specials:
                    return match_title_to_path(ep.get("title", ""), title_path_idx)
                if season_path_idx.get(n):
                    return season_path_idx[n]
                if path_idx.get(n):
                    return path_idx[n]
                if path_idx:
                    return None
                return match_title_to_path(ep.get("title", ""), title_path_idx)
            p = _get_path()
            if append:
                if p:
                    ep["paths"].append(_mk(p))
            elif ep["torrents"]:
                ep["paths"] = [_mk(p, t2.get("infohash")) for t2 in ep["torrents"]]
            else:
                ep["paths"] = [_mk(p)] if p else []

def consolidate(structures, torrents_by_id, torrents_by_infohash):
    """Remonte les packs intégraux et saisons détectés en haut de la structure."""
    consolidated = 0

    for structure in structures.values():

        # 0. Même torrent sur toutes les saisons → structure["torrents"]
        season_torrents = [s["torrents"][0] for s in structure["seasons"] if s.get("torrents")]
        season_nyaa_ids = {t["nyaa_id"] for t in season_torrents if t and t.get("nyaa_id")}
        total_seasons   = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0]) or len(structure["seasons"])
        if len(season_nyaa_ids) == 1 and len(season_torrents) >= max(1, total_seasons - 1):
            pack_ref = season_torrents[0]
            _add_torrent_to_structure(structure, pack_ref)
            pack_raw = torrents_by_id.get(pack_ref.get("nyaa_id")) or torrents_by_infohash.get(pack_ref.get("infohash"), {})
            for season in structure["seasons"]:
                season["torrents"] = []
            _populate_paths_from_torrent(structure, pack_raw)
            consolidated += 1

        # 1. Détecter les torrents intégraux par Counter
        total_eps = sum(
            1 for s in structure["seasons"] for ep in s["episodes"]
            if ep.get("aired") or ep.get("torrents")
        )
        all_ep_keys_per_ep = [
            {_torrent_key(t) for t in ep["torrents"] if _torrent_key(t)}
            for s in structure["seasons"]
            for ep in s["episodes"]
            if ep.get("aired") or ep.get("torrents")
        ]
        key_counts  = Counter(k for keys in all_ep_keys_per_ep for k in keys)
        real_seasons = [s for s in structure["seasons"] if s.get("season_number", 0) != 0]

        def key_covers_all_seasons(k):
            for s in real_seasons:
                has = any(k in {_torrent_key(t) for t in ep["torrents"] if _torrent_key(t)}
                          for ep in s["episodes"])
                if not has: return False
            return True

        integral_keys = [k for k, cnt in key_counts.items()
                         if cnt >= max(2, total_eps - 2) and total_eps > 0
                         and (len(real_seasons) <= 1 or key_covers_all_seasons(k))]

        if integral_keys:
            integral_keys_set = set(integral_keys)
            integral_raws     = []
            for k in integral_keys:
                raw = torrents_by_id.get(k) or torrents_by_infohash.get(k, {})
                integral_raws.append(raw)
                ref = next(
                    t for s in structure["seasons"]
                    for ep in s["episodes"]
                    for t in ep["torrents"]
                    if _torrent_key(t) == k
                )
                _add_torrent_to_structure(structure, ref)

            for s in structure["seasons"]:
                for ep in s["episodes"]:
                    kept_torrents, kept_paths = [], []
                    for t in ep["torrents"]:
                        if _torrent_key(t) not in integral_keys_set:
                            kept_torrents.append(t)
                            existing = next(
                                (obj for obj in ep["paths"]
                                 if isinstance(obj, dict) and obj.get("infohash") == t.get("infohash")),
                                None
                            )
                            if existing:
                                kept_paths.append(existing)
                    ep["torrents"] = kept_torrents
                    ep["paths"]    = kept_paths

            for raw in integral_raws:
                if raw.get("files"):
                    _populate_paths_from_torrent(structure, raw, append=True)

            for s in structure["seasons"]:
                for ep in s["episodes"]:
                    if not ep["torrents"]: continue
                    good_paths        = [obj for obj in ep["paths"] if isinstance(obj, dict) and obj.get("path")]
                    null_torrent_paths = [obj for obj in ep["paths"] if isinstance(obj, dict) and not obj.get("path")]
                    if good_paths and null_torrent_paths:
                        ep["torrents"] = []
                        ep["paths"]    = good_paths

            consolidated += 1
            continue

        # 2. Même torrent sur tous les épisodes d'une saison → season["torrents"]
        for season in structure["seasons"]:
            ep_torrents = [ep["torrents"][0] for ep in season["episodes"] if ep["torrents"]]
            ep_keys     = {_torrent_key(t) for t in ep_torrents if _torrent_key(t)}
            nb_eps      = sum(1 for ep in season["episodes"] if ep.get("aired") or ep.get("torrents"))
            if (len(ep_keys) == 1 and len(ep_torrents) >= max(1, nb_eps - 2)
                    and nb_eps > 1 and not season["torrents"]):
                pack_key = next(iter(ep_keys))
                pack_raw = torrents_by_id.get(pack_key) or torrents_by_infohash.get(pack_key, {})
                nb_real = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])
                if nb_real <= 1:
                    _add_torrent_to_structure(structure, ep_torrents[0])
                else:
                    season["torrents"].append(ep_torrents[0])
                path_idx        = build_ep_path_index(pack_raw)
                title_path_idx  = build_title_path_index(pack_raw)
                folder_idx_c    = build_folder_ep_index(pack_raw)
                fidx2           = build_file_index(pack_raw)
                is_specials     = season.get("season_number") == 0
                folder_key      = find_best_folder(season.get("title", ""), folder_idx_c)
                season_path_idx = folder_idx_c.get(folder_key, {}) if folder_key else {}
                pack_infohash2  = pack_raw.get("infohash")
                nb_seasons_real = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])
                for ep in season["episodes"]:
                    ep["torrents"] = []
                    n = ep.get("episode_number")
                    p = _compute_path(ep, n, is_specials, folder_key, season_path_idx,
                                      path_idx, title_path_idx, nb_seasons_real)
                    ep["paths"] = [{"infohash": pack_infohash2, "path": p,
                                    "file_index": file_index_of(p, fidx2)}] if p else []
                consolidated += 1

        # 1b. structure["torrents"] non-vide → peupler depuis chaque pack intégral
        if structure["torrents"]:
            has_any_paths = any(ep.get("paths") for s in structure["seasons"] for ep in s["episodes"])
            for pack_ref in structure["torrents"]:
                pack_raw = (torrents_by_id.get(pack_ref.get("nyaa_id"))
                            or torrents_by_infohash.get(pack_ref.get("infohash"), {}))
                if pack_raw.get("files"):
                    _populate_paths_from_torrent(structure, pack_raw, append=has_any_paths)
                    has_any_paths = True

    return consolidated


def cleanup_null_paths(structures, torrents_by_id, torrents_by_infohash):
    """Supprime les paths null et tente un fallback depuis le titre du torrent."""
    for structure in structures.values():
        for s in structure["seasons"]:
            for ep in s["episodes"]:
                if not ep["paths"]: continue
                valid      = [obj for obj in ep["paths"] if isinstance(obj, dict) and obj.get("path")]
                null_paths = [obj for obj in ep["paths"] if isinstance(obj, dict) and not obj.get("path")]
                if valid:
                    ep["paths"] = valid
                    if null_paths:
                        null_infohashes = {obj.get("infohash") for obj in null_paths}
                        ep["torrents"]  = [t for t in ep["torrents"]
                                           if (t.get("nyaa_id") or t.get("infohash")) not in null_infohashes
                                           and t.get("infohash") not in null_infohashes]
                elif null_paths:
                    new_paths = []
                    for obj in null_paths:
                        ih = obj.get("infohash")
                        ref_torrent = next(
                            (t for t in ep["torrents"] if t.get("infohash") == ih), None
                        )
                        if ref_torrent:
                            p = _torrent_title_to_path(ref_torrent.get("title"))
                            new_paths.append({"infohash": ih, "path": p, "file_index": None})
                        else:
                            new_paths.append(obj)
                    ep["paths"] = new_paths


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Étape 4 : Fankai Episode Assignor ===\n")

    if not Path(MATCHED_FILE).exists():
        print(f"[ERR] {MATCHED_FILE} introuvable → lancez d'abord s3_match.py")
        return

    matched = json.loads(Path(MATCHED_FILE).read_text(encoding="utf-8"))
    print(f"[Input] {len(matched)} torrents matchés chargés")

    # Lire les non-matchés de l'étape précédente
    unmatched_score = []
    if Path(UNMATCHED_FILE).exists():
        unmatched_score = json.loads(Path(UNMATCHED_FILE).read_text(encoding="utf-8"))

    # Collecter les séries uniques à charger
    serie_ids = sorted({entry["serie_id"] for entry in matched})
    print(f"[API] Construction de {len(serie_ids)} structures de séries...")

    # Faux objets série minimal pour build_structure
    serie_meta = {entry["serie_id"]: {"id": entry["serie_id"], "title": entry["serie_title"]}
                  for entry in matched}

    structures = {}
    for i, sid in enumerate(serie_ids, 1):
        print(f"  [{i:02d}/{len(serie_ids)}] série id={sid}")
        seasons_raw = fetch_seasons(sid)
        time.sleep(DELAY)
        structures[sid] = build_structure(serie_meta[sid], seasons_raw)
        time.sleep(DELAY)

    print(f"\n[OK] {len(structures)} structures prêtes\n")

    # Construire les index de torrents pour la consolidation
    all_torrents         = [entry["torrent"] for entry in matched]
    torrents_by_id       = {t["nyaa_id"]: t for t in all_torrents if t.get("nyaa_id")}
    torrents_by_infohash = {t["infohash"]: t for t in all_torrents if t.get("infohash") and not t.get("nyaa_id")}

    # Assigner chaque torrent à sa structure
    assigned_count = 0
    assign_failed  = []

    for entry in matched:
        torrent = entry["torrent"]
        ttype   = entry["ttype"]
        sid     = entry["serie_id"]

        ok = assign(structures[sid], torrent, ttype)
        if ok:
            assigned_count += 1
        else:
            ttitle     = torrent.get("title", "")
            serie_norm = (structures[sid].get("title") or "").lower()
            torrent_norm = ttitle.lower()
            is_replaced = re.search(r'ka[iï]', torrent_norm) and re.search(r'yaba[iï]?', serie_norm)
            if is_replaced:
                print(f"  [Skip] Série remplacée : {ttitle[:60]}")
            else:
                assign_failed.append({
                    "title":      ttitle,
                    "score":      entry["score"],
                    "best_match": entry["serie_title"],
                    "type":       ttype,
                    "reason":     "assign_failed",
                })

    print(f"[Assign] {assigned_count}/{len(matched)} torrents placés")
    print(f"[Assign] {len(assign_failed)} échecs d'assignation\n")

    # Consolidation des packs
    consolidated = consolidate(structures, torrents_by_id, torrents_by_infohash)
    cleanup_null_paths(structures, torrents_by_id, torrents_by_infohash)
    print(f"[Consolidation] {consolidated} packs détectés et remontés\n")

    # Écriture des fichiers séries
    for sid, structure in structures.items():
        out = OUTPUT_DIR / f"{sid}.json"
        out.write_text(json.dumps(structure, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[Output] {len(structures)} fichiers → {OUTPUT_DIR}/")

    # Rapport final des non-matchés (score + assign)
    all_unmatched = unmatched_score + assign_failed
    Path(REPORT_FILE).write_text(
        json.dumps(all_unmatched, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[Output] {len(all_unmatched)} non-matchés → {REPORT_FILE}")

    # Stats
    s_integral = sum(1 for s in structures.values() if s["torrents"])
    s_seasons  = sum(1 for s in structures.values() for ss in s["seasons"] if ss["torrents"])
    s_episodes = sum(1 for s in structures.values() for ss in s["seasons"] for e in ss["episodes"] if e["torrents"])
    print(f"\nIntégrales: {s_integral} | Saisons: {s_seasons} | Épisodes: {s_episodes}")

    if assign_failed:
        print(f"\n⚠️  Premiers échecs d'assignation :")
        for u in assign_failed[:15]:
            print(f"   {u['title'][:70]}")
            if u.get("best_match"): print(f"   → {u['best_match']} | type={u.get('type')}")

    print("\n✅ Étape 4 terminée !")


if __name__ == "__main__":
    main()

"""
ÉTAPE 2 - Fankai Torrent Matcher (v2)
======================================
Structure de sortie :
  - structure["torrents"]       → liste d'intégrales (Nyaa ou locaux)
  - season["torrents"]          → pack(s) saison
  - episode["torrents"]         → liste de torrents individuels
  - episode["paths"]            → [{infohash, path}] lié au torrent par infohash
"""

import re
import json
import time
import unicodedata
import requests
from pathlib import Path
from collections import Counter

METADATA_BASE    = "https://metadata.fankai.fr"
TORRENT_FILE     = "data/torrent_raw.json"
MANUAL_FILE      = "data/manual_torrents.json"
OUTPUT_DIR       = Path("series")
DELAY            = 0.3
SCORE_THRESHOLD  = 40

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
API_CACHE_DIR = Path("data/api_cache")
API_CACHE_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "fankarr-matcher/1.0"})

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

def norm(s):
    if not s: return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"!!", "xx", s)
    s = re.sub(r"!", "x", s)
    s = re.sub(r"[\W_]+", " ", s)
    return s.strip()

_NOISE = {
    "henshu", "hensh", "fancut", "fan", "cut", "kyodai",
    "brazh", "johker", "triggerforce", "roro", "livai", "livaï",
    "khyinn", "odji", "tenma", "manny", "hellinouille", "hokage",
    "shalouf", "crew", "esdra", "trafalgarwater", "elfenomomo",
    "pourquoi", "goruden", "r1", "san", "foxthug",
    "multi", "vostfr", "vf", "vo", "sub", "subs", "multisub",
    "x264", "x265", "1080p", "720p", "480p", "640p",
    "films", "film", "integrale", "integral",
    "saisons", "saison", "seasons", "season", "partie", "part",
    "complete", "pack", "ultimate", "audio", "subtitle", "subtitles",
    "eng", "fre", "fr", "en", "ita", "por", "esp", "deu", "ara",
}

def clean_tokens(title):
    tokens = set()
    for w in norm(title).split():
        if w not in _NOISE and not re.fullmatch(r"\d+", w):
            if len(w) >= 2 or (len(w) == 1 and w.isalpha()):
                tokens.add(w)
    return tokens

def extract_serie_title(torrent_title):
    t = torrent_title
    t = re.sub(r"^\[[^\]]*\]\s*", "", t).strip()
    t = re.sub(r"\s*\(Fan-Ka[iï]\)\s*", " ", t, flags=re.IGNORECASE).strip()
    cuts = [
        r"\s*[-–]\s*\d{1,3}\s*[-–]", r"\s+\d{2,3}\s*[-–]",
        r"\s*\(Fan-?Ka[iï]\)\s*[-–]\s*\d+",
        r"\s*[-–]\s*(?:integrale?|intégrale?|pack|saison|saisons|season|seasons|films?\s+\d|arc\s+\d)",
        r"\s+(?:integrale?|intégrale?|pack\s+complet)", r"\s*\(.*?\)",
        r"\s*[-–]\s*\d{4}p", r"\s+saison\s+\d", r"\s+s\d{1,2}\b",
    ]
    for pat in cuts:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            t = t[:m.start()].strip(" -–")
            break
    return t.strip()

_IS_INTEGRAL = re.compile(r"""
      integrale?|intégrale?
    | pack\s*(?:complet|integral)
    | ultimate\s*pack
    | toutes?\s*(?:les\s*)?saisons
    | all\s*seasons
    | films?\s+\d+\s*[àa]\s*\d+
    | saisons?\s*\d+\s*[/&,]\s*\d
    | seasons?\s*\d+\s*[/&,]\s*\d
    | parties?\s*\d+\s*[/&,]\s*\d
    | s\d{1,2}\s*/\s*s\d{1,2}
    | saga\s*\d+/\d+
    """, re.IGNORECASE | re.VERBOSE)

_SEASON_NUM  = re.compile(r"(?:saison|season)\s*(\d{1,2})\b|(?<!\w)s(\d{1,2})\b(?!\s*/)", re.IGNORECASE)
_PART_NUM    = re.compile(r"\bpart(?:ie)?\s*(\d{1,2})\b", re.IGNORECASE)
_ARC_NUM     = re.compile(r"\barc\s+(\d+)\b", re.IGNORECASE)
_FILM_RANGE  = re.compile(r"films?\s+(\d{1,3})\s*[àa]\s*(\d{1,3})", re.IGNORECASE)
_EPISODE_NUM = re.compile(r"""
    (?:
      (?:henshu|henshū|henshû|henshu|kaï|kai|yaba[iï]|yabaï|fancut|fan.cut)\s+(\d{1,3})\b
    | \(fan-?ka[iï]\)\s*[-–]\s*(\d{1,3})\b
    | \b(\d{1,3})\s*\(fan-?ka[iï]\)
    | [-–]\s*(\d{1,3})\s*[-–]
    | \bep(?:isode)?\s*(\d{1,3})\b
    )""", re.IGNORECASE | re.VERBOSE)

def detect_type(title):
    r = {"type": "unknown", "season": None, "episode": None, "ep_from": None, "ep_to": None}
    if _IS_INTEGRAL.search(title):
        r["type"] = "integral"; return r
    fm = _FILM_RANGE.search(title)
    if fm:
        r["type"] = "episode_range"; r["ep_from"] = int(fm.group(1)); r["ep_to"] = int(fm.group(2))
        sm = _SEASON_NUM.search(title)
        if sm: r["season"] = int(next(g for g in sm.groups() if g is not None))
        return r
    sm = _SEASON_NUM.search(title)
    if sm:
        r["type"] = "season"; r["season"] = int(next(g for g in sm.groups() if g is not None)); return r
    pm = _PART_NUM.search(title)
    if pm:
        r["type"] = "season"; r["season"] = int(pm.group(1)); return r
    am = _ARC_NUM.search(title)
    if am:
        r["type"] = "season"; r["season"] = int(am.group(1)); return r
    em = _EPISODE_NUM.search(title)
    if em:
        r["type"] = "episode"; r["episode"] = int(next(g for g in em.groups() if g is not None)); return r
    # Spéciaux : "Spécial #1", "Special 01", "SP1"
    sm2 = re.search(r"sp[eé][cç]ial\s*#?\s*0*(\d+)", title, re.IGNORECASE)
    if sm2:
        r["type"] = "episode"; r["episode"] = int(sm2.group(1)); return r
    return r

def _dedup_letters(tokens):
    return {re.sub(r'(.)\1+', r'\1', t) for t in tokens}

def score_match(torrent_title, serie):
    t_tokens = clean_tokens(extract_serie_title(torrent_title))
    t_dedup  = _dedup_letters(t_tokens)
    best = 0
    for field in ("title", "show_title", "original_title"):
        s_tokens = clean_tokens(serie.get(field) or "")
        if not t_tokens or not s_tokens: continue
        s_dedup = _dedup_letters(s_tokens)
        inter = len(t_tokens & s_tokens); union = len(t_tokens | s_tokens)
        jaccard = inter / union if union else 0
        coverage = inter / len(s_tokens) if s_tokens else 0
        sc = int(jaccard * 50 + coverage * 50)
        inter2 = len(t_dedup & s_dedup); union2 = len(t_dedup | s_dedup)
        jaccard2 = inter2 / union2 if union2 else 0
        coverage2 = inter2 / len(s_dedup) if s_dedup else 0
        sc2 = int(jaccard2 * 50 + coverage2 * 50)
        best = max(best, sc, sc2)
    return min(best, 100)

def load_torrents():
    p = Path(TORRENT_FILE)
    if not p.exists():
        print(f"[ERR] {TORRENT_FILE} introuvable → lancez d'abord step1"); return []
    torrents = json.loads(p.read_text(encoding="utf-8"))
    m = Path(MANUAL_FILE)
    if m.exists():
        manual = json.loads(m.read_text(encoding="utf-8"))
        manual = [t for t in manual if isinstance(t, dict)
                  and not any(k.startswith("_") for k in t) and t.get("title")]
        if manual:
            existing = {t.get("infohash") for t in torrents if t.get("infohash")}
            existing_titles = {t.get("title") for t in torrents if t.get("title")}
            added = 0
            for t in manual:
                if t.get("infohash") and t["infohash"] in existing: continue
                if t.get("title") and t["title"] in existing_titles: continue
                torrents.append(t); added += 1
            if added:
                print(f"[Manual] {added} torrent(s) manuel(s) chargé(s) depuis {MANUAL_FILE}")
    return torrents

def fetch_all_series():
    series = api_get(f"{METADATA_BASE}/series") or []
    active    = [s for s in series if (s.get("status") or "").lower() not in ("en suspens", "suspended", "ended_replaced")]
    suspended = [s for s in series if (s.get("status") or "").lower() in ("en suspens", "suspended", "ended_replaced")]
    def _base_title(s):
        t = norm(s.get("title") or s.get("show_title") or "")
        return re.sub(r"(?:kai|kais|yabai|henshu|hensh|recut|fancut)", "", t).strip()
    active_bases = {_base_title(s) for s in active}
    kept_suspended = []
    for s in suspended:
        base = _base_title(s)
        if base and base not in active_bases: kept_suspended.append(s)
        else: print(f"[API] Ignoré (En suspens, doublon actif) : {s.get('title')}")
    return active + kept_suspended

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

def normalize_seasons(seasons_raw):
    if not seasons_raw: return []
    return [s for s in seasons_raw if isinstance(s, dict)]

def fix_encoding(s):
    if not s or not isinstance(s, str): return s
    try: return s.encode('cp1252').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        try: return s.encode('latin-1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError): return s

def build_structure(serie, seasons_raw):
    seasons_raw = normalize_seasons(seasons_raw)
    seasons_out = []
    for season in sorted(seasons_raw, key=lambda s: s.get("season_number", 0)):
        eps_raw = fetch_episodes(season["id"])
        eps_raw = [e for e in eps_raw if isinstance(e, dict)]
        time.sleep(DELAY)
        episodes_out = [
            {
                "id":             ep["id"],
                "episode_number": ep.get("episode_number"),
                "title":          fix_encoding(ep.get("title")),
                "aired":          ep.get("aired") or None,
                "torrents":       [],
                "paths":          [],
            }
            for ep in sorted(eps_raw, key=lambda e: e.get("episode_number", 0))
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
        "nyaa_id":     t.get("nyaa_id"),
        "nyaa_url":    t.get("nyaa_url"),
        "title":       t.get("title"),
        "torrent_url": t.get("torrent_url"),
        "magnet":      t.get("magnet"),
        "infohash":    t.get("infohash"),
        "size":        t.get("size"),
        "pub_date":    t.get("pub_date"),
        "seeders":     t.get("seeders"),
        "fankai":      t.get("fankai", True),
    }

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

def build_ep_path_index(torrent):
    """Construit un index ep_number → path.
    Gère aussi la numérotation SxxEyy en utilisant ep_numbers pour mapper
    les numéros relatifs aux numéros globaux."""
    index = {}
    ep_numbers = torrent.get("ep_numbers") or []

    # Index par numéro extrait du filename
    raw_index = {}
    for f in torrent.get("files") or []:
        num = _extract_ep_video(Path(f).name)
        if num is not None:
            if num not in raw_index:
                raw_index[num] = f

    # Si ep_numbers fournis et numéros dans les fichiers ne correspondent pas
    # (ex: torrent avec S01x01..S01x16 + S02x01..S02x06 → raw_index a 1-16 pour S01
    # mais S02 ep1 écrase S01 ep1), utiliser l'ordre des fichiers vidéo
    video_files = sorted([
        f for f in (torrent.get("files") or [])
        if Path(f).suffix.lower() in _VIDEO_EXT
        and _extract_ep_video(Path(f).name) is not None
        and not Path(f).name.lower().endswith(('.png', '.jpg', '.nfo', '.zip'))
    ])

    if ep_numbers and len(ep_numbers) == len(video_files):
        # Mapper dans l'ordre fichier → ep_number global
        for ep_num, f in zip(sorted(ep_numbers), video_files):
            index[ep_num] = f
    else:
        index = raw_index

    return index

def build_title_path_index(torrent):
    result = []
    for f in torrent.get("files") or []:
        fname = Path(f).name
        if Path(fname).suffix.lower() not in _VIDEO_EXT: continue
        stem = Path(fname).stem
        stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()
        stem = re.sub(r'[-–]?\s*\d{3,4}p.*$', '', stem, flags=re.IGNORECASE).strip()
        stem = re.sub(r'\s*[-–]\s*(?:dvd|blu.?ray|bdrip|web|hdtv|multi).*$', '', stem, flags=re.IGNORECASE).strip()
        parts = re.split(r'\s*[-–]\s*', stem)
        _NOISE_PARTS = {'film', 'bonus', 'ova', 'special', 'sp'}
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
        ep_num = _extract_ep_video(parts[-1])
        if ep_num is None: continue
        if folder_key not in index: index[folder_key] = {}
        if ep_num not in index[folder_key]: index[folder_key][ep_num] = f
    return index

def _norm_folder(s):
    """Normalise un nom de dossier : remplace 'Saison 1' par '1', garde les noms distinctifs."""
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    # Remplacer "Saison 1" / "Season 2" / "Partie 3" par juste le numéro
    s = re.sub(r"\b(?:partie|part|saison|season|arc)\s*(\d+)\b", r"\1", s, flags=re.IGNORECASE)
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).lower().strip()

def find_best_folder(season_title, folder_index):
    season_norm = _norm_folder(season_title)
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
    ep_norm = _stem_title(ep_title)
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

def _torrent_title_to_path(torrent_title):
    if not torrent_title: return None
    t = re.sub(r'^\[[^\]]*\]\s*', '', torrent_title).strip()
    t = re.sub(r'\s*[-–]\s*\d{3,4}p.*$', '', t, flags=re.IGNORECASE).strip()
    t = re.sub(r'\s*\(.*?\)\s*$', '', t).strip()
    return t + ".mkv" if t else None

def _compute_path(ep, n, is_specials, folder_key, season_path_idx, path_idx, title_path_idx, nb_seasons, strict=False):
    """
    Calcule le path d'un épisode.
    strict=True : assign() — ne pas deviner si multi-saisons sans dossier distinctif.
    strict=False : consolidation — utiliser path_idx même multi-saisons.
    """
    if is_specials:
        return match_title_to_path(ep.get("title", ""), title_path_idx)
    if season_path_idx.get(n):
        return season_path_idx[n]
    if folder_key is not None:
        return path_idx.get(n) or match_title_to_path(ep.get("title", ""), title_path_idx)
    if nb_seasons <= 1:
        return path_idx.get(n) or match_title_to_path(ep.get("title", ""), title_path_idx)
    # Multi-saisons sans dossier distinctif
    if strict:
        return None
    if path_idx.get(n):
        return path_idx[n]
    return match_title_to_path(ep.get("title", ""), title_path_idx)

def _add_torrent_to_structure(structure, ref):
    existing = {t.get("nyaa_id") or t.get("infohash") for t in structure["torrents"]}
    key = ref.get("nyaa_id") or ref.get("infohash")
    if key not in existing:
        structure["torrents"].append(ref)

def assign(structure, torrent, ttype):
    ref = make_ref(torrent)
    t   = ttype["type"]

    if t == "integral":
        _add_torrent_to_structure(structure, ref)
        return True

    if t == "season":
        # Assigner aux épisodes de la saison ciblée avec les bons paths.
        # NE PAS appeler _add_torrent_to_structure — la consolidation étape 2 s'en charge.
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
            is_specials = sn == 0
            folder_key  = find_best_folder(season.get("title", ""), folder_idx)
            season_path_idx = folder_idx.get(folder_key, {}) if folder_key else {}
            for ep in season["episodes"]:
                n = ep.get("episode_number")
                if ep_numbers_list and n not in ep_numbers_list:
                    continue
                existing = {t2.get("nyaa_id") or t2.get("infohash") for t2 in ep["torrents"]}
                key = ref.get("nyaa_id") or ref.get("infohash")
                if key not in existing:
                    ep["torrents"].append(ref)
                    p = _compute_path(ep, n, is_specials, folder_key, season_path_idx,
                                      path_idx, title_path_idx, nb_seasons_loc, strict=True)
                    if p is None:
                        p = _torrent_title_to_path(torrent.get("title"))
                    ep["paths"].append({"infohash": ref.get("infohash"), "path": p})
                assigned = True
        return assigned

    nb_seasons = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])

    if t == "episode_range":
        ep_from = ttype["ep_from"]; ep_to = ttype["ep_to"]
        path_idx       = build_ep_path_index(torrent)
        title_path_idx = build_title_path_index(torrent)
        folder_idx     = build_folder_ep_index(torrent)
        assigned = False
        for season in structure["seasons"]:
            is_specials = season.get("season_number") == 0
            folder_key  = find_best_folder(season.get("title", ""), folder_idx)
            season_path_idx = folder_idx.get(folder_key, {}) if folder_key else {}
            for ep in season["episodes"]:
                n = ep.get("episode_number")
                if n is not None and ep_from <= n <= ep_to:
                    existing = {t2.get("nyaa_id") or t2.get("infohash") for t2 in ep["torrents"]}
                    key = ref.get("nyaa_id") or ref.get("infohash")
                    if key not in existing:
                        ep["torrents"].append(ref)
                        p = _compute_path(ep, n, is_specials, folder_key, season_path_idx,
                                          path_idx, title_path_idx, nb_seasons, strict=True)
                        if p is None:
                            p = _torrent_title_to_path(torrent.get("title"))
                        ep["paths"].append({"infohash": ref.get("infohash"), "path": p})
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
        for season in structure["seasons"]:
            # Si force_season spécifié, ne traiter que cette saison
            if force_season is not None and season.get("season_number") != force_season:
                continue
            is_specials = season.get("season_number") == 0
            folder_key  = find_best_folder(season.get("title", ""), folder_idx)
            season_path_idx = folder_idx.get(folder_key, {}) if folder_key else {}
            for ep in season["episodes"]:
                if ep.get("episode_number") == ep_num:
                    existing = {t2.get("nyaa_id") or t2.get("infohash") for t2 in ep["torrents"]}
                    key = ref.get("nyaa_id") or ref.get("infohash")
                    if key not in existing:
                        ep["torrents"].append(ref)
                        if force_path:
                            p = force_path
                        else:
                            p = _compute_path(ep, ep_num, is_specials, folder_key, season_path_idx,
                                              path_idx, title_path_idx, nb_seasons, strict=True)
                            if p is None:
                                p = _torrent_title_to_path(torrent.get("title"))
                        ep["paths"].append({"infohash": ref.get("infohash"), "path": p})
                    return True
        return False

    return False

def slugify(s):
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^\w\s-]", "", s.lower())
    s = re.sub(r"[\s_-]+", "_", s).strip("_")
    return s[:60]

def _populate_paths_from_torrent(structure, pack_raw, append=False):
    """
    Peuple les paths des épisodes depuis les fichiers du torrent pack.
    append=True : ajoute au lieu de remplacer (pour les multi-intégrales).
    """
    path_idx       = build_ep_path_index(pack_raw)
    title_path_idx = build_title_path_index(pack_raw)
    folder_idx_c   = build_folder_ep_index(pack_raw)
    nb_seasons     = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])
    pack_infohash  = pack_raw.get("infohash")
    for season in structure["seasons"]:
        is_specials = season.get("season_number") == 0
        folder_key  = find_best_folder(season.get("title", ""), folder_idx_c)
        season_path_idx = folder_idx_c.get(folder_key, {}) if folder_key else {}
        for ep in season["episodes"]:
            n = ep.get("episode_number")
            def _get_path():
                if is_specials:
                    return match_title_to_path(ep.get("title", ""), title_path_idx)
                if season_path_idx.get(n):
                    return season_path_idx[n]
                return path_idx.get(n) or match_title_to_path(ep.get("title", ""), title_path_idx)
            p = _get_path()
            path_obj = {"infohash": pack_infohash, "path": p}
            if append:
                if p:
                    ep["paths"].append(path_obj)
            elif ep["torrents"]:
                ep["paths"] = [{"infohash": t2.get("infohash"), "path": p}
                               for t2 in ep["torrents"]]
            else:
                ep["paths"] = [path_obj] if p else []

def _torrent_key(t):
    return t.get("nyaa_id") or t.get("infohash")

def main():
    print("=== Étape 2 : Fankai Torrent Matcher v2 ===\n")

    torrents = load_torrents()
    if not torrents: return
    print(f"[Torrents] {len(torrents)} chargés")

    print("[API] Récupération des séries...")
    series = fetch_all_series()
    print(f"[API] {len(series)} séries\n")

    structures = {}
    for i, serie in enumerate(series, 1):
        sid = serie["id"]
        print(f"  [{i:02d}/{len(series)}] {serie.get('title')} (id={sid})")
        seasons_raw = fetch_seasons(sid)
        time.sleep(DELAY)
        structures[sid] = build_structure(serie, seasons_raw)
        time.sleep(DELAY)

    print(f"\n[OK] {len(structures)} structures prêtes\n")

    matched = 0; unmatched = []

    for torrent in torrents:
        ttitle = torrent.get("title", "")
        if not ttitle: continue

        best_serie, best_score = None, 0
        for s in series:
            sc = score_match(ttitle, s)
            if sc > best_score: best_score = sc; best_serie = s

        if best_serie is None or best_score < SCORE_THRESHOLD:
            unmatched.append({"title": ttitle, "score": best_score,
                               "best_match": best_serie.get("title") if best_serie else None})
            continue

        ep_numbers = torrent.get("ep_numbers")
        force_type = torrent.get("force_type")

        if force_type == "integral":
            ttype = {"type": "integral", "season": None, "episode": None, "ep_from": None, "ep_to": None}
        elif torrent.get("force_season") is not None and ep_numbers and len(ep_numbers) == 1:
            # Torrent manuel avec saison+épisode forcés → episode direct
            ttype = {"type": "episode", "episode": ep_numbers[0], "season": None, "ep_from": None, "ep_to": None}
        elif torrent.get("force_season") is not None and ep_numbers and len(ep_numbers) > 1:
            # Torrent manuel avec saison forcée et plage d'épisodes
            ttype = {"type": "episode_range", "season": None, "episode": None,
                     "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
        else:
            title_type = detect_type(ttitle)
            if title_type["type"] == "season" and ep_numbers is not None and len(ep_numbers) > 0:
                # Pack saison avec numéro explicite + ep_numbers → garder "season"
                # pour que assign() cible uniquement la bonne saison
                ttype = title_type
            elif title_type["type"] == "integral" and ep_numbers is not None and len(ep_numbers) > 0:
                # Intégrale avec ep_numbers → episode_range pour peupler précisément
                if len(ep_numbers) == 1:
                    ttype = {"type": "episode", "episode": ep_numbers[0],
                             "season": None, "ep_from": None, "ep_to": None}
                else:
                    ttype = {"type": "episode_range", "season": None, "episode": None,
                             "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
            elif title_type["type"] in ("integral", "season"):
                ttype = title_type
            elif ep_numbers is not None and len(ep_numbers) > 0:
                if len(ep_numbers) == 1:
                    ttype = {"type": "episode", "episode": ep_numbers[0],
                             "season": None, "ep_from": None, "ep_to": None}
                else:
                    ttype = {"type": "episode_range", "season": None, "episode": None,
                             "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
            else:
                ttype = title_type

        ok = assign(structures[best_serie["id"]], torrent, ttype)

        if ok:
            matched += 1
        else:
            torrent_norm = norm(ttitle)
            serie_norm   = norm(best_serie.get("title") or "")
            is_replaced  = re.search(r'ka[iï]', torrent_norm) and re.search(r'yaba[iï]?', serie_norm)
            if is_replaced:
                print(f"  [Skip] Série remplacée : {ttitle[:60]}")
            else:
                unmatched.append({"title": ttitle, "score": best_score,
                                   "best_match": best_serie.get("title"),
                                   "type": ttype, "reason": "assign_failed"})

    print(f"[Match] {matched}/{len(torrents)} torrents affectés")
    print(f"[Match] {len(unmatched)} non affectés\n")

    # ── Consolidation ────────────────────────────────────────────────────────
    torrents_by_id       = {t["nyaa_id"]: t for t in torrents if t.get("nyaa_id")}
    torrents_by_infohash = {t["infohash"]: t for t in torrents if t.get("infohash") and not t.get("nyaa_id")}
    consolidated = 0

    for structure in structures.values():

        # 0. Même torrent sur toutes les saisons → structure["torrents"]
        season_torrents = [s["torrents"][0] for s in structure["seasons"] if s.get("torrents")]
        season_nyaa_ids = {t["nyaa_id"] for t in season_torrents if t and t.get("nyaa_id")}
        total_seasons = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0]) or len(structure["seasons"])
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
        key_counts = Counter(k for keys in all_ep_keys_per_ep for k in keys)

        # Saisons réelles (non-spéciales)
        real_seasons = [s for s in structure["seasons"] if s.get("season_number", 0) != 0]

        def key_covers_all_seasons(k):
            """Vrai si le torrent couvre au moins 1 épisode dans chaque saison réelle."""
            for s in real_seasons:
                has = any(k in {_torrent_key(t) for t in ep["torrents"] if _torrent_key(t)}
                          for ep in s["episodes"])
                if not has:
                    return False
            return True

        # Intégral = couvre ≥ max(2, total-2) épisodes ET toutes les saisons réelles
        integral_keys = [k for k, cnt in key_counts.items()
                         if cnt >= max(2, total_eps - 2) and total_eps > 0
                         and (len(real_seasons) <= 1 or key_covers_all_seasons(k))]

        if integral_keys:
            integral_keys_set = set(integral_keys)
            integral_raws = []
            for k in integral_keys:
                raw = torrents_by_id.get(k) or next(
                    (t for t in torrents if t.get("infohash") == k), {}
                )
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

            # Nettoyer : si un épisode a un torrent individuel avec path null
            # mais qu'il a aussi un path non-null venant du pack → supprimer le torrent individuel
            for s in structure["seasons"]:
                for ep in s["episodes"]:
                    if not ep["torrents"]: continue
                    good_paths = [obj for obj in ep["paths"]
                                  if isinstance(obj, dict) and obj.get("path")]
                    null_torrent_paths = [obj for obj in ep["paths"]
                                          if isinstance(obj, dict) and not obj.get("path")]
                    if good_paths and null_torrent_paths:
                        # Le pack couvre déjà cet épisode → supprimer les torrents individuels
                        ep["torrents"] = []
                        ep["paths"] = good_paths

            consolidated += 1
            continue

        # 2. Même torrent sur tous les épisodes d'une saison → season["torrents"]
        for season in structure["seasons"]:
            ep_torrents = [ep["torrents"][0] for ep in season["episodes"] if ep["torrents"]]
            ep_keys = {_torrent_key(t) for t in ep_torrents if _torrent_key(t)}
            nb_eps = sum(1 for ep in season["episodes"] if ep.get("aired") or ep.get("torrents"))
            if (len(ep_keys) == 1 and len(ep_torrents) >= max(1, nb_eps - 2)
                    and nb_eps > 1 and not season["torrents"]):
                pack_key = next(iter(ep_keys))
                pack_raw = torrents_by_id.get(pack_key) or next(
                    (t for t in torrents if t.get("infohash") == pack_key), {}
                )
                nb_real = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])
                if nb_real <= 1:
                    _add_torrent_to_structure(structure, ep_torrents[0])
                else:
                    season["torrents"].append(ep_torrents[0])
                path_idx       = build_ep_path_index(pack_raw)
                title_path_idx = build_title_path_index(pack_raw)
                folder_idx_c   = build_folder_ep_index(pack_raw)
                is_specials    = season.get("season_number") == 0
                folder_key     = find_best_folder(season.get("title", ""), folder_idx_c)
                season_path_idx = folder_idx_c.get(folder_key, {}) if folder_key else {}
                pack_infohash2 = pack_raw.get("infohash")
                nb_seasons_real = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0])
                for ep in season["episodes"]:
                    ep["torrents"] = []
                    n = ep.get("episode_number")
                    p = _compute_path(ep, n, is_specials, folder_key, season_path_idx,
                                      path_idx, title_path_idx, nb_seasons_real)
                    ep["paths"] = [{"infohash": pack_infohash2, "path": p}] if p else []
                consolidated += 1

        # 1b. structure["torrents"] non-vide mais épisodes sans paths → peupler
        if structure["torrents"] and not any(ep.get("paths")
                                              for s in structure["seasons"]
                                              for ep in s["episodes"]):
            pack_ref = next((t for t in structure["torrents"] if t.get("nyaa_id")), structure["torrents"][0])
            pack_raw = torrents_by_id.get(pack_ref.get("nyaa_id")) or torrents_by_infohash.get(pack_ref.get("infohash"), {})
            if pack_raw.get("files"):
                _populate_paths_from_torrent(structure, pack_raw, append=False)

    # Nettoyage final : supprimer les paths avec path=null
    # Si l'épisode a d'autres paths valides, garder seulement ceux-là
    # Si l'épisode n'a que des paths null, les remplacer par le fallback titre
    for structure in structures.values():
        for s in structure["seasons"]:
            for ep in s["episodes"]:
                if not ep["paths"]: continue
                valid = [obj for obj in ep["paths"]
                         if isinstance(obj, dict) and obj.get("path")]
                null_paths = [obj for obj in ep["paths"]
                              if isinstance(obj, dict) and not obj.get("path")]
                if valid:
                    # Garder seulement les paths valides, supprimer les null
                    ep["paths"] = valid
                    # Si on avait des torrents individuels sans path, les supprimer aussi
                    # (le pack les couvre déjà)
                    if null_paths:
                        null_infohashes = {obj.get("infohash") for obj in null_paths}
                        ep["torrents"] = [t for t in ep["torrents"]
                                          if (t.get("nyaa_id") or t.get("infohash")) not in null_infohashes
                                          and t.get("infohash") not in null_infohashes]
                elif null_paths:
                    # Aucun path valide → essayer le fallback titre pour chaque torrent
                    new_paths = []
                    for obj in null_paths:
                        # Trouver le torrent correspondant
                        ih = obj.get("infohash")
                        ref_torrent = next(
                            (t for t in ep["torrents"] if t.get("infohash") == ih),
                            None
                        )
                        if ref_torrent:
                            p = _torrent_title_to_path(ref_torrent.get("title"))
                            new_paths.append({"infohash": ih, "path": p})
                        else:
                            new_paths.append(obj)
                    ep["paths"] = new_paths

    print(f"[Consolidation] {consolidated} packs détectés et remontés\n")

    for sid, structure in structures.items():
        out = OUTPUT_DIR / f"{sid}.json"
        out.write_text(json.dumps(structure, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[Output] {len(structures)} fichiers → {OUTPUT_DIR}/")
    report = Path("data/unmatched_torrents.json")
    report.write_text(json.dumps(unmatched, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[Output] {len(unmatched)} non-matchés → {report}")

    s_integral = sum(1 for s in structures.values() if s["torrents"])
    s_seasons  = sum(1 for s in structures.values() for ss in s["seasons"] if ss["torrents"])
    s_episodes = sum(1 for s in structures.values() for ss in s["seasons"] for e in ss["episodes"] if e["torrents"])
    print(f"\n📊 Intégrales: {s_integral} | Saisons: {s_seasons} | Épisodes: {s_episodes}")
    print("\n✅ Étape 2 terminée !")

    if unmatched:
        print(f"\n⚠️  Premiers non-matchés (score < {SCORE_THRESHOLD}) :")
        for u in unmatched[:25]:
            print(f"   [{u['score']:3d}] {u['title'][:80]}")
            if u.get("best_match"): print(f"         best_match → {u['best_match']}")
            if u.get("reason") == "assign_failed": print(f"         type → {u.get('type')}")

if __name__ == "__main__":
    main()
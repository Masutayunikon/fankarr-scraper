"""
ÉTAPE 2 - Fankai Torrent Matcher (v2)
======================================
Structure de sortie :
  - structure["torrents"]       → liste d'intégrales (Nyaa ou locaux)
  - season["torrent"]           → pack saison (un seul, le plus récent)
  - episode["torrents"]         → liste de torrents individuels
  - episode["paths"]            → liste de paths, index lié à episode["torrents"]
"""

import re
import json
import time
import unicodedata
import requests
from pathlib import Path

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
                "torrents":       [],   # liste de torrents individuels
                "paths":          [],   # [{infohash, path}] — lié au torrent par infohash
            }
            for ep in sorted(eps_raw, key=lambda e: e.get("episode_number", 0))
        ]
        seasons_out.append({
            "id":            season["id"],
            "season_number": season.get("season_number"),
            "title":         fix_encoding(season.get("title") or season.get("name")),
            "torrents":      [],  # pack(s) saison
            "episodes":      episodes_out,
        })
    return {
        "id":         serie["id"],
        "title":      fix_encoding(serie.get("title")),
        "show_title": fix_encoding(serie.get("show_title")),
        "torrents":   [],  # pack(s) intégral(aux)
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
    index = {}
    for f in torrent.get("files") or []:
        num = _extract_ep_video(Path(f).name)
        if num is not None and num not in index:
            index[num] = f
    return index

def build_title_path_index(torrent):
    result = []
    for f in torrent.get("files") or []:
        fname = Path(f).name
        if Path(fname).suffix.lower() not in _VIDEO_EXT: continue
        stem = Path(fname).stem
        stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()
        stem = re.sub(r'[-–]?\s*\d{3,4}p.*$', '', stem, flags=re.IGNORECASE).strip()
        # Supprimer suffixes source : DVD, Blu-ray, WEB, MULTI, x264... qui traînent après le titre
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
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\bpartie\b|\bpart\b|\bsaison\b|\bseason\b|\barc\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\d+", "", s)
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
    """Construit un path depuis le titre Nyaa quand files est absent (torrent individuel)."""
    if not torrent_title: return None
    t = re.sub(r'^\[[^\]]*\]\s*', '', torrent_title).strip()
    t = re.sub(r'\s*[-–]\s*\d{3,4}p.*$', '', t, flags=re.IGNORECASE).strip()
    t = re.sub(r'\s*\(.*?\)\s*$', '', t).strip()
    return t + ".mkv" if t else None


def _compute_path(ep, n, is_specials, folder_key, season_path_idx, path_idx, title_path_idx, nb_seasons):
    """Calcule le path d'un épisode selon la hiérarchie de priorité."""
    if is_specials:
        return match_title_to_path(ep.get("title", ""), title_path_idx)
    if season_path_idx.get(n):
        return season_path_idx[n]
    if folder_key is not None:
        return path_idx.get(n) or match_title_to_path(ep.get("title", ""), title_path_idx)
    if nb_seasons <= 1:
        return path_idx.get(n) or match_title_to_path(ep.get("title", ""), title_path_idx)
    # Multi-saisons sans dossier distinctif : utiliser path_idx si le numéro est présent
    # (le torrent ne couvre qu'une partie de la série, pas d'ambiguïté de collision)
    if path_idx.get(n):
        return path_idx[n]
    return match_title_to_path(ep.get("title", ""), title_path_idx)

def _add_torrent_to_structure(structure, ref):
    """Ajoute un torrent intégral à structure["torrents"] si pas déjà présent."""
    existing = {t.get("nyaa_id") or t.get("infohash") for t in structure["torrents"]}
    key = ref.get("nyaa_id") or ref.get("infohash")
    if key not in existing:
        structure["torrents"].append(ref)

def assign(structure, torrent, ttype):
    ref = make_ref(torrent)
    t   = ttype["type"]

    if t in ("integral", "season"):
        _add_torrent_to_structure(structure, ref)
        return True

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
                    existing = {t.get("nyaa_id") or t.get("infohash") for t in ep["torrents"]}
                    key = ref.get("nyaa_id") or ref.get("infohash")
                    if key not in existing:
                        ep["torrents"].append(ref)
                        p = _compute_path(ep, n, is_specials, folder_key, season_path_idx, path_idx, title_path_idx, nb_seasons)
                        # Fallback titre seulement si le torrent n'a pas de files
                        # (si files présents, la consolidation peuplera les paths correctement)
                        if p is None and not torrent.get("files"):
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
        for season in structure["seasons"]:
            is_specials = season.get("season_number") == 0
            folder_key  = find_best_folder(season.get("title", ""), folder_idx)
            season_path_idx = folder_idx.get(folder_key, {}) if folder_key else {}
            for ep in season["episodes"]:
                if ep.get("episode_number") == ep_num:
                    existing = {t.get("nyaa_id") or t.get("infohash") for t in ep["torrents"]}
                    key = ref.get("nyaa_id") or ref.get("infohash")
                    if key not in existing:
                        ep["torrents"].append(ref)
                        p = _compute_path(ep, ep_num, is_specials, folder_key, season_path_idx, path_idx, title_path_idx, nb_seasons)
                        if p is None and not torrent.get("files"):
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
    Peuple les paths des épisodes depuis les fichiers du torrent pack (consolidation).
    Si append=True, ajoute le path à la liste existante plutôt que de remplacer.
    paths = [{infohash, path}] — lié au torrent par infohash.
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
                ep["paths"] = [{"infohash": t.get("infohash"), "path": p}
                               for t in ep["torrents"]]
            else:
                ep["paths"] = [path_obj] if p else []

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
        else:
            # detect_type prime pour intégrale/saison, SAUF si ep_numbers couvre
            # une plage partielle (ex: Kuroko S01=[1-7], S02=[8-11], S03=[12-15])
            # Dans ce cas on garde episode_range pour peupler les bons paths
            title_type = detect_type(ttitle)
            if title_type["type"] in ("integral", "season") and ep_numbers is not None and len(ep_numbers) > 0:
                # Pack intégrale ou saison avec fichiers connus → episode_range
                # pour peupler les paths des épisodes couverts précisément
                if len(ep_numbers) == 1:
                    ttype = {"type": "episode", "episode": ep_numbers[0],
                             "season": None, "ep_from": None, "ep_to": None}
                else:
                    ttype = {"type": "episode_range", "season": None, "episode": None,
                             "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
            elif title_type["type"] in ("integral", "season"):
                # Sans fichiers connus → garder le type original
                ttype = title_type
            elif ep_numbers is not None and len(ep_numbers) > 0:
                if len(ep_numbers) == 1:
                    ttype = {"type": "episode", "episode": ep_numbers[0],
                             "season": None, "ep_from": None, "ep_to": None}
                else:
                    ttype = {"type": "episode_range", "season": None, "episode": None,
                             "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
            elif ep_numbers is not None and len(ep_numbers) == 0:
                ttype = title_type
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
    torrents_by_id = {t["nyaa_id"]: t for t in torrents if t.get("nyaa_id")}
    # Index secondaire par infohash pour les torrents manuels sans nyaa_id
    torrents_by_infohash = {t["infohash"]: t for t in torrents if t.get("infohash") and not t.get("nyaa_id")}
    consolidated = 0

    for structure in structures.values():

        # 0. Même torrent sur toutes les saisons → remonte sur structure["torrents"]
        season_torrents = [s["torrents"][0] for s in structure["seasons"] if s.get("torrents")]
        season_nyaa_ids = {t["nyaa_id"] for t in season_torrents if t and t.get("nyaa_id")}
        total_seasons = len([s for s in structure["seasons"] if s.get("season_number", 0) != 0]) or len(structure["seasons"])
        if len(season_nyaa_ids) == 1 and len(season_torrents) >= max(1, total_seasons - 1):
            pack_ref = season_torrents[0]
            _add_torrent_to_structure(structure, pack_ref)
            if pack_ref.get("nyaa_id"):
                pack_raw = torrents_by_id.get(pack_ref["nyaa_id"], {})
            else:
                pack_raw = torrents_by_infohash.get(pack_ref.get("infohash"), {})
            for season in structure["seasons"]:
                season["torrents"] = []
            _populate_paths_from_torrent(structure, pack_raw)
            consolidated += 1

        # 1. Détecter tous les torrents intégraux (même clé sur tous les épisodes sortis)
        def _torrent_key(t):
            return t.get("nyaa_id") or t.get("infohash")

        # Compter les épisodes "actifs" : sortis OU ayant déjà un torrent assigné
        total_eps = sum(
            1 for s in structure["seasons"] for ep in s["episodes"]
            if ep.get("aired") or ep.get("torrents")
        )

        # Collecter toutes les clés uniques présentes dans les épisodes actifs
        all_ep_keys_per_ep = [
            {_torrent_key(t) for t in ep["torrents"] if _torrent_key(t)}
            for s in structure["seasons"]
            for ep in s["episodes"]
            if ep.get("aired") or ep.get("torrents")
        ]

        # Une clé est "intégrale" si elle apparaît dans au moins total_eps-2 épisodes
        from collections import Counter
        key_counts = Counter(k for keys in all_ep_keys_per_ep for k in keys)
        integral_keys = [k for k, cnt in key_counts.items()
                         if cnt >= max(1, total_eps - 2) and total_eps > 0]

        if integral_keys:
            # Construire les paths finaux : une entrée par torrent intégral
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

            # Vider les torrents intégraux des épisodes (garder les torrents individuels)
            for s in structure["seasons"]:
                for ep in s["episodes"]:
                    # Ne retirer que les torrents qui font partie des intégrales
                    kept_torrents = []
                    kept_paths = []
                    for t in ep["torrents"]:
                        if _torrent_key(t) not in integral_keys_set:
                            kept_torrents.append(t)
                            # Retrouver le path lié par infohash
                            existing = next(
                                (obj for obj in ep["paths"]
                                 if isinstance(obj, dict) and obj.get("infohash") == t.get("infohash")),
                                None
                            )
                            if existing:
                                kept_paths.append(existing)
                    ep["torrents"] = kept_torrents
                    ep["paths"] = kept_paths

            # Peupler les paths depuis chaque torrent intégral (append aux paths existants)
            for raw in integral_raws:
                if raw.get("files"):
                    _populate_paths_from_torrent(structure, raw, append=True)

            consolidated += 1
            continue

        # 2. Même torrent sur tous les épisodes d'une saison → season["torrents"]
        for season in structure["seasons"]:
            ep_torrents = [ep["torrents"][0] for ep in season["episodes"] if ep["torrents"]]
            ep_keys = {_torrent_key(t) for t in ep_torrents if _torrent_key(t)}
            # Compter les épisodes sortis OU ayant un torrent
            nb_eps = sum(1 for ep in season["episodes"] if ep.get("aired") or ep.get("torrents"))
            if (len(ep_keys) == 1 and len(ep_torrents) >= max(1, nb_eps - 2)
                    and nb_eps > 1 and not season["torrents"]):
                pack_key = next(iter(ep_keys))
                pack_raw = torrents_by_id.get(pack_key) or next(
                    (t for t in torrents if t.get("infohash") == pack_key), {}
                )
                pack_nyaa_id = pack_raw.get("nyaa_id")
                # Si une seule saison réelle → intégrale série plutôt que pack saison
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
                for ep in season["episodes"]:
                    ep["torrents"] = []
                    n = ep.get("episode_number")
                    p = _compute_path(ep, n, is_specials, folder_key, season_path_idx, path_idx, title_path_idx, 1)
                    ep["paths"] = [{"infohash": pack_infohash2, "path": p}] if p else []
                consolidated += 1

        # 1b. structure["torrents"] non-vide mais épisodes sans paths → peupler depuis le 1er torrent Nyaa
        if structure["torrents"] and not any(ep.get("paths")
                                              for s in structure["seasons"]
                                              for ep in s["episodes"]):
            pack_ref = next((t for t in structure["torrents"] if t.get("nyaa_id")), structure["torrents"][0])
            if pack_ref.get("nyaa_id"):
                pack_raw = torrents_by_id.get(pack_ref["nyaa_id"], {})
            else:
                pack_raw = torrents_by_infohash.get(pack_ref.get("infohash"), {})
            already_has_paths = any(ep.get("paths")
                                       for s in structure["seasons"]
                                       for ep in s["episodes"])
            if pack_raw.get("files"):
                _populate_paths_from_torrent(structure, pack_raw, append=already_has_paths)

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
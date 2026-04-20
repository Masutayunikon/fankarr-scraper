"""
ÉTAPE 3 - Fankai Torrent Matcher
=================================
Charge les torrents (Nyaa + manuels), récupère les séries depuis l'API
metadata.fankai.fr et associe chaque torrent à la meilleure série.

Input  : data/torrent_raw.json, data/manual_torrents.json
Output : data/torrents_matched.json, data/torrents_unmatched.json
"""

import re
import json
import time
import unicodedata
import requests
from pathlib import Path

METADATA_BASE   = "https://metadata.fankai.fr"
TORRENT_FILE    = "data/torrent_raw.json"
MANUAL_FILE     = "data/manual_torrents.json"
MATCHED_FILE    = "data/torrents_matched.json"
UNMATCHED_FILE  = "data/torrents_unmatched.json"
DELAY           = 0.3
SCORE_THRESHOLD = 40

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


# ── Normalisation texte ───────────────────────────────────────────────────────

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


# ── Détection du type de torrent ──────────────────────────────────────────────

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
    sm2 = re.search(r"sp[eé][cç]ial\s*#?\s*0*(\d+)", title, re.IGNORECASE)
    if sm2:
        r["type"] = "episode"; r["episode"] = int(sm2.group(1)); r["season"] = 0; return r
    return r


# ── Score de correspondance torrent ↔ série ───────────────────────────────────

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

def resolve_ttype(torrent, title_type):
    """Résout le type final d'un torrent (integral / season / episode / episode_range)."""
    ep_numbers = torrent.get("ep_numbers")
    force_type = torrent.get("force_type")

    if force_type == "integral":
        return {"type": "integral", "season": None, "episode": None, "ep_from": None, "ep_to": None}
    if torrent.get("force_season") is not None and ep_numbers and len(ep_numbers) == 1:
        return {"type": "episode", "episode": ep_numbers[0], "season": None, "ep_from": None, "ep_to": None}
    if torrent.get("force_season") is not None and ep_numbers and len(ep_numbers) > 1:
        return {"type": "episode_range", "season": None, "episode": None,
                "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
    if title_type["type"] == "season" and ep_numbers:
        return title_type
    if title_type["type"] == "integral" and ep_numbers:
        if len(ep_numbers) == 1:
            return {"type": "episode", "episode": ep_numbers[0], "season": None, "ep_from": None, "ep_to": None}
        return {"type": "episode_range", "season": None, "episode": None,
                "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
    if title_type["type"] in ("integral", "season"):
        return title_type
    if ep_numbers:
        if len(ep_numbers) == 1:
            return {"type": "episode", "episode": ep_numbers[0], "season": None, "ep_from": None, "ep_to": None}
        return {"type": "episode_range", "season": None, "episode": None,
                "ep_from": min(ep_numbers), "ep_to": max(ep_numbers)}
    return title_type


# ── Chargement des données ────────────────────────────────────────────────────

def load_torrents():
    p = Path(TORRENT_FILE)
    if not p.exists():
        print(f"[ERR] {TORRENT_FILE} introuvable → lancez d'abord s1_collect.py")
        return []
    torrents = json.loads(p.read_text(encoding="utf-8"))
    m = Path(MANUAL_FILE)
    if m.exists():
        manual = json.loads(m.read_text(encoding="utf-8"))
        manual = [t for t in manual if isinstance(t, dict)
                  and not any(k.startswith("_") for k in t) and t.get("title")]
        if manual:
            existing        = {t.get("infohash") for t in torrents if t.get("infohash")}
            existing_titles = {t.get("title")    for t in torrents if t.get("title")}
            added = 0
            for t in manual:
                if t.get("infohash") and t["infohash"] in existing: continue
                if t.get("title")    and t["title"]    in existing_titles: continue
                torrents.append(t); added += 1
            if added:
                print(f"[Manual] {added} torrent(s) manuel(s) chargé(s) depuis {MANUAL_FILE}")
    return torrents

def fetch_all_series():
    series    = api_get(f"{METADATA_BASE}/series") or []
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Étape 3 : Fankai Torrent Matcher ===\n")

    torrents = load_torrents()
    if not torrents:
        return
    print(f"[Torrents] {len(torrents)} chargés")

    print("[API] Récupération des séries...")
    series = fetch_all_series()
    print(f"[API] {len(series)} séries\n")

    matched   = []
    unmatched = []

    for torrent in torrents:
        ttitle = torrent.get("title", "")
        if not ttitle:
            continue

        best_serie, best_score = None, 0
        for s in series:
            sc = score_match(ttitle, s)
            if sc > best_score:
                best_score = sc; best_serie = s

        if best_serie is None or best_score < SCORE_THRESHOLD:
            unmatched.append({
                "title":      ttitle,
                "score":      best_score,
                "best_match": best_serie.get("title") if best_serie else None,
            })
            continue

        title_type = detect_type(ttitle)
        ttype      = resolve_ttype(torrent, title_type)

        matched.append({
            "torrent":     torrent,
            "serie_id":    best_serie["id"],
            "serie_title": best_serie.get("title"),
            "score":       best_score,
            "ttype":       ttype,
        })

    print(f"[Match] {len(matched)}/{len(torrents)} torrents associés à une série")
    print(f"[Match] {len(unmatched)} non associés (score < {SCORE_THRESHOLD})\n")

    Path(MATCHED_FILE).write_text(
        json.dumps(matched, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    Path(UNMATCHED_FILE).write_text(
        json.dumps(unmatched, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"[Output] {MATCHED_FILE}  ({len(matched)} entrées)")
    print(f"[Output] {UNMATCHED_FILE}  ({len(unmatched)} entrées)")

    if unmatched:
        print(f"\n⚠️  Premiers non-matchés :")
        for u in unmatched[:20]:
            print(f"   [{u['score']:3d}] {u['title'][:80]}")
            if u.get("best_match"):
                print(f"         → {u['best_match']}")

    print("\n✅ Étape 3 terminée !")


if __name__ == "__main__":
    main()

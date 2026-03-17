"""
ÉTAPE 2 - Fankai Torrent Name Parser
======================================
Parse les noms de torrents Nyaa pour extraire :
  - type     : "episode" | "pack_saison" | "pack_integrale" | "pack_episodes"
  - episodes : liste d'ints (ex: [1, 2, 3]) ou None si pack non borné
  - saisons  : liste d'ints ou None
  - groupe   : auteur/groupe (ex: "Brazh", "Johker")

Le matching série se fait via show_title de l'API Fankai.

Cas couverts (d'après torrent_names.txt) :
  A) [Auteur] Show Title - NUM - Titre ep - qualité          → épisode simple
  B) Show Title - NUM - Titre ep - qualité                   → épisode simple (sans auteur)
  C) [Auteur] Show Title NUM - Titre ep - qualité            → épisode simple (pas de tiret avant num)
  D) [Auteur] Show Title - Saga/Arc NUM - Titre              → épisode simple (saga/arc)
  E) [Auteur] Show Title - Saison N - ...                    → pack 1 saison
  F) [Auteur] Show Title - Saisons N/M/P/... - ...           → pack multi-saisons
  G) [Auteur] Show Title - Saisons N/M + extra               → pack multi-saisons
  H) [Auteur] Show Title Films N à M                         → pack épisodes (films = eps)
  I) [Auteur] Show Title - INTEGRALE (N films)               → pack intégrale
  J) [Auteur] Show Title - Saison N - N Films                → pack saison avec films
  K) Show Title NUM LANG - Auteur  (style Roro)              → épisode simple
  L) Show Title Films N à M - LANG - Auteur (style Roro)     → pack épisodes
  M) [Auteur] Show Title.qualité...  (sans num du tout)      → pack intégrale / inconnu
  N) Show Title - Saga 1/2/3/4/5/6 - qualité                → pack intégrale
"""

import re
from dataclasses import dataclass, field

INPUT_FILE  = "data/torrent_raw.json"
OUTPUT_FILE = "data/torrent_parsed.json"
NAME_FILE = "data/torrent_names.txt"

from pathlib import Path
Path("data").mkdir(exist_ok=True)


# ─── Structures ───────────────────────────────────────────────────────────────

@dataclass
class ParsedTorrent:
    raw_name    : str
    show_title  : str | None = None   # matché via l'API Fankai
    groupe      : str | None = None
    type        : str        = "unknown"
    # "episode" | "pack_saison" | "pack_integrale" | "pack_episodes"
    episodes    : list[int]  = field(default_factory=list)   # vide = non borné
    saisons     : list[int]  = field(default_factory=list)   # vide = inconnue
    torrent_url : str | None = None
    magnet      : str | None = None
    infohash    : str | None = None


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _expand_range(start: int, end: int) -> list[int]:
    return list(range(start, end + 1))

def _parse_slash_list(s: str) -> list[int]:
    """'1/2/3/4/5/6' → [1, 2, 3, 4, 5, 6]"""
    parts = s.split("/")
    result = []
    for p in parts:
        p = p.strip()
        if p.isdigit():
            result.append(int(p))
    return result


# ─── Patterns regex (ordonnés du plus spécifique au plus général) ─────────────

# Groupe optionnel en début de nom : [Brazh], [Johker], etc.
_GROUP_RE = re.compile(r"^\[([^\]]+)\]\s*")

# Qualité / codec en fin (pour nettoyer)
_QUALITY_RE = re.compile(
    r"\s*[-–]?\s*(?:V\d+\s*)?(?:(?:720|1080|2160)p|x264|x265|HEVC|AAC|MULTI|VF|VOSTFR|FRENCH|"
    r"\bENG\b|\bFRE\b|Multiple\s+Subtitle.*?|Multi(?:sub|audio|subs)?|\.mkv|\.mp4).*$",
    re.IGNORECASE,
)

def _strip_quality(s: str) -> str:
    return _QUALITY_RE.sub("", s).strip(" .-")


# Les patterns sont des (nom, regex, handler)
# Le handler reçoit le match + le reste du nom et retourne un dict partiel

PATTERNS: list[tuple[str, re.Pattern, callable]] = []

def _reg(name: str, pattern: str, flags=re.IGNORECASE):
    def decorator(fn):
        PATTERNS.append((name, re.compile(pattern, flags), fn))
        return fn
    return decorator


# ── A/B/C : épisode simple numéroté ──────────────────────────────────────────
# "[Auteur] Show Title - 056 - Titre"  ou  "Show Title - 056 - Titre"
# "[Auteur] Show Title 01 - Titre"  (pas de tiret avant le num)
# "[Auteur] Show Title NUM (Fan-kai) - Titre"  (Inazuma style)

@_reg("episode_simple_tiret", r"[-–]\s*(\d{2,3})\s*[-–]")
def _ep_simple_tiret(m, **_):
    return {"type": "episode", "episodes": [int(m.group(1))]}

@_reg("episode_simple_notir", r"\b(\d{2,3})\s*\(Fan-[Kk]a[iï]\)\s*[-–]")
def _ep_inazuma(m, **_):
    return {"type": "episode", "episodes": [int(m.group(1))]}

@_reg("episode_simple_space", r"(?:^|\s)(\d{2,3})\s+(?:VOSTFR|MULTI|VF)\b")
def _ep_roro_style(m, **_):
    return {"type": "episode", "episodes": [int(m.group(1))]}

# ── D : Saga / Arc numéroté (épisode unique) ──────────────────────────────────
# "- Saga 01 -"  "- Arc 1 -"

@_reg("pack_saga_arc", r"[-–]\s*(?:Saga|Arc)\s+(\d+)\s*[-–]", re.IGNORECASE)
def _ep_saga(m, **_):
    return {"type": "pack_saison", "saisons": [int(m.group(1))]}

# ── N : Saga multi (slash) → pack_integrale ───────────────────────────────────
# "Saga 1/2/3/4/5/6"  → pack intégrale
# Doit être avant les patterns épisode pour ne pas matcher le dernier chiffre

@_reg("pack_saga_slash",
      r"[-–\s]\s*(?:Saga|Partie|Part)\s+[\d]+(?:/[\d]+)+",
      re.IGNORECASE)
def _pack_saga_slash(m, **_):
    return {"type": "pack_integrale"}


# ── E/F/G : Packs saisons ────────────────────────────────────────────────────
# Multi-saisons en premier (plus spécifiques) puis saison unique

# "- Saisons 1/2/3/4/5/6 -"   "- Saisons 1/2 + Code White"
@_reg("pack_saisons_slash",
      r"[-–\s]\s*(?:Saisons|Seasons)\s+([\d]+(?:/[\d]+)+)(?:\s*\+[^-\n]*)?",
      re.IGNORECASE)
def _pack_saisons_slash(m, **_):
    return {"type": "pack_saison", "saisons": _parse_slash_list(m.group(1))}

# "- Seasons 1 & 2 -"  "- Saisons 1 et 2 -"
@_reg("pack_saisons_amp",
      r"[-–\s]\s*(?:Saisons?|Seasons?)\s+(\d+)\s*(?:[&]|\bet\b)\s*(\d+)",
      re.IGNORECASE)
def _pack_saisons_amp(m, **_):
    return {"type": "pack_saison", "saisons": [int(m.group(1)), int(m.group(2))]}

# "- Saison 01 -"  "- Season 3 -"  "- Saison 1 - 8 Films"
@_reg("pack_saison_single",
      r"[-–\s]\s*(?:Saison|Season)s?\s+(\d+)\s*(?=[-–\s]|$|\s*-\s*\d+\s*Films)(?!\s*[/&])",
      re.IGNORECASE)
def _pack_saison(m, **_):
    return {"type": "pack_saison", "saisons": [int(m.group(1))]}


# ── H/L : Pack épisodes bornés (films N à M) ─────────────────────────────────
# "Films 01 à 10"  "Films 1 à 5"  "Films 1 à 15"

@_reg("pack_films_range",
      r"Films?\s+(\d+)\s+[àa]\s+(\d+)",
      re.IGNORECASE)
def _pack_films_range(m, **_):
    return {
        "type"    : "pack_episodes",
        "episodes": _expand_range(int(m.group(1)), int(m.group(2))),
    }


# ── I : Intégrale ─────────────────────────────────────────────────────────────
# "INTEGRALE (32 films)"

@_reg("pack_integrale", r"\bINTEGRALE\b", re.IGNORECASE)
def _pack_integrale(m, **_):
    return {"type": "pack_integrale"}


# ── J : Saison + N Films (Frieren style) ──────────────────────────────────────
# "- Saison 1 - 8 Films -"   → pack_saison avec saison connue, épisodes non bornés

@_reg("pack_saison_n_films",
      r"[-–\s]\s*(?:Saison|Season)\s+(\d+)\s*[-–]\s*(\d+)\s*Films",
      re.IGNORECASE)
def _pack_saison_films(m, **_):
    n_films = int(m.group(2))
    return {
        "type"    : "pack_saison",
        "saisons" : [int(m.group(1))],
        "episodes": _expand_range(1, n_films),
    }


# ── K : épisode num + tiret après (pattern large, doit être après les packs) ──
# "[Auteur] Show Title 01 - Titre"  (numéro collé au titre, tiret après)
# ex: [Brazh] Boruto Kaï 01 - C'est mon histoire
#     [Triggerforce] Naruto Shippuden Yabaï 16 - Les Kage...
#     [Triggerforce] Tokyo Revengers Henshu 03 - Revanche
# IMPORTANT : placé après les packs pour ne pas les court-circuiter
# (?<!/) empêche de matcher un chiffre précédé d'un slash (ex: "1/2/3/4/5/6 - 1080p")

@_reg("episode_num_tiret_apres", r"(?<!/)\b(\d{1,3})\s*[-–]\s+\w")
def _ep_num_tiret_apres(m, **_):
    return {"type": "episode", "episodes": [int(m.group(1))]}


# ── L : style Roro - épisode seul sans tiret ──────────────────────────────────
# "Nichijou Henshû 01 VOSTFR - Roro"

@_reg("episode_roro_notiret",
      r"\b(\d{2,3})\s+(?:VOSTFR|MULTI|VF|FRENCH)\b")
def _ep_roro(m, **_):
    return {"type": "episode", "episodes": [int(m.group(1))]}


# ─── Moteur de parsing ────────────────────────────────────────────────────────

def parse_torrent_name(raw: str) -> ParsedTorrent:
    result = ParsedTorrent(raw_name=raw)

    # 1. Extraire le groupe
    gm = _GROUP_RE.match(raw)
    if gm:
        result.groupe = gm.group(1)

    # 2. Appliquer les patterns dans l'ordre
    for pat_name, pat_re, handler in PATTERNS:
        m = pat_re.search(raw)
        if m:
            data = handler(m)
            result.type     = data.get("type",     result.type)
            result.episodes = data.get("episodes", result.episodes)
            result.saisons  = data.get("saisons",  result.saisons)
            # On s'arrête au premier match de type (sauf si on peut raffiner)
            if result.type != "unknown":
                break

    # 3. Si rien n'a matché → pack_integrale par défaut
    #    (ex: [Triggerforce] Tokyo Revengers Henshū.1080p.MULTI...)
    if result.type == "unknown":
        result.type = "pack_integrale"

    return result


# ─── Matching avec show_title Fankai ─────────────────────────────────────────

def match_show_title(torrent_name: str, show_titles: list[str]) -> str | None:
    """
    Trouve le show_title Fankai le plus long qui apparaît dans le nom du torrent.
    On retire d'abord le groupe [Auteur] et la qualité pour ne garder que le titre.
    """
    # Nettoyer le nom
    clean = _GROUP_RE.sub("", torrent_name)
    clean = _strip_quality(clean)

    best     = None
    best_len = 0

    for title in show_titles:
        # Recherche insensible à la casse, avec les caractères spéciaux échappés
        pattern = re.compile(re.escape(title), re.IGNORECASE)
        if pattern.search(clean):
            if len(title) > best_len:
                best     = title
                best_len = len(title)

    return best


# ─── Test sur torrent_names.txt ───────────────────────────────────────────────

if __name__ == "__main__":
    import json

    # Charger les noms
    with open(NAME_FILE, encoding="utf-8") as f:
        names = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    # Charger torrent_raw.json pour récupérer torrent_url / magnet / infohash
    raw_index: dict[str, dict] = {}
    try:
        with open(INPUT_FILE, encoding="utf-8") as f:
            raw_list = json.load(f)
        for t in raw_list:
            title = t.get("title") or t.get("name") or t.get("Name") or ""
            if title:
                raw_index[title] = t
        print(f"[Raw] {len(raw_index)} entrées chargées depuis torrent_raw.json")
    except FileNotFoundError:
        print("[Raw] torrent_raw.json introuvable, torrent_url/magnet non propagés")

    # Charger les show_titles depuis l'API Fankai (si dispo), sinon mock
    try:
        import requests
        resp = requests.get("https://metadata.fankai.fr/series", timeout=10)
        show_titles = [s["show_title"] for s in resp.json()]
        print(f"[Fankai] {len(show_titles)} séries chargées")
    except Exception as e:
        print(f"[Fankai] Impossible de charger l'API ({e}), mode mock")
        show_titles = []

    # Parser + afficher les résultats
    results   = []
    unknowns  = []

    for name in names:
        p = parse_torrent_name(name)
        if show_titles:
            p.show_title = match_show_title(name, show_titles)
        results.append(p)

        if p.type == "unknown":
            unknowns.append(name)

        # Propager torrent_url / magnet / infohash depuis torrent_raw.json
        raw = raw_index.get(name)
        if raw:
            p.torrent_url = raw.get("torrent_url") or raw.get("torrent") or None
            p.magnet      = raw.get("magnet") or None
            p.infohash    = raw.get("infohash") or None

    # Résumé
    from collections import Counter
    types = Counter(r.type for r in results)
    print("\n=== RÉSUMÉ ===")
    for t, c in sorted(types.items()):
        print(f"  {t:20s} : {c}")

    print(f"\n=== CAS INCONNUS ({len(unknowns)}) ===")
    for u in unknowns:
        print(f"  {u}")

    print("\n=== DÉTAIL ===")
    for r in results:
        ep_str  = str(r.episodes) if r.episodes else "[]"
        sai_str = str(r.saisons)  if r.saisons  else "[]"
        print(
            f"  [{r.type:20s}] ep={ep_str:20s} sai={sai_str:10s}"
            f" grp={str(r.groupe):20s}  {r.raw_name[:60]}"
        )

    # Sauvegarder en JSON
    out = [
        {
            "raw"        : r.raw_name,
            "show_title" : r.show_title,
            "groupe"     : r.groupe,
            "type"       : r.type,
            "episodes"   : r.episodes,
            "saisons"    : r.saisons,
            "torrent_url": r.torrent_url,
            "magnet"     : r.magnet,
            "infohash"   : r.infohash,
        }
        for r in results
    ]
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n✅ Résultats sauvegardés dans torrent_parsed.json")
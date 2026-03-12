"""
ÉTAPE 5 - Fankai Final Resolver
=================================
Pour chaque torrent enrichi (torrent_enriched.json), résout les file_ep_numbers
en episode_id Fankai via l'API, avec :
  - Détection des films spéciaux (numéros X,5) → saison 0
  - Fallback sur série liée (ex: One Piece Yabai → One Piece Kaï)
  - Cache Fankai réutilisé depuis step3

Input  : torrent_enriched.json
Output : torrent_final.json
"""

import re
import json
import time
import requests
from pathlib import Path


# ─── Config ───────────────────────────────────────────────────────────────────

FANKAI_BASE   = "https://metadata.fankai.fr"
INPUT_FILE  = "data/torrent_enriched.json"
OUTPUT_FILE = "data/torrent_final.json"
CACHE_FILE  = "data/fankai_cache.json"
DELAY         = 0.2

from pathlib import Path
Path("data").mkdir(exist_ok=True)

# Liens entre séries : si épisode introuvable dans serie_id A → essayer B
# Format : {serie_id_principal: serie_id_fallback}
SERIE_FALLBACK = {
    # One Piece Yabai (87) → fallback One Piece Kaï (88)
    # Note : les IDs sont à confirmer selon ton API
    93: 92,
    92: 93,
}


# ─── Cache (réutilisé depuis step3) ──────────────────────────────────────────

class Cache:
    def __init__(self, path: str):
        self.path = Path(path)
        self.data: dict = {}
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
                print(f"[Cache] {len(self.data)} entrées chargées")
            except Exception:
                self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def __contains__(self, key):
        return key in self.data


# ─── Client API Fankai ────────────────────────────────────────────────────────

class FankaiClient:
    def __init__(self, cache: Cache):
        self.cache   = cache
        self.session = requests.Session()

    def _get(self, path: str):
        if path in self.cache:
            return self.cache.get(path)
        try:
            r = self.session.get(f"{FANKAI_BASE}{path}", timeout=15)
            r.raise_for_status()
            data = r.json()
            self.cache.set(path, data)
            time.sleep(DELAY)
            return data
        except Exception as e:
            print(f"  [!] API {path}: {e}")
            return None

    def get_seasons(self, serie_id: int) -> list[dict]:
        data = self._get(f"/series/{serie_id}/seasons")
        return data.get("seasons", []) if isinstance(data, dict) else []

    def get_episodes(self, season_id: int) -> list[dict]:
        data = self._get(f"/seasons/{season_id}/episodes")
        return data.get("episodes", []) if isinstance(data, dict) else []


# ─── Resolver avec cache mémoire ─────────────────────────────────────────────

class Resolver:
    def __init__(self, client: FankaiClient):
        self.client          = client
        self._seasons_cache  : dict[int, list[dict]] = {}
        self._episodes_cache : dict[int, list[dict]] = {}

    def _seasons(self, serie_id: int) -> list[dict]:
        if serie_id not in self._seasons_cache:
            self._seasons_cache[serie_id] = self.client.get_seasons(serie_id)
        return self._seasons_cache[serie_id]

    def _episodes(self, season_id: int) -> list[dict]:
        if season_id not in self._episodes_cache:
            self._episodes_cache[season_id] = self.client.get_episodes(season_id)
        return self._episodes_cache[season_id]

    def find_episode(self, serie_id: int, ep_number: int,
                     season_number: int | None = None,
                     exclude_season_zero: bool = False) -> dict | None:
        """
        Cherche episode_number dans toutes les saisons de serie_id.
        Si season_number fourni, cherche d'abord dans cette saison.
        Si exclude_season_zero=True, ignore la saison 0 (spéciaux/films).
        """
        seasons = self._seasons(serie_id)

        # Exclure saison 0 pour les épisodes normaux
        if exclude_season_zero:
            seasons = [s for s in seasons if s.get("season_number", 0) != 0]

        # Priorité : saison demandée en premier
        if season_number is not None:
            ordered = sorted(seasons,
                key=lambda s: 0 if s.get("season_number") == season_number else 1)
        else:
            ordered = seasons

        for season in ordered:
            for ep in self._episodes(season["id"]):
                if ep.get("episode_number") == ep_number:
                    return {
                        "serie_id"      : serie_id,
                        "season_id"     : season["id"],
                        "season_number" : season.get("season_number"),
                        "episode_id"    : ep["id"],
                        "episode_number": ep_number,
                    }
        return None

    def resolve_ep(self, serie_id: int, ep_number: int,
                   season_number: int | None = None,
                   is_special: bool = False) -> dict | None:
        """Cherche l'épisode, avec fallback sur série liée si introuvable."""
        # Les épisodes non-spéciaux ne doivent jamais matcher la saison 0
        exclude_s0 = not is_special
        result = self.find_episode(serie_id, ep_number, season_number,
                                   exclude_season_zero=exclude_s0)
        if result:
            return result

        # Fallback série liée
        fallback_id = SERIE_FALLBACK.get(serie_id)
        if fallback_id:
            result = self.find_episode(fallback_id, ep_number, season_number,
                                       exclude_season_zero=exclude_s0)
            if result:
                print(f"    ↩️  ep {ep_number} trouvé via fallback serie {fallback_id}")
                return result

        return None


# ─── Index des séries connues (titre normalisé → serie_id) ──────────────────
# Utilisé pour détecter la série depuis le nom du fichier
# Format : liste de (pattern_regex, serie_id)
# L'ordre compte : plus spécifique en premier
SERIE_PATTERNS: list[tuple[re.Pattern, int]] = [
    # One Piece Yabai avant Kaï (plus spécifique)
    (re.compile(r"One\s+Piece\s+Yaba[iï]", re.IGNORECASE), 93),
    (re.compile(r"One\s+Piece\s+Ka[iï]",   re.IGNORECASE), 92),
]

def detect_serie_from_filename(filename: str, default_serie_id: int) -> int:
    """
    Détecte la série depuis le nom du fichier.
    Retourne default_serie_id si aucun pattern ne matche.
    """
    for pattern, serie_id in SERIE_PATTERNS:
        if pattern.search(filename):
            return serie_id
    return default_serie_id


# ─── Parsing des numéros de films spéciaux (X,5 ou X.5) ─────────────────────

_SPECIAL_RE = re.compile(
    r"(?:Henshu|Henshū|Ka[iï]|Yaba[iï])\s+(\d+)[,.]5\b",
    re.IGNORECASE
)
_EPISODE_RE = re.compile(
    r"(?:Henshu|Henshū|Ka[iï]|Yaba[iï])\s+(\d+)\b",
    re.IGNORECASE
)

# S00E01 → special saison 0
_S00_RE = re.compile(r"S00E(\d+)", re.IGNORECASE)
# S01E05 → épisode normal (saison non-zero)
_SxxE_RE = re.compile(r"S(\d+)E(\d+)", re.IGNORECASE)

# Film spécial numéroté X,5 → numéro d'épisode en saison 0
# Format : (serie_id, numero_x) → episode_number_saison0
# Ex: MHA 7,5 = "Film 1 - Two Heroes" = S00E01
SPECIAL_EP_MAP: dict[tuple[int, int], int] = {
    (6, 7):  1,  # MHA 7,5  → Two Heroes         (S00E01, id=26)
    (6, 19): 2,  # MHA 19,5 → Heroes Rising       (S00E02, id=25)
    (6, 20): 3,  # MHA 20,5 → World Heroes Mission (S00E03, id=27)
}

def parse_file_episode(filename: str, serie_id: int = 0) -> tuple[int | None, bool, bool]:
    """
    Retourne (episode_number, is_special, use_season0_map).
    is_special = True si c'est un special (S00Exx) ou film bonus (7,5 / 19,5 etc.)
    use_season0_map = True si le numéro retourné est déjà le numéro de la saison 0
                      (via SPECIAL_EP_MAP), False sinon.
    """
    # S00E01 → special saison 0, numéro direct
    m = _S00_RE.search(filename)
    if m:
        return int(m.group(1)), True, True

    # Film spécial : "Henshū 7,5" ou "Henshū 19.5"
    m = _SPECIAL_RE.search(filename)
    if m:
        base_num = int(m.group(1))
        # Chercher dans la map pour convertir en numéro saison 0
        s0_num = SPECIAL_EP_MAP.get((serie_id, base_num))
        if s0_num is not None:
            return s0_num, True, True
        # Pas dans la map → garder le numéro brut, chercher en saison 0
        return base_num, True, False

    # S01E05 → épisode normal, on utilise le numéro d'épisode global
    m = _SxxE_RE.search(filename)
    if m and int(m.group(1)) > 0:
        return int(m.group(2)), False, False

    # Épisode normal via nom de série
    m = _EPISODE_RE.search(filename)
    if m:
        return int(m.group(1)), False, False

    # Fallback : numéro précédé d'un tiret
    m = re.search(r"[-–]\s*(\d{1,3})\s*[-–]", filename)
    if m:
        return int(m.group(1)), False, False

    return None, False, False


# ─── Résolution complète d'une saison (pack_saison déjà résolu) ─────────────

def resolve_season_episodes(torrent: dict, resolver: Resolver) -> dict:
    """
    Pour les pack_saison qui ont déjà un season_id mais pas de resolved_episodes,
    récupère tous les épisodes de toutes les saisons depuis l'API.
    Utilise resolved_seasons si présent (multi-saisons), sinon season_id seul.
    """
    serie_id = torrent.get("serie_id")

    # Construire la liste des saisons à résoudre
    resolved_seasons = torrent.get("resolved_seasons") or []
    if resolved_seasons:
        seasons_to_resolve = resolved_seasons  # [{"season_id": x, "season_number": y}, ...]
    elif torrent.get("season_id"):
        seasons_to_resolve = [{"season_id": torrent["season_id"],
                                "season_number": torrent.get("season_number")}]
    else:
        return torrent

    resolved = []
    for s in seasons_to_resolve:
        season_id     = s["season_id"]
        season_number = s["season_number"]
        episodes = resolver._episodes(season_id)
        for ep in episodes:
            resolved.append({
                "serie_id"      : serie_id,
                "season_id"     : season_id,
                "season_number" : season_number,
                "episode_id"    : ep["id"],
                "episode_number": ep.get("episode_number"),
                "filename"      : None,
                "is_special"    : False,
            })

    if resolved:
        torrent["resolved_episodes"] = resolved
    return torrent


# ─── Résolution des épisodes d'un torrent enrichi ────────────────────────────

def resolve_torrent_episodes(torrent: dict, resolver: Resolver) -> dict:
    """
    Résout les file_ep_numbers (ou torrent_files) d'un torrent enrichi
    en episode_id Fankai.
    """
    serie_id = torrent.get("serie_id")
    if not serie_id:
        return torrent

    torrent_files = torrent.get("torrent_files", [])
    if not torrent_files:
        # Fallback sur file_ep_numbers si torrent_files absent
        ep_numbers = torrent.get("file_ep_numbers", [])
        torrent_files = [{"num": n, "filename": "", "path": []} for n in ep_numbers]

    resolved_episodes = []
    unresolved        = []

    for f in torrent_files:
        filename    = f.get("filename", "")
        ep_num      = f.get("num")
        file_season = f.get("season_number")  # issu du dossier "Saison 0/1/2..."

        # Si season_number absent, tenter de le déduire depuis le path
        if file_season is None:
            for folder in f.get("path", [])[:-1]:
                m = re.search(r"saison\s*(\d+)", folder, re.IGNORECASE)
                if m:
                    file_season = int(m.group(1))
                    break
        is_special  = False

        # Ré-parser le filename pour détecter les spéciaux ET la série
        file_serie_id = serie_id
        use_s0_map = False
        if filename:
            ep_num_parsed, is_special, use_s0_map = parse_file_episode(filename, serie_id)
            # Si on a détecté un numéro depuis le filename, on l'utilise
            if ep_num_parsed is not None:
                ep_num = ep_num_parsed
            # Détecter la série depuis le nom du fichier (ex: Yabai vs Kaï)
            file_serie_id = detect_serie_from_filename(filename, serie_id)

        # Si le fichier est dans "Saison 0" (dossier ou bonus détecté en s4) → forcer special
        if file_season == 0:
            is_special = True
            use_s0_map = False
            # ep 00 → ep 1 (l'API numérote à partir de 1)
            if ep_num == 0:
                ep_num = 1

        if ep_num is None:
            continue

        # Spéciaux via map → chercher directement en saison 0 avec le numéro converti
        # Spéciaux sans map → chercher en saison 0 en priorité (numéro brut)
        # Normaux → exclure saison 0
        if use_s0_map:
            season_hint = 0
        elif is_special:
            season_hint = 0
        else:
            season_hint = None

        result = resolver.resolve_ep(file_serie_id, ep_num, season_hint,
                                     is_special=is_special)

        if result:
            result["filename"]   = filename
            result["is_special"] = is_special
            resolved_episodes.append(result)
        else:
            unresolved.append({"episode_number": ep_num, "filename": filename})

    # Dédupliquer par episode_id (un même épisode peut apparaître dans 2 fichiers)
    seen     = set()
    deduped  = []
    for ep in resolved_episodes:
        eid = ep["episode_id"]
        if eid not in seen:
            seen.add(eid)
            deduped.append(ep)

    torrent["resolved_episodes"] = deduped
    torrent["unresolved_files"]  = unresolved

    if unresolved:
        torrent["resolve_status"] = "partial"
        print(f"    ⚠️  {len(unresolved)} fichiers non résolus: "
              f"{[u['episode_number'] for u in unresolved]}")
    else:
        torrent["resolve_status"] = "ok"

    # Mettre à jour season_id avec le premier épisode résolu
    if deduped:
        torrent["season_id"]     = deduped[0]["season_id"]
        torrent["season_number"] = deduped[0]["season_number"]

    return torrent


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        torrents = json.load(f)
    print(f"[Input] {len(torrents)} torrents")

    cache    = Cache(CACHE_FILE)
    client   = FankaiClient(cache)
    resolver = Resolver(client)

    enriched_count = sum(1 for t in torrents if t.get("torrent_files"))
    print(f"[Step5] {enriched_count} torrents avec fichiers à résoudre\n")

    for i, torrent in enumerate(torrents, 1):
        # Uniquement les torrents enrichis par step4 sans résolution complète
        if not torrent.get("torrent_files"):
            continue
        if torrent.get("resolve_status") == "ok" and torrent.get("resolved_episodes"):
            continue

        raw = torrent.get("raw", "")
        print(f"[{i:3d}] {raw[:65]}")
        print(f"       serie={torrent.get('serie_id')} type={torrent.get('type')}")

        resolve_torrent_episodes(torrent, resolver)
        ep_ids = [e["episode_id"] for e in torrent.get("resolved_episodes", [])]
        print(f"       → {len(ep_ids)} épisodes résolus: {ep_ids[:10]}"
              f"{'...' if len(ep_ids) > 10 else ''}")

    # Passe 2 : pack_saison avec season_id mais sans resolved_episodes
    print("\n--- Passe 2 : résolution des épisodes des pack_saison ---")
    for i, torrent in enumerate(torrents, 1):
        if torrent.get("type") != "pack_saison":
            continue
        if torrent.get("resolved_episodes"):
            continue
        if not torrent.get("season_id"):
            continue

        raw = torrent.get("raw", "")
        print(f"[{i:3d}] {raw[:65]}")
        resolve_season_episodes(torrent, resolver)
        ep_ids = [e["episode_id"] for e in torrent.get("resolved_episodes", [])]
        print(f"       → {len(ep_ids)} épisodes résolus: {ep_ids[:10]}"
              f"{'...' if len(ep_ids) > 10 else ''}")

    # Résumé
    from collections import Counter
    statuses = Counter(t.get("resolve_status", "?") for t in torrents)
    print("\n=== RÉSUMÉ FINAL ===")
    for s, c in sorted(statuses.items()):
        print(f"  {s:20s} : {c}")

    # Compter les torrents avec episode_ids
    with_eps = sum(1 for t in torrents if t.get("resolved_episodes"))
    print(f"\n  Torrents avec episodes résolus : {with_eps}/{len(torrents)}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(torrents, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Résultat final → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
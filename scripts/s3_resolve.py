"""
ÉTAPE 3 - Fankai Resolver
==========================
Pour chaque torrent parsé (torrent_parsed.json), résout via l'API Fankai :
  - serie_id   : match show_title
  - season_id  : via season_number  (pack_saison)
                 via episode_number (episode → on trouve la saison en même temps)
  - episode_id : via episode_number (episode / pack_episodes)

Cache disque dans fankai_cache.json pour ne pas re-appeler l'API à chaque run.

Input  : torrent_parsed.json  (produit par step2)
Output : torrent_resolved.json
"""

import re
import json
import time
import requests
from pathlib import Path

# ─── Config ───────────────────────────────────────────────────────────────────

FANKAI_BASE  = "https://metadata.fankai.fr"
INPUT_FILE  = "data/torrent_parsed.json"
OUTPUT_FILE = "data/torrent_resolved.json"
CACHE_FILE  = "data/fankai_cache.json"
DELAY        = 0.2

from pathlib import Path
Path("data").mkdir(exist_ok=True)


# ─── Cache disque ─────────────────────────────────────────────────────────────

class Cache:
    def __init__(self, path: str):
        self.path = Path(path)
        self.data: dict = {}
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
                print(f"[Cache] Chargé : {len(self.data)} entrées depuis {path}")
            except Exception:
                self.data = {}

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, value):
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
            print(f"  [!] API error {path}: {e}")
            return None

    def get_all_series(self) -> list[dict]:
        data = self._get("/series")
        return data if isinstance(data, list) else []

    def get_seasons(self, serie_id: int) -> list[dict]:
        data = self._get(f"/series/{serie_id}/seasons")
        if isinstance(data, dict):
            return data.get("seasons", [])
        return []

    def get_episodes(self, season_id: int) -> list[dict]:
        data = self._get(f"/seasons/{season_id}/episodes")
        if isinstance(data, dict):
            return data.get("episodes", [])
        return []


# ─── Matching show_title ──────────────────────────────────────────────────────

_GROUP_RE   = re.compile(r"^\[([^\]]+)\]\s*")
_QUALITY_RE = re.compile(
    r"\s*[-–]?\s*(?:V\d+\s*)?(?:(?:720|1080|2160)p|x264|x265|HEVC|AAC|MULTI|VF|VOSTFR|FRENCH|"
    r"\bENG\b|\bFRE\b|Multiple\s+Subtitle.*?|Multi(?:sub|audio|subs)?|\.mkv|\.mp4).*$",
    re.IGNORECASE,
)

import unicodedata

# Parenthèses parasites : (Fan-kai), (Fan-Kaï), (Partie 1), etc.
_PARENS_RE = re.compile(r"\(Fan-[Kk]a[iï]\)", re.IGNORECASE)

def _clean_name(name: str) -> str:
    s = _GROUP_RE.sub("", name)
    s = _PARENS_RE.sub("", s)
    s = _QUALITY_RE.sub("", s)
    return s.strip(" .-")

def _normalize(s: str) -> str:
    """Normalisation agressive pour comparaison floue :
    - supprime les accents (ū→u, î→i, ï→i)
    - supprime la ponctuation parasite (: ! -)
    - normalise les espaces multiples
    """
    # Supprimer les accents
    s = "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    )
    # Supprimer ponctuation parasite qui varie entre torrent et API
    s = re.sub(r"!+", "", s)          # Haikyu!! → haikyu
    s = re.sub(r"[:：]", " ", s)      # Re:Zero → re zero, Kaguya : → kaguya
    s = re.sub(r"\s+", " ", s)        # espaces multiples
    return s.strip()

# Aliases : torrent_prefix_normalisé → show_title exact dans l'API
# Utilisé quand la normalisation seule ne suffit pas
SERIE_ALIASES: dict[str, str] = {
    "haikyu":              "Haikyuu!! Henshū",
    "inazuma eleven":      "Inazuma Eleven Fan-Cut",
    "kaguya-sama":         "Kaguya-sama : Love is War Henshū",
    "dragon ball gt":      "Dragon Ball GT Fan-Cut",
    "dragon ball super":   "Dragon Ball Super Kaï",
    "dragon ball yabai":   "Dragon Ball Yabai",
    "dragon ball z yabai": "Dragon Ball Z Yabai",
    "hunter x hunter":  "Hunter x Hunter Kaï (2011)",

}

def find_serie(torrent_name: str, series: list[dict]) -> dict | None:
    clean      = _clean_name(torrent_name)
    clean_norm = _normalize(clean)

    # Stratégie 0 : alias explicite — teste si le nom normalisé commence par un alias connu
    for alias_norm, target_title in SERIE_ALIASES.items():
        if clean_norm.startswith(alias_norm):
            for serie in series:
                if serie.get("title") == target_title or serie.get("show_title") == target_title:
                    return serie
    best       = None
    best_len   = 0
    for serie in series:
        title = serie.get("show_title", "") or ""
        if not title:
            continue
        title_norm = _normalize(title)

        # Stratégie 1 : match exact du show_title complet (ex: "One Piece Yabai")
        if re.search(re.escape(title_norm), clean_norm):
            if len(title) > best_len:
                best     = serie
                best_len = len(title)
            continue

        # Stratégie 2 : match par préfixe de mots (ex: "Inazuma Eleven Kai"
        # dans "Inazuma Eleven 19" → on teste mot par mot jusqu'au premier échec)
        title_words = title_norm.split()
        clean_words = clean_norm.split()
        if len(title_words) >= 2:
            match_count = 0
            for tw, cw in zip(title_words, clean_words):
                if tw == cw:
                    match_count += 1
                else:
                    break
            if match_count >= len(title_words) - 1 and match_count >= 2:
                last_word = title_words[-1]
                # Si le dernier mot du titre n'a pas matché, vérifier que
                # le mot à cette position dans clean n'est pas un mot incompatible
                # Ex: "dragon ball z recut" ne doit pas matcher "dragon ball z yabai"
                if match_count < len(title_words):
                    clean_word_at_pos = clean_words[match_count] if match_count < len(clean_words) else ""
                    if clean_word_at_pos and not clean_word_at_pos.isdigit() and clean_word_at_pos != last_word:
                        continue  # mots incompatibles → skip
                score = match_count * 10
                if score > best_len:
                    best = serie
                    best_len = score

    return best


# ─── Resolver ────────────────────────────────────────────────────────────────

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

    def resolve_episode(self, serie_id: int, ep_number: int) -> dict | None:
        """Trouve episode_number == ep_number dans toutes les saisons de la série."""
        for season in self._seasons(serie_id):
            season_id = season["id"]
            for ep in self._episodes(season_id):
                if ep.get("episode_number") == ep_number:
                    return {
                        "season_id"     : season_id,
                        "season_number" : season.get("season_number"),
                        "episode_id"    : ep["id"],
                        "episode_number": ep_number,
                    }
        return None

    def resolve_season(self, serie_id: int, season_number: int) -> dict | None:
        """Trouve season_number == season_number dans les saisons de la série."""
        for season in self._seasons(serie_id):
            if season.get("season_number") == season_number:
                return {
                    "season_id"    : season["id"],
                    "season_number": season_number,
                }
        return None

    def resolve(self, torrent: dict, serie: dict) -> dict:
        serie_id = serie["id"]
        t_type   = torrent["type"]

        result = {
            **torrent,
            "serie_id"         : serie_id,
            "serie_title"      : serie.get("show_title"),
            "season_id"        : None,
            "season_number"    : None,
            "resolved_episodes": [],
            "resolved_seasons" : [],
            "resolve_status"   : "ok",
        }

        # ── pack_integrale : juste la série ───────────────────────────────────
        if t_type == "pack_integrale":
            pass

        # ── pack_saison : résoudre chaque numéro de saison → season_id ───────
        elif t_type == "pack_saison":
            saisons = torrent.get("saisons", [])
            for sn in saisons:
                s = self.resolve_season(serie_id, sn)
                if s:
                    result["resolved_seasons"].append(s)
                else:
                    print(f"         ⚠️  Saison {sn} introuvable (serie {serie_id})")

            found   = len(result["resolved_seasons"])
            total   = len(saisons)
            # Si moins de la moitié des saisons sont trouvées → pack_integrale
            # (cas MHA Saisons 1/2/3/4/5/6 = saisons anime ≠ saisons Fankai)
            if found == 0 or (total >= 3 and found < total / 2):
                print(f"         ℹ️  {found}/{total} saisons trouvées → reclassé pack_integrale")
                result["type"]             = "pack_integrale"
                result["resolved_seasons"] = []
                result["resolve_status"]   = "ok"
            else:
                if found < total:
                    result["resolve_status"] = "partial"
                result["season_id"]     = result["resolved_seasons"][0]["season_id"]
                result["season_number"] = result["resolved_seasons"][0]["season_number"]

        # ── episode / pack_episodes : résoudre chaque épisode global ─────────
        elif t_type in ("episode", "pack_episodes"):
            for ep_num in torrent.get("episodes", []):
                r = self.resolve_episode(serie_id, ep_num)
                if r:
                    result["resolved_episodes"].append(r)
                else:
                    print(f"         ⚠️  Épisode {ep_num} introuvable (serie {serie_id})")
                    result["resolve_status"] = "partial"
            if result["resolved_episodes"]:
                result["season_id"]     = result["resolved_episodes"][0]["season_id"]
                result["season_number"] = result["resolved_episodes"][0]["season_number"]

        return result


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        torrents = json.load(f)
    print(f"[Input] {len(torrents)} torrents à résoudre")

    cache    = Cache(CACHE_FILE)
    client   = FankaiClient(cache)
    resolver = Resolver(client)

    print("[Fankai] Chargement des séries...")
    series = client.get_all_series()
    print(f"[Fankai] {len(series)} séries disponibles")
    if not series:
        print("[!] Impossible de charger les séries, abandon.")
        return

    resolved_all = []
    no_match     = []

    for i, torrent in enumerate(torrents, 1):
        raw = torrent.get("raw", "")
        print(f"[{i:3d}/{len(torrents)}] {raw[:65]}")

        serie = find_serie(raw, series)
        if not serie:
            print(f"         ⚠️  Aucune série trouvée")
            no_match.append(raw)
            resolved_all.append({**torrent, "resolve_status": "no_serie_match"})
            continue

        print(f"         → {serie['show_title']} (id={serie['id']}) [{torrent['type']}]")
        result = resolver.resolve(torrent, serie)
        resolved_all.append(result)

    # Résumé
    from collections import Counter
    statuses = Counter(r.get("resolve_status", "?") for r in resolved_all)
    print("\n=== RÉSUMÉ ===")
    for s, c in sorted(statuses.items()):
        print(f"  {s:20s} : {c}")

    if no_match:
        print(f"\n=== SANS MATCH SÉRIE ({len(no_match)}) ===")
        for n in no_match:
            print(f"  {n}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(resolved_all, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Résultats sauvegardés dans {OUTPUT_FILE}")

    # Aperçu
    print("\n=== APERÇU ===")
    for r in resolved_all[:15]:
        ep_ids = [e["episode_id"] for e in r.get("resolved_episodes", [])]
        print(
            f"  [{r.get('resolve_status','?'):8s}] "
            f"serie={str(r.get('serie_id','?')):4s} "
            f"saison={str(r.get('season_id','?')):6s} "
            f"ep_ids={str(ep_ids):25s}  "
            f"{r['raw'][:50]}"
        )


if __name__ == "__main__":
    main()
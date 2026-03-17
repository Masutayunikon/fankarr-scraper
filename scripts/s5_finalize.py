"""
ÉTAPE 5 - Fankai Final Resolver
"""

import re
import json
import time
import requests
from pathlib import Path

FANKAI_BASE = "https://metadata.fankai.fr"
INPUT_FILE  = "data/torrent_enriched.json"
OUTPUT_FILE = "data/torrent_final.json"
CACHE_FILE  = "data/fankai_cache.json"
DELAY       = 0.2

Path("data").mkdir(exist_ok=True)

SERIE_FALLBACK = { 93: 92 }

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
    def get(self, key): return self.data.get(key)
    def set(self, key, value):
        self.data[key] = value
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
    def __contains__(self, key): return key in self.data

class FankaiClient:
    def __init__(self, cache: Cache):
        self.cache   = cache
        self.session = requests.Session()
    def _get(self, path: str):
        if path in self.cache: return self.cache.get(path)
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
                     exclude_season_zero: bool = False,
                     strict_season: bool = False) -> dict | None:
        seasons = self._seasons(serie_id)
        if exclude_season_zero:
            seasons = [s for s in seasons if s.get("season_number", 0) != 0]
        if season_number is not None and strict_season:
            ordered = [s for s in seasons if s.get("season_number") == season_number]
        elif season_number is not None:
            ordered = sorted(seasons, key=lambda s: 0 if s.get("season_number") == season_number else 1)
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
                   is_special: bool = False,
                   strict_season: bool = False) -> dict | None:
        exclude_s0 = not is_special
        result = self.find_episode(serie_id, ep_number, season_number,
                                   exclude_season_zero=exclude_s0, strict_season=strict_season)
        if result: return result
        if strict_season and season_number is not None:
            result = self.find_episode(serie_id, ep_number, season_number,
                                       exclude_season_zero=exclude_s0, strict_season=False)
            if result: return result
        fallback_id = SERIE_FALLBACK.get(serie_id)
        if fallback_id:
            result = self.find_episode(fallback_id, ep_number, season_number,
                                       exclude_season_zero=exclude_s0, strict_season=False)
            if result:
                print(f"    ↩️  ep {ep_number} trouvé via fallback serie {fallback_id}")
                return result
        return None

SERIE_PATTERNS: list[tuple[re.Pattern, int]] = [
    (re.compile(r"One\s+Piece\s+Yaba[iï]", re.IGNORECASE), 92),
    (re.compile(r"One\s+Piece\s+Ka[iï]",   re.IGNORECASE), 92),
]

def detect_serie_from_filename(filename: str, default_serie_id: int) -> int:
    for pattern, serie_id in SERIE_PATTERNS:
        if pattern.search(filename): return serie_id
    return default_serie_id

_SPECIAL_RE = re.compile(r"(?:Henshu|Henshū|Ka[iï]|Yaba[iï])\s+(\d+)[,.]5\b", re.IGNORECASE)
_EPISODE_RE = re.compile(r"(?:Henshu|Henshū|Ka[iï]|Yaba[iï])\s+(\d+)\b",      re.IGNORECASE)
_S00_RE     = re.compile(r"S00E(\d+)",   re.IGNORECASE)
_SxxE_RE    = re.compile(r"S(\d+)E(\d+)", re.IGNORECASE)

SPECIAL_EP_MAP: dict[tuple[int, int], int] = {
    (6, 7):  1,  # MHA 7,5  → Two Heroes
    (6, 19): 2,  # MHA 19,5 → Heroes Rising
    (6, 20): 3,  # MHA 20,5 → World Heroes Mission
    (6, 4):  4,  # MHA Film 4 → You're Next
}

SEASON_EP_OFFSET: dict[int, dict[int, int]] = {
    33: {2: 16},  # Hokuto No Ken Kaï : saison 02 → +16
}

def parse_file_episode(filename: str, serie_id: int = 0) -> tuple[int | None, bool, bool]:
    m = _S00_RE.search(filename)
    if m: return int(m.group(1)), True, True
    m = _SPECIAL_RE.search(filename)
    if m:
        base_num = int(m.group(1))
        s0_num = SPECIAL_EP_MAP.get((serie_id, base_num))
        if s0_num is not None: return s0_num, True, True
        return base_num, True, False
    m = _SxxE_RE.search(filename)
    if m and int(m.group(1)) > 0: return int(m.group(2)), False, False
    m = _EPISODE_RE.search(filename)
    if m: return int(m.group(1)), False, False
    m = re.search(r"[-–]\s*(\d{1,3})\s*[-–]", filename)
    if m: return int(m.group(1)), False, False
    return None, False, False

def resolve_season_episodes(torrent: dict, resolver: Resolver) -> dict:
    serie_id = torrent.get("serie_id")
    resolved_seasons = torrent.get("resolved_seasons") or []
    if resolved_seasons:
        seasons_to_resolve = resolved_seasons
    elif torrent.get("season_id"):
        seasons_to_resolve = [{"season_id": torrent["season_id"], "season_number": torrent.get("season_number")}]
    else:
        return torrent
    resolved = []
    for s in seasons_to_resolve:
        season_id     = s["season_id"]
        season_number = s["season_number"]
        for ep in resolver._episodes(season_id):
            resolved.append({
                "serie_id"      : serie_id,
                "season_id"     : season_id,
                "season_number" : season_number,
                "episode_id"    : ep["id"],
                "episode_number": ep.get("episode_number"),
                "filename"      : None,
                "is_special"    : False,
            })
    if resolved: torrent["resolved_episodes"] = resolved
    return torrent

def resolve_torrent_episodes(torrent: dict, resolver: Resolver) -> dict:
    serie_id = torrent.get("serie_id")
    if not serie_id: return torrent

    torrent_files = torrent.get("torrent_files", [])
    if not torrent_files:
        ep_numbers = torrent.get("file_ep_numbers", [])
        torrent_files = [{"num": n, "filename": "", "path": []} for n in ep_numbers]

    resolved_episodes = []
    unresolved        = []

    for f in torrent_files:
        filename    = f.get("filename", "")
        ep_num      = f.get("num")
        file_season = f.get("season_number")

        if file_season is None:
            for folder in f.get("path", [])[:-1]:
                m = re.search(r"(?:saison|partie|part)\s*(\d+)", folder, re.IGNORECASE)
                if m:
                    file_season = int(m.group(1))
                    break

        is_special    = False
        file_serie_id = serie_id
        use_s0_map    = False

        if filename:
            ep_num_parsed, is_special, use_s0_map = parse_file_episode(filename, serie_id)
            if ep_num_parsed is not None: ep_num = ep_num_parsed
            file_serie_id = detect_serie_from_filename(filename, serie_id)

        if file_season == 0:
            is_special = True
            use_s0_map = False
            if ep_num == 0: ep_num = 1

        if file_season and file_season > 0 and ep_num is not None:
            offset = SEASON_EP_OFFSET.get(serie_id, {}).get(file_season)
            if offset: ep_num = ep_num + offset

        if ep_num is None: continue

        if use_s0_map:
            season_hint, strict = 0, True
        elif is_special:
            season_hint, strict = 0, False
        elif file_season is not None and file_season > 0:
            season_hint, strict = file_season, True
        else:
            season_hint, strict = None, False

        result = resolver.resolve_ep(file_serie_id, ep_num, season_hint,
                                     is_special=is_special, strict_season=strict)
        if result:
            result["filename"]   = filename
            result["is_special"] = is_special
            resolved_episodes.append(result)
        else:
            unresolved.append({"episode_number": ep_num, "filename": filename})

    seen, deduped = set(), []
    for ep in resolved_episodes:
        eid = ep["episode_id"]
        if eid not in seen:
            seen.add(eid)
            deduped.append(ep)

    torrent["resolved_episodes"] = deduped
    torrent["unresolved_files"]  = unresolved
    torrent["resolve_status"]    = "partial" if unresolved else "ok"

    if unresolved:
        print(f"    ⚠️  {len(unresolved)} fichiers non résolus: {[u['episode_number'] for u in unresolved]}")

    if deduped:
        torrent["season_id"]     = deduped[0]["season_id"]
        torrent["season_number"] = deduped[0]["season_number"]

    return torrent

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
        # ── Cas torrent manuel : season_number=0 et episodes connus, pas de torrent_files
        if (torrent.get("season_number") == 0
                and torrent.get("episodes")
                and not torrent.get("resolved_episodes")
                and not torrent.get("torrent_files")):
            serie_id = torrent.get("serie_id")
            for ep_num in torrent["episodes"]:
                result = resolver.resolve_ep(serie_id, ep_num, season_number=0,
                                             is_special=True, strict_season=True)
                if result:
                    result["filename"]   = torrent.get("raw", "")
                    result["is_special"] = True
                    torrent.setdefault("resolved_episodes", []).append(result)
            if torrent.get("resolved_episodes"):
                torrent["resolve_status"] = "ok"
                print(f"[{i:3d}] {torrent.get('raw','')[:65]}")
                print(f"       → Manuel S00: {[e['episode_id'] for e in torrent['resolved_episodes']]}")
            continue

        if not torrent.get("torrent_files"):
            continue

        resolved_count = len(torrent.get("resolved_episodes") or [])
        files_count    = len(torrent.get("torrent_files") or [])
        if torrent.get("resolve_status") == "ok" and resolved_count > 0 and resolved_count >= files_count:
            continue

        raw = torrent.get("raw", "")
        print(f"[{i:3d}] {raw[:65]}")
        print(f"       serie={torrent.get('serie_id')} type={torrent.get('type')}")

        resolve_torrent_episodes(torrent, resolver)
        ep_ids = [e["episode_id"] for e in torrent.get("resolved_episodes", [])]
        print(f"       → {len(ep_ids)} épisodes résolus: {ep_ids[:10]}"
              f"{'...' if len(ep_ids) > 10 else ''}")

    print("\n--- Passe 2 : résolution des épisodes des pack_saison ---")
    for i, torrent in enumerate(torrents, 1):
        if torrent.get("type") != "pack_saison": continue
        if torrent.get("resolved_episodes"):      continue
        if not torrent.get("season_id"):          continue
        raw = torrent.get("raw", "")
        print(f"[{i:3d}] {raw[:65]}")
        resolve_season_episodes(torrent, resolver)
        ep_ids = [e["episode_id"] for e in torrent.get("resolved_episodes", [])]
        print(f"       → {len(ep_ids)} épisodes résolus: {ep_ids[:10]}"
              f"{'...' if len(ep_ids) > 10 else ''}")

    from collections import Counter
    statuses = Counter(t.get("resolve_status", "?") for t in torrents)
    print("\n=== RÉSUMÉ FINAL ===")
    for s, c in sorted(statuses.items(), key=lambda x: x[0] or ""):
        print(f"  {str(s or '?'):20s} : {c}")

    with_eps = sum(1 for t in torrents if t.get("resolved_episodes"))
    print(f"\n  Torrents avec episodes résolus : {with_eps}/{len(torrents)}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(torrents, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Résultat final → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
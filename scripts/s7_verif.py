#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ÉTAPE 6 - Fankai Verify
========================
Vérifie que tous les épisodes disponibles sur l'API Fankai
sont couverts par au moins un torrent dans torrent_final.json.

Affiche :
  - Les séries entièrement absentes du fichier
  - Les épisodes manquants par série
  - Un résumé global

Input  : torrent_final.json
Output : console uniquement
"""

import json
import time
import requests
from pathlib import Path
from collections import defaultdict

# ─── Config ───────────────────────────────────────────────────────────────────

FANKAI_BASE  = "https://metadata.fankai.fr"
INPUT_FILE   = "data/torrent_final.json"
CACHE_FILE   = "data/fankai_cache.json"
DELAY        = 0.2

# ─── Cache ────────────────────────────────────────────────────────────────────

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

# ─── Client API ───────────────────────────────────────────────────────────────

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

    def get_all_series(self) -> list[dict]:
        data = self._get("/series")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("series", [])
        return []

    def get_seasons(self, serie_id: int) -> list[dict]:
        data = self._get(f"/series/{serie_id}/seasons")
        return data.get("seasons", []) if isinstance(data, dict) else []

    def get_episodes(self, season_id: int) -> list[dict]:
        data = self._get(f"/seasons/{season_id}/episodes")
        return data.get("episodes", []) if isinstance(data, dict) else []

    def get_all_episodes(self, serie_id: int) -> list[dict]:
        """Retourne tous les épisodes (hors saison 0) d'une série, dédupliqués par episode_id."""
        episodes = []
        seen_ids = set()
        for season in self.get_seasons(serie_id):
            if season.get("season_number", 0) == 0:
                continue
            for ep in self.get_episodes(season["id"]):
                eid = ep["id"]
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                episodes.append({
                    "episode_id"    : eid,
                    "episode_number": ep.get("episode_number"),
                    "season_number" : season.get("season_number"),
                    "season_id"     : season["id"],
                })
        return episodes

# ─── Collecte des épisodes couverts par les torrents ─────────────────────────

def collect_covered_episodes(torrents: list[dict]) -> dict[int, set[int]]:
    """
    Retourne un dict { serie_id → set(episode_id) }
    avec tous les episode_id couverts par au moins un torrent.
    """
    covered: dict[int, set[int]] = defaultdict(set)
    for torrent in torrents:
        torrent_serie_id = torrent.get("serie_id")
        for ep in torrent.get("resolved_episodes", []):
            sid = ep.get("serie_id") or torrent_serie_id
            eid = ep.get("episode_id")
            if sid and eid:
                covered[sid].add(eid)
    return covered

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    # Charger torrent_final.json
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        print(f"[ERR] {INPUT_FILE} introuvable")
        return
    with open(input_path, encoding="utf-8") as f:
        torrents = json.load(f)
    print(f"[Input] {len(torrents)} torrents chargés\n")

    cache  = Cache(CACHE_FILE)
    client = FankaiClient(cache)

    # Récupérer toutes les séries API
    print("[API] Récupération de toutes les séries...")
    all_series = client.get_all_series()
    if not all_series:
        print("[ERR] Aucune série récupérée depuis l'API")
        return
    print(f"[API] {len(all_series)} séries trouvées\n")

    # Collecter les épisodes couverts dans torrent_final.json
    covered = collect_covered_episodes(torrents)
    series_in_file = set(covered.keys())

    # ── Rapport ──────────────────────────────────────────────────────────────
    total_missing_eps    = 0
    total_missing_series = 0
    series_complete      = 0

    print("=" * 65)
    print(f"{'SÉRIE':<35} {'API':>5} {'OK':>5} {'MANQUE':>6}")
    print("=" * 65)

    for serie in sorted(all_series, key=lambda s: s.get("title", "")):
        serie_id    = serie["id"]
        serie_title = serie.get("title", f"Série {serie_id}")

        # Récupérer tous les épisodes API pour cette série
        api_episodes = client.get_all_episodes(serie_id)
        if not api_episodes:
            continue  # Série sans épisodes sur l'API, on skip

        api_ep_ids = {ep["episode_id"] for ep in api_episodes}
        covered_ids = covered.get(serie_id, set())
        missing_ids = api_ep_ids - covered_ids

        api_count     = len(api_ep_ids)
        covered_count = len(covered_ids & api_ep_ids)
        missing_count = len(missing_ids)

        # Série entièrement absente
        if serie_id not in series_in_file:
            total_missing_series += 1
            total_missing_eps    += api_count
            print(f"  ✗ {serie_title:<33} {api_count:>5} {'0':>5} {api_count:>6}  ← SÉRIE ABSENTE")
            continue

        if missing_count == 0:
            series_complete += 1
            print(f"  ✓ {serie_title:<33} {api_count:>5} {covered_count:>5} {'0':>6}")
        else:
            total_missing_eps += missing_count
            # Construire la liste des numéros d'épisodes manquants
            missing_ep_labels = sorted(
                f"S{ep['season_number']:02d}E{ep['episode_number']:02d}"
                for ep in api_episodes
                if ep["episode_id"] in missing_ids and ep["episode_number"] is not None
            )
            print(f"  ⚠ {serie_title:<33} {api_count:>5} {covered_count:>5} {missing_count:>6}  → {', '.join(missing_ep_labels)}")

    # ── Résumé global ─────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"\n{'RÉSUMÉ':}")
    print(f"  Séries complètes     : {series_complete}")
    print(f"  Séries avec manques  : {len(all_series) - series_complete - total_missing_series}")
    print(f"  Séries absentes      : {total_missing_series}")
    print(f"  Épisodes manquants   : {total_missing_eps}")

if __name__ == "__main__":
    main()
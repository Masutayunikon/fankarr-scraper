"""
ÉTAPE 4 - Fankai Available Series
===================================
Génère available.json : liste des IDs de séries ayant au moins un torrent disponible.

Un torrent est considéré disponible si :
  - structure["torrents"] non vide (pack intégral)
  - OU au moins un season["torrents"] non vide (pack saison)
  - OU au moins un episode["torrents"] non vide (épisode individuel)
  - OU au moins un episode["paths"] non vide (path depuis pack intégral)

Output : available.json  (à la racine)
"""

import json
from pathlib import Path

SERIES_DIR     = Path("series")
OUTPUT_FILE    = Path("available.json")


def serie_has_torrent(data: dict) -> bool:
    # Pack intégral
    if data.get("torrents"):
        return True
    for season in data.get("seasons") or []:
        # Pack saison
        if season.get("torrents"):
            return True
        for ep in season.get("episodes") or []:
            # Torrent individuel, path depuis pack, ou épisode avec métadonnées
            if ep.get("torrents") or ep.get("paths") or ep.get("formatted_name") or ep.get("original_filename"):
                return True
    return False


def main():
    print("=== Génération de available.json ===\n")

    files = sorted(SERIES_DIR.glob("*.json"))
    if not files:
        print(f"[ERR] Aucun fichier dans {SERIES_DIR}/")
        return

    available = []
    for fpath in files:
        try:
            data = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  [!] {fpath.name}: {e}")
            continue

        if serie_has_torrent(data):
            available.append(data["id"])

    available.sort()
    OUTPUT_FILE.write_text(json.dumps(available, indent=2), encoding="utf-8")

    print(f"[OK] {len(available)}/{len(files)} séries disponibles → {OUTPUT_FILE}")
    print(f"     IDs: {available}")


if __name__ == "__main__":
    main()
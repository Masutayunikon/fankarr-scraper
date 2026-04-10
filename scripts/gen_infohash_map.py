"""
Génère un fichier infohash_map.json : { infohash: titre }
depuis data/torrent_raw.json + data/manual_torrents.json
"""

import json
from pathlib import Path

TORRENT_FILE = "data/torrent_raw.json"
MANUAL_FILE  = "data/manual_torrents.json"
OUTPUT_FILE  = "data/infohash_map.json"

def main():
    torrents = []

    p = Path(TORRENT_FILE)
    if p.exists():
        torrents += json.loads(p.read_text(encoding="utf-8"))

    m = Path(MANUAL_FILE)
    if m.exists():
        torrents += json.loads(m.read_text(encoding="utf-8"))

    result = {}
    for t in torrents:
        ih = t.get("infohash")
        title = t.get("title")
        if ih and title:
            result[ih] = title

    Path(OUTPUT_FILE).parent.mkdir(exist_ok=True)
    Path(OUTPUT_FILE).write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[OK] {len(result)} entrées → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
"""
Torrent File Parser
====================
Récupère l'arborescence des fichiers d'un torrent via son infohash,
et extrait les numéros d'épisodes depuis les noms de fichiers .mkv

pip install bencode.py requests
"""

import re
import time
import json
import sys
import requests
import bencodepy as bencode    # pip install bencode.py

from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

Path("data").mkdir(exist_ok=True)

# ─── Config ───────────────────────────────────────────────────────────────────
INPUT_FILE    = "data/torrent_resolved.json"
OUTPUT_FILE   = "data/torrent_enriched.json"
NYA_RAW_PATH  = "data/torrent_raw.json"

# Sources de téléchargement de .torrent par infohash (fallback dans l'ordre)
TORRENT_SOURCES = [
    "https://itorrents.org/torrent/{HASH}.torrent",
    "https://torrage.info/torrent.php?h={HASH}",
    "https://thetorrent.org/{HASH}.torrent",
]


# ─── Helpers bencode ──────────────────────────────────────────────────────────

def _bget(d: dict, key: str):
    """Récupère une valeur depuis un dict bencode — clé str ou bytes."""
    return d.get(key) or d.get(key.encode()) or d.get(key.encode("utf-8"))

def _bstr(v) -> str:
    """Convertit bytes → str si nécessaire."""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v) if v is not None else ""


# ─── Parsing bencode ──────────────────────────────────────────────────────────

def _parse_bencode_files(data: dict) -> list[dict]:
    """Extrait la liste de fichiers depuis un dict bencode décodé."""
    info = _bget(data, "info") or {}
    files = []

    file_list = _bget(info, "files")
    if file_list:
        for f in file_list:
            path_raw = _bget(f, "path") or []
            path = [_bstr(p) for p in path_raw]
            size = _bget(f, "length") or 0
            files.append({"path": path, "size": size})
    else:
        name = _bget(info, "name")
        if name:
            size = _bget(info, "length") or 0
            files.append({"path": [_bstr(name)], "size": size})

    return files


# ─── Récupération + parsing du .torrent ──────────────────────────────────────

def fetch_torrent_files(infohash: str) -> list[dict]:
    """
    Télécharge le .torrent depuis plusieurs sources et retourne la liste des fichiers.
    """
    h = infohash.upper()

    for source_tpl in TORRENT_SOURCES:
        url = source_tpl.replace("{HASH}", h)
        try:
            r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            if len(r.content) < 100:
                continue
            data = bencode.decode(r.content)
            print(f"  [✓] Source OK: {url}")
            return _parse_bencode_files(data)
        except Exception as e:
            print(f"  [!] Source KO ({url}): {e}")
            continue

    print(f"  [!] Toutes les sources ont échoué pour {infohash}")
    return []


def fetch_torrent_files_from_url(url: str) -> list[dict]:
    """Télécharge un .torrent depuis une URL directe et retourne les fichiers."""
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = bencode.decode(r.content)
        return _parse_bencode_files(data)
    except Exception as e:
        print(f"  [!] Erreur URL {url}: {e}")
        return []


# ─── Extraction des épisodes depuis les noms de fichiers ─────────────────────

_EP_PATTERNS = [
    re.compile(r"Henshu\s+(\d{1,3})\b",              re.IGNORECASE),
    re.compile(r"Henshū\s+(\d{1,3})\b",              re.IGNORECASE),
    re.compile(r"Ka[iï]\s+(\d{1,3})\b",              re.IGNORECASE),
    re.compile(r"Yaba[iï]\s+(\d{1,3})\b",            re.IGNORECASE),
    re.compile(r"\bS\d+E(\d+)\b",                    re.IGNORECASE),
    re.compile(r"\b(\d{1,3})\s*[-–]\s+\w",           re.IGNORECASE),
    re.compile(r"[-–\s](\d{2,3})[-–\s]",             re.IGNORECASE),
]

def extract_episode_number(filename: str) -> int | None:
    for pat in _EP_PATTERNS:
        m = pat.search(filename)
        if m:
            return int(m.group(1))
    return None

def parse_torrent_structure(files: list[dict]) -> dict:
    episodes = []
    folders  = set()
    extras   = []

    for f in files:
        path     = f["path"]
        filename = path[-1] if path else ""
        size     = f["size"]

        for folder in path[:-1]:
            if folder:
                folders.add(folder)

        if filename.lower().endswith((".mkv", ".mp4", ".avi")):
            ep_num = extract_episode_number(filename)
            if ep_num is not None:
                episodes.append({
                    "num"     : ep_num,
                    "filename": filename,
                    "path"    : path,
                    "size"    : size,
                })
        elif filename and not filename.startswith("."):
            extras.append(filename)

    episodes.sort(key=lambda e: e["num"])

    return {
        "episodes": episodes,
        "folders" : sorted(folders),
        "extras"  : extras,
    }


# ─── Enrichissement ──────────────────────────────────────────────────────────

def enrich_with_file_structure(resolved_path: str, nyaa_raw_path: str, output_path: str):
    with open(resolved_path, encoding="utf-8") as f:
        resolved = json.load(f)

    with open(nyaa_raw_path, encoding="utf-8") as f:
        raw_torrents = json.load(f)

    raw_by_title = {}
    for t in raw_torrents:
        title = t.get("name") or t.get("title") or t.get("Name") or ""
        if title:
            raw_by_title[title] = t

    enriched = 0
    for torrent in resolved:
        t_type = torrent.get("type")
        status = torrent.get("resolve_status")

        if t_type not in ("pack_integrale", "pack_saison", "episode", "pack_episodes"):
            continue
        if status not in ("ok", "partial", None):
            continue
        if status == "ok" and torrent.get("resolved_episodes"):
            continue
        if status == "ok" and t_type == "pack_saison" and torrent.get("season_id"):
            continue

        raw_name = torrent.get("raw", "")
        raw      = raw_by_title.get(raw_name)
        if not raw:
            continue

        torrent_url = raw.get("torrent_url") or raw.get("torrent")
        infohash    = raw.get("infohash") or raw.get("info_hash") or raw.get("hash")

        if not infohash:
            magnet = raw.get("magnet", "")
            m = re.search(r"btih:([a-fA-F0-9]{40})", magnet, re.I)
            if m:
                infohash = m.group(1).lower()

        if not torrent_url and not infohash:
            print(f"  [!] Pas de source pour: {raw_name[:60]}")
            continue

        print(f"  → Parsing torrent: {raw_name[:60]}")
        files     = fetch_torrent_files_from_url(torrent_url) if torrent_url else fetch_torrent_files(infohash)
        structure = parse_torrent_structure(files)

        torrent["torrent_files"]   = structure["episodes"]
        torrent["torrent_folders"] = structure["folders"]
        torrent["torrent_extras"]  = structure["extras"]
        torrent["file_ep_numbers"] = [e["num"] for e in structure["episodes"]]

        print(f"     {len(structure['episodes'])} épisodes trouvés: {torrent['file_ep_numbers']}")
        print(f"     Dossiers: {structure['folders']}")
        enriched += 1
        time.sleep(0.5)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(resolved, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {enriched} torrents enrichis → {output_path}")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Mode test : python s4_enrich.py <infohash>
        infohash  = sys.argv[1]
        files     = fetch_torrent_files(infohash)
        structure = parse_torrent_structure(files)

        print(f"\n=== DOSSIERS ===")
        for folder in structure["folders"]:
            print(f"  {folder}")

        print(f"\n=== ÉPISODES ({len(structure['episodes'])}) ===")
        for ep in structure["episodes"]:
            print(f"  ep={ep['num']:3d}  {ep['size']//1024//1024:6.0f} MB  {ep['filename'][:70]}")

        print(f"\n=== EXTRAS ===")
        for ex in structure["extras"]:
            print(f"  {ex}")
    else:
        enrich_with_file_structure(INPUT_FILE, NYA_RAW_PATH, OUTPUT_FILE)
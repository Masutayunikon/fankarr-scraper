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
    re.compile(r"\bS\d+E(\d+)\b",                    re.IGNORECASE),  # S01E10
    re.compile(r"\b\d{1,2}x(\d{2,3})\b",             re.IGNORECASE),  # 01x10
    re.compile(r"\b(\d{1,3})\s*[-–]\s+\w",           re.IGNORECASE),
    re.compile(r"[-–\s](\d{2,3})[-–\s]",             re.IGNORECASE),
]

# Patterns pour détecter les numéros flottants type 8.5, 2.5...
_EP_FLOAT_PATTERN = re.compile(r"\b(\d{1,3}\.\d)\b")

# Mots-clés bonus/extra — utilise \b pour éviter les faux positifs
# ex: "ona" dans "national", "nced" dans autre chose, etc.
_BONUS_KW_RE = re.compile(
    r"\b(?:bonus|extra|special|ova|ona|ncop|nced|opening|ending|film\s+bonus)\b",
    re.IGNORECASE
)

_SEASON_FOLDER_PATTERN = re.compile(r"saison\s*(\d+)", re.IGNORECASE)
_SEASON_FROM_FILE_PATTERN = re.compile(r"\b(\d{1,2})x\d{2,3}\b", re.IGNORECASE)  # 01x10 → saison 1

def extract_season_from_path(path: list[str]) -> int | None:
    """Déduit le numéro de saison depuis le dossier parent (ex: 'Saison 1', 'Saison 0')."""
    for folder in path[:-1]:
        m = _SEASON_FOLDER_PATTERN.search(folder)
        if m:
            return int(m.group(1))
    return None

def _is_bonus_filename(filename: str) -> bool:
    """Retourne True si le nom de fichier contient un mot-clé bonus (avec word boundaries)."""
    return bool(_BONUS_KW_RE.search(filename))

def extract_episode_number(filename: str) -> int | None:
    # D'abord vérifier si c'est un numéro flottant (ex: 8.5) → traiter comme extra
    if _EP_FLOAT_PATTERN.search(filename):
        return None
    for pat in _EP_PATTERNS:
        m = pat.search(filename)
        if m:
            return int(m.group(1))
    return None

def extract_season_from_filename(filename: str) -> int | None:
    """Extrait le numéro de saison depuis un nom de fichier style 01x10."""
    m = _SEASON_FROM_FILE_PATTERN.search(filename)
    return int(m.group(1)) if m else None

# Dossiers à exclure de l'organisation (bonus, musiques, images...)
_EXCLUDED_FOLDERS = {
    "endings", "ending", "openings", "opening", "ost", "artworks", "artwork",
    "bonus", "extras", "extra", "specials", "special", "ncop", "nced",
    "images", "image", "scans", "scan", "soundtrack", "music",
}

def _is_in_excluded_folder(path: list[str]) -> bool:
    """Retourne True si le fichier est dans un dossier à exclure."""
    for folder in path[:-1]:
        if folder.lower().strip() in _EXCLUDED_FOLDERS:
            return True
        # Préfixes connus : "ENDINGS", "ENDING 01", "OST", etc.
        folder_lower = folder.lower()
        if any(folder_lower.startswith(excl) for excl in _EXCLUDED_FOLDERS):
            return True
    return False

def parse_torrent_structure(files: list[dict]) -> dict:
    episodes  = []
    specials  = []
    folders   = set()
    extras    = []

    for f in files:
        path     = f["path"]
        filename = path[-1] if path else ""
        size     = f["size"]

        for folder in path[:-1]:
            if folder:
                folders.add(folder)

        if filename.lower().endswith((".mkv", ".mp4", ".avi")):
            # Fichiers dans des dossiers bonus/musique/images → extras
            if _is_in_excluded_folder(path):
                extras.append(filename)
                continue

            # Fichiers bonus/special dans le nom ou numéro flottant → saison 0
            if _is_bonus_filename(filename) or _EP_FLOAT_PATTERN.search(filename):
                season_from_path = extract_season_from_path(path)
                specials.append({
                    "filename"     : filename,
                    "path"         : path,
                    "size"         : size,
                    "season_number": season_from_path if season_from_path == 0 else 0,
                })
                continue

            ep_num = extract_episode_number(filename)
            if ep_num is not None:
                # Priorité : saison depuis le dossier > saison depuis le nom de fichier
                season_num = extract_season_from_path(path) or extract_season_from_filename(filename)

                # Fichier en saison 0 (dossier "Saison 0") → épisode 00 devient épisode 1
                if season_num == 0:
                    specials.append({
                        "num"          : max(ep_num, 1),
                        "filename"     : filename,
                        "path"         : path,
                        "size"         : size,
                        "season_number": 0,
                    })
                else:
                    episodes.append({
                        "num"          : ep_num,
                        "filename"     : filename,
                        "path"         : path,
                        "size"         : size,
                        "season_number": season_num,
                    })
            else:
                extras.append(filename)
        elif filename and not filename.startswith("."):
            extras.append(filename)

    episodes.sort(key=lambda e: e["num"])

    # Numéroter les specials séquentiellement
    for i, sp in enumerate(specials, start=1):
        sp["num"] = i

    all_episodes = episodes + specials

    return {
        "episodes": all_episodes,
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
        # Toujours enrichir torrent_files même si resolved_episodes existe déjà
        # (nécessaire pour que l'organizer sache où sont les fichiers)

        raw_name = torrent.get("raw", "")
        raw      = raw_by_title.get(raw_name)

        # Fallback : utiliser directement torrent_url et infohash du torrent résolu
        torrent_url = (raw.get("torrent_url") or raw.get("torrent")) if raw else None
        torrent_url = torrent_url or torrent.get("torrent_url")
        infohash    = (raw.get("infohash") or raw.get("info_hash") or raw.get("hash")) if raw else None
        infohash    = infohash or torrent.get("infohash")

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
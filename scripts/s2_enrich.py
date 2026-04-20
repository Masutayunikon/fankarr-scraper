"""
ÉTAPE 1.5 - Fankai Torrent File Enricher
==========================================
Télécharge chaque .torrent depuis nyaa_id et parse son contenu
pour extraire la liste des fichiers/dossiers internes.

Input  : data/torrent_raw.json
Output : data/torrent_raw.json  (enrichi avec champ "files")
"""

import json
import re
import time
import requests
from pathlib import Path

try:
    import torrentool.api as tapi
except ImportError:
    print("[ERR] Installe torrentool : pip install torrentool")
    raise

# ─── Config ───────────────────────────────────────────────────────────────────

TORRENT_FILE  = "data/torrent_raw.json"
CACHE_DIR     = Path("data/torrent_cache")
DELAY         = 0.5
MAX_ERRORS    = 10

CACHE_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "fankarr-enricher/1.0"})

VIDEO_EXT = {'.mkv', '.mp4', '.avi', '.m4v', '.mov'}

# ─── Download + parse ─────────────────────────────────────────────────────────

def download_torrent(nyaa_id: int) -> Path | None:
    cache_path = CACHE_DIR / f"{nyaa_id}.torrent"
    if cache_path.exists():
        return cache_path
    url = f"https://nyaa.si/download/{nyaa_id}.torrent"
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        cache_path.write_bytes(r.content)
        return cache_path
    except Exception as e:
        print(f"    [!] Download {nyaa_id} → {e}")
        return None


def parse_files(torrent_path: Path) -> list[str]:
    try:
        torrent = tapi.Torrent.from_file(str(torrent_path))
        return sorted(f.name for f in torrent.files)
    except Exception as e:
        print(f"    [!] Parse {torrent_path.name} → {e}")
        return []


# ─── Extraction des numéros ───────────────────────────────────────────────────

def extract_ep_from_filename(fname: str):
    """Extrait le numéro d'épisode relatif d'un nom de fichier vidéo."""
    if Path(fname).suffix.lower() not in VIDEO_EXT:
        return None
    stem = Path(fname).stem
    stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()
    if re.search(r'\b\d+[,.]\d+\b', stem): return None
    if re.search(r'\bbonus\b', stem, re.IGNORECASE): return None

    m = re.search(r'\bS\d{1,2}E(\d{2,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b\d{1,2}x(\d{2,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b(?:henshu|henshū|henshû|hensh|kaï|kai|yabai|yabaï|kyodai|film)\s+(\d{1,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b(\d{1,3})\s*\(fan-?ka[iï]\)', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\(fan-?ka[iï]\)\s+(\d{1,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'[-–]\s*0*(\d{1,3})\s*[-–]', stem)
    if m:
        num = int(m.group(1))
        if num <= 999: return num
    return None


def _is_video(f: str) -> bool:
    return (Path(f).suffix.lower() in VIDEO_EXT
            and not re.search(r'\b\d+[,.]\d+\b', Path(f).stem)
            and not re.search(r'\bbonus\b', Path(f).name, re.IGNORECASE))


def _uses_season_ep_notation(files: list[str]) -> bool:
    """Détecte si le torrent utilise la notation NNxNN (ex: 01x01, 02x03)."""
    for f in files:
        if Path(f).suffix.lower() in VIDEO_EXT:
            if re.search(r'\b\d{1,2}x\d{2,3}\b', Path(f).stem):
                return True
    return False


def extract_ep_numbers_from_files(files: list[str]) -> list[int]:
    """
    Extrait les numéros d'épisodes globaux depuis la liste de fichiers.

    Pour les torrents avec notation NNxNN (ex: 01x01, 02x01) :
    → tri alphabétique des fichiers vidéo et numérotation séquentielle 1, 2, 3...
    → évite les collisions entre saisons (02x01 ≠ 01x01)

    Pour les autres formats :
    → extraction directe du numéro depuis le nom de fichier.
    """
    video_files = sorted([f for f in files if _is_video(f)])

    if _uses_season_ep_notation(files):
        # Numérotation séquentielle basée sur l'ordre alphabétique des fichiers
        return list(range(1, len(video_files) + 1))

    numbers = set()
    for f in video_files:
        num = extract_ep_from_filename(Path(f).name)
        if num is not None:
            numbers.add(num)
    return sorted(numbers)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== Étape 1.5 : Fankai Torrent File Enricher ===\n")

    p = Path(TORRENT_FILE)
    if not p.exists():
        print(f"[ERR] {TORRENT_FILE} introuvable → lancez d'abord step1")
        return

    torrents = json.loads(p.read_text(encoding="utf-8"))
    print(f"[Input] {len(torrents)} torrents chargés")

    todo = [t for t in torrents if t.get("files") is None and t.get("nyaa_id")]
    print(f"[Todo]  {len(todo)} torrents à enrichir ({len(torrents)-len(todo)} déjà faits)\n")

    errors = 0
    done   = 0

    for i, torrent in enumerate(todo, 1):
        nyaa_id = torrent["nyaa_id"]
        title   = torrent.get("title", "?")[:60]
        print(f"  [{i:03d}/{len(todo)}] id={nyaa_id} | {title}")

        torrent_path = download_torrent(nyaa_id)
        if torrent_path is None:
            torrent["files"]      = []
            torrent["ep_numbers"] = []
            errors += 1
            if errors >= MAX_ERRORS:
                print(f"\n[STOP] {MAX_ERRORS} erreurs consécutives → arrêt")
                break
            continue

        errors = 0
        files   = parse_files(torrent_path)
        ep_nums = extract_ep_numbers_from_files(files)

        torrent["files"]      = files
        torrent["ep_numbers"] = ep_nums

        if files:
            print(f"         {len(files)} fichier(s) | ep_numbers={ep_nums[:10]}")
        else:
            print(f"         ⚠️  aucun fichier parsé")

        done += 1
        time.sleep(DELAY)

        if done % 20 == 0:
            p.write_text(json.dumps(torrents, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  [Save] {done} enrichis...")

    p.write_text(json.dumps(torrents, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[OK] {done} torrents enrichis → {TORRENT_FILE}")
    print(f"[Cache] {len(list(CACHE_DIR.glob('*.torrent')))} fichiers .torrent dans {CACHE_DIR}/")
    print("\n✅ Étape 1.5 terminée !")


if __name__ == "__main__":
    main()
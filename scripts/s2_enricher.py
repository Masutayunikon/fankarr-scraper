"""
ÉTAPE 1.5 - Fankai Torrent File Enricher
==========================================
Télécharge chaque .torrent depuis nyaa_id et parse son contenu
pour extraire la liste des fichiers/dossiers internes.

Cela permet au matcher (étape 2) de se baser sur les vrais noms
de fichiers plutôt que sur le titre Nyaa, beaucoup plus fiable.

Dépendance : pip install torrentool

Input  : data/torrent_raw.json
Output : data/torrent_raw.json  (enrichi avec champ "files")

Structure ajoutée par torrent :
  "files": [
    "Shingeki No Kyojin Henshū/SNK_01_1080p.mkv",
    "Shingeki No Kyojin Henshū/SNK_02_1080p.mkv",
    ...
  ]
"""

import json
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
CACHE_DIR     = Path("data/torrent_cache")   # stocke les .torrent téléchargés
DELAY         = 0.5
MAX_ERRORS    = 10   # stop si trop d'erreurs réseau consécutives

CACHE_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "fankarr-enricher/1.0"})

# ─── Download + parse ─────────────────────────────────────────────────────────

def download_torrent(nyaa_id: int) -> Path | None:
    """Télécharge le .torrent depuis nyaa.si et le met en cache."""
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
    """
    Retourne la liste des chemins de fichiers contenus dans le torrent.
    Pour un torrent single-file → ["nom_du_fichier.mkv"]
    Pour un torrent multi-file  → ["dossier/fichier1.mkv", "dossier/fichier2.mkv", ...]
    """
    try:
        torrent = tapi.Torrent.from_file(str(torrent_path))
        files = []
        for f in torrent.files:
            # f.name est le chemin relatif complet (ex: "Serie/ep01.mkv")
            files.append(f.name)
        return sorted(files)
    except Exception as e:
        print(f"    [!] Parse {torrent_path.name} → {e}")
        return []


# ─── Extraction des numéros depuis les noms de fichiers ───────────────────────

import re

VIDEO_EXT = {'.mkv', '.mp4', '.avi', '.m4v', '.mov'}

def extract_ep_from_filename(fname: str):
    """
    Extrait le numéro d'épisode d'un nom de fichier vidéo Fan-Kai.
    Ignore les fichiers non-vidéo. Retourne int ou None.
    """
    if Path(fname).suffix.lower() not in VIDEO_EXT:
        return None

    stem = Path(fname).stem
    # Enlever les tags [xxx] en tête
    stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()

    # Ignorer les fichiers bonus : numéros avec virgule (7,5), point (08.5), ou mot "Bonus"
    if re.search(r'\b\d+[,.]\d+\b', stem):
        return None
    if re.search(r'\bbonus\b', stem, re.IGNORECASE):
        return None

    # 1. Format S01E03
    m = re.search(r'\bS\d{1,2}E(\d{2,3})\b', stem, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # 2. Format 01x02 (ex: Hokuto No Ken Fan-Kai - 01x02 - ...)
    m = re.search(r'\b\d{1,2}x(\d{2,3})\b', stem, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # 3. Format Fan-Kai : "Henshū 01" / "Kaï 04" / "film 1"
    m = re.search(
        r'\b(?:henshu|henshū|henshû|hensh|kaï|kai|yabai|yabaï|kyodai|film)\s+(\d{1,3})\b',
        stem, re.IGNORECASE
    )
    if m:
        return int(m.group(1))

    # 4. Format "Inazuma Eleven 01 (Fan-kai)" → numéro suivi de (Fan-k...)
    m = re.search(r'\b(\d{1,3})\s*\(fan-?ka[iï]\)', stem, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # 4b. Format "(Fan-Kai) 01" → numéro après la parenthèse
    m = re.search(r'\(fan-?ka[iï]\)\s+(\d{1,3})\b', stem, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # 5. Format "- 001 -" ou "- 22 -" (One Piece, SNK, NSY...)
    m = re.search(r'[-–]\s*0*(\d{1,3})\s*[-–]', stem)
    if m:
        num = int(m.group(1))
        if num <= 999:
            return num

    return None


def extract_ep_numbers_from_files(files: list[str]) -> list[int]:
    """
    Extrait les numéros d'épisodes depuis la liste de fichiers du torrent.
    N'analyse que les fichiers vidéo. Retourne une liste triée de numéros uniques.
    """
    numbers = set()
    for f in files:
        fname = Path(f).name
        num = extract_ep_from_filename(fname)
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

    # Filtrer ceux qui n'ont pas encore de "files"
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

        errors = 0  # reset compteur si succès
        files  = parse_files(torrent_path)
        ep_nums = extract_ep_numbers_from_files(files)

        torrent["files"]      = files
        torrent["ep_numbers"] = ep_nums

        if files:
            print(f"         {len(files)} fichier(s) | ep_numbers={ep_nums[:10]}")
        else:
            print(f"         ⚠️  aucun fichier parsé")

        done += 1
        time.sleep(DELAY)

        # Sauvegarder tous les 20 torrents
        if done % 20 == 0:
            p.write_text(json.dumps(torrents, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  [Save] {done} enrichis...")

    # Sauvegarde finale
    p.write_text(json.dumps(torrents, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[OK] {done} torrents enrichis → {TORRENT_FILE}")
    print(f"[Cache] {len(list(CACHE_DIR.glob('*.torrent')))} fichiers .torrent dans {CACHE_DIR}/")
    print("\n✅ Étape 1.5 terminée !")


if __name__ == "__main__":
    main()
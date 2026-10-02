"""Accès à l'API metadata.fankai.fr : séries, saisons/épisodes, migration d'ID."""

import copy
import json
import re
import unicodedata
from pathlib import Path

import requests

METADATA_BASE = "https://metadata.fankai.fr"
HEADERS = {"User-Agent": "fankarr-scraper/1.0"}

CACHE_DIR = Path("scripts/.cache")
SEASONS_CACHE = CACHE_DIR / "series_seasons.json"
SERIES_INDEX = CACHE_DIR / "series_index.json"        # {id: {title, version}}
MATCHES_CACHE = CACHE_DIR / "matches.json"
FILE_MATCHES_CACHE = CACHE_DIR / "file_matches.json"

# Correspondances forcées ancien_id -> nouvel_id, quand le titre a aussi changé.
ID_ALIASES: dict[str, str] = {}


# --- utilitaires (déplacés depuis update_series.py) -------------------------

def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"  [!] Cache illisible ({path}): {e}")
        return {}


def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def http_get_json(url: str):
    r = requests.get(url, timeout=20, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def norm(text: str) -> str:
    """Minuscules, sans diacritiques ni ponctuation, années conservées,
    mots entre parenthèses non numériques supprimés ((Triggerforce) -> '')."""
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"\([^0-9)]+\)", "", text)
    text = re.sub(r"\((\d+)\)", r" \1 ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# --- séries -----------------------------------------------------------------

def fetch_series() -> list[dict]:
    data = http_get_json(f"{METADATA_BASE}/series")

    if not isinstance(data, list):
        raise ValueError("L'API des séries n'a pas retourné une liste")

    for series in data:
        series["torrents"] = []

    return data


def series_version(serie: dict) -> str:
    """Valeur qui change quand la série est modifiée côté API."""
    return str(serie.get("last_update") or "")


# --- saisons / épisodes (cache invalidé par last_updated) -------------------

def slim_episode(ep: dict) -> dict:
    keys = (
        "id", "episode_number", "title", "aired",
        "original_filename", "formatted_name", "nfo_filename",
    )
    return {k: ep.get(k) for k in keys}


def _fetch_seasons(series_id) -> list[dict]:
    data = http_get_json(f"{METADATA_BASE}/series/{series_id}/seasons")
    raw_seasons = data.get("seasons", []) if isinstance(data, dict) else data

    seasons = []

    for raw in sorted(raw_seasons, key=lambda s: s.get("season_number") or 0):
        link = (raw.get("links") or {}).get("episodes") or f"/seasons/{raw['id']}/episodes"

        ep_data = http_get_json(f"{METADATA_BASE}{link}")
        raw_eps = ep_data.get("episodes", []) if isinstance(ep_data, dict) else ep_data

        episodes = sorted(
            (slim_episode(e) for e in raw_eps),
            key=lambda e: e.get("episode_number") or 0,
        )

        seasons.append({
            "id": raw["id"],
            "season_number": raw.get("season_number"),
            "title": raw.get("title"),
            "episodes": episodes,
        })

    return seasons


def get_series_seasons(serie: dict, cache: dict, refresh: bool = False) -> list[dict]:
    """
    Cache : {id: {"version": last_updated, "seasons": [...]}}.
    Re-télécharge si la version a changé (ou ancien format de cache, ou refresh).
    Si l'API est en panne, on retombe sur le cache existant.
    """
    key = str(serie["id"])
    version = series_version(serie)
    entry = cache.get(key) if isinstance(cache.get(key), dict) else None

    if entry and not refresh and entry.get("version") == version:
        return copy.deepcopy(entry["seasons"])

    try:
        seasons = _fetch_seasons(serie["id"])
    except Exception as e:
        if entry:
            print(f"  [!] API injoignable pour {serie['title']}, cache conservé : {e}")
            return copy.deepcopy(entry["seasons"])
        raise

    cache[key] = {"version": version, "seasons": seasons}
    save_json(SEASONS_CACHE, cache)

    return copy.deepcopy(seasons)


# --- migration d'ID ---------------------------------------------------------

def migrate_series_ids(series: list[dict], output_dir: Path) -> dict[str, str]:
    """
    Détecte les séries dont l'ID a changé (ancien ID connu du run précédent,
    absent de l'API) et reporte leurs caches sur le nouvel ID, pour ne pas
    relancer l'IA. Liaison : alias manuel, sinon titre normalisé identique
    parmi les IDs nouveaux. Retourne {ancien_id: nouvel_id}.
    """
    current = {str(s["id"]): s for s in series}
    previous = load_json(SERIES_INDEX)
    mapping: dict[str, str] = {}

    if previous and current:
        gone = {i: info for i, info in previous.items() if i not in current}
        by_title: dict[str, list[str]] = {}

        for new_id in current:
            if new_id not in previous:
                by_title.setdefault(norm(current[new_id]["title"]), []).append(new_id)

        for old_id, info in gone.items():
            target = ID_ALIASES.get(old_id)

            if target is None:
                candidates = by_title.get(norm(info.get("title", "")), [])
                if len(candidates) == 1:
                    target = candidates[0]
                elif len(candidates) > 1:
                    print(f"  [!] ID {old_id} ({info.get('title')}) : plusieurs candidats {candidates}")

            if target in current and target not in mapping.values():
                mapping[old_id] = target
                print(f"  [id] {info.get('title')} : {old_id} → {target}")
            else:
                print(
                    f"  [!] ID {old_id} ({info.get('title')}) disparu sans équivalent "
                    f"(ajoute-le à ID_ALIASES si l'ID a changé)"
                )

    if mapping:
        matches = load_json(MATCHES_CACHE)
        for entry in matches.values():
            if str(entry.get("series_id")) in mapping:
                entry["series_id"] = mapping[str(entry["series_id"])]
        save_json(MATCHES_CACHE, matches)

        # clé : "<infohash>:<index>:<serie_id>"
        file_matches = {}
        for key, entry in load_json(FILE_MATCHES_CACHE).items():
            head, _, serie_id = key.rpartition(":")
            file_matches[f"{head}:{mapping[serie_id]}" if serie_id in mapping else key] = entry
        save_json(FILE_MATCHES_CACHE, file_matches)

        seasons_cache = load_json(SEASONS_CACHE)
        for old_id in mapping:
            seasons_cache.pop(old_id, None)
        save_json(SEASONS_CACHE, seasons_cache)

    save_json(SERIES_INDEX, {
        i: {"title": s["title"], "version": series_version(s)} for i, s in current.items()
    })

    return mapping
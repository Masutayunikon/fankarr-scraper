import argparse
import copy
import hashlib
import html as html_lib
import json
import os
import posixpath
import re
import unicodedata
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote, urlparse
from fankai_api import (
    METADATA_BASE, SEASONS_CACHE, MATCHES_CACHE, FILE_MATCHES_CACHE,
    fetch_series, get_series_seasons, series_version, migrate_series_ids,
    http_get_json, load_json, save_json, norm,
)

import requests
from typesafe_sdk import Choice, TypeSafeClient


BATCH_SIZE = 10

# Clé API TypeSafe : lue dans la variable d'environnement TYPESAFE_API_KEY
# (ne jamais l'écrire en dur dans un fichier versionné).
API_KEY = os.environ.get("TYPESAFE_API_KEY")

SERIES_URL = f"{METADATA_BASE}/series"
API_BASE = "https://nyaaapi.onrender.com"
NYAA_USER = "Fan-Kai"
HEADERS = {"User-Agent": "fankarr-scraper/1.0"}

# Torrents Nyaa qui ne viennent pas de l'utilisateur Fan-Kai mais font
# partie du projet (fankai = False). Ajouter d'autres IDs ici.
EXTRA_NYAA_IDS = ["2024827"]

# Un Choice accepte 255 options max : au-delà, on passe par la saison d'abord.
MAX_CHOICE_OPTIONS = 250

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".m4v", ".mov", ".webm",
    ".ts", ".m2ts", ".wmv", ".flv", ".mpg", ".mpeg",
}

LOCAL_TORRENTS_DIR = Path("scripts/torrents")
OUTPUT_DIR = Path("series")

# True : un torrent est placé au niveau le plus bas qui couvre tous ses
# fichiers (1 épisode -> episode.torrents, 1 saison -> season.torrents,
# sinon série). False : tous les torrents au niveau de la série.
PLACE_TORRENTS_BY_SCOPE = True
CACHE_DIR = Path("scripts/.cache")
NYAA_CACHE = CACHE_DIR / "nyaa_torrents.json"           # torrents Fan-Kai Nyaa
NYAA_EXTRA_CACHE = CACHE_DIR / "nyaa_extra.json"        # torrents Nyaa hors Fan-Kai
WIKI_CACHE = CACHE_DIR / "wiki.json"                    # {titre wiki: url}
TORRENT_FILES_DIR = CACHE_DIR / "torrents"              # .torrent téléchargés
STATE_FILE = CACHE_DIR / "state.json"

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


def nfc(text: str) -> str:
    """Normalisation Unicode (ū peut être 1 ou 2 caractères selon la source)."""
    return unicodedata.normalize("NFC", text)


# ---------------------------------------------------------------------------
# Identifiants
# ---------------------------------------------------------------------------

def get_torrent_id(torrent: dict) -> str:
    """
    ID unique d'un torrent.
    - torrent local / extra : champ 'id' déjà renseigné
    - torrent Nyaa          : https://nyaa.si/view/1392787 -> "1392787"
    """
    if torrent.get("id"):
        return str(torrent["id"])

    link = torrent.get("link", "")

    if not link:
        raise ValueError("Torrent sans champ 'id' ni 'link'")

    return urlparse(link).path.rstrip("/").split("/")[-1]



# ---------------------------------------------------------------------------
# Source Nyaa (utilisateur Fan-Kai, cache incrémental)
# ---------------------------------------------------------------------------

def fetch_user_page(user: str, page: int) -> list[dict]:
    url = f"{API_BASE}/nyaa/user/{user}"

    try:
        r = requests.get(
            url,
            params={"page": page, "sort": "id", "order": "desc"},
            timeout=20,
            headers=HEADERS,
        )

        # 404 / 422 = plus de pages
        if r.status_code in (404, 422):
            return []

        r.raise_for_status()

        data = r.json()

        if isinstance(data, list):
            return data

        return data.get("data", data.get("torrents", []))

    except Exception as e:
        print(f"  [!] Erreur page {page}: {e}")
        return []


def fetch_all_torrents(use_cache: bool = True) -> list[dict]:
    """
    Récupère les torrents de l'utilisateur Fan-Kai. Les pages sont triées
    par ID décroissant : dès qu'on retombe sur un torrent déjà en cache,
    on s'arrête.
    """
    known = load_json(NYAA_CACHE) if use_cache else {}

    if known:
        print(f"  {len(known)} torrents Nyaa déjà en cache.")

    page = 1
    new_count = 0

    while True:
        print(f"Récupération de la page {page}...")

        page_torrents = fetch_user_page(NYAA_USER, page)

        if not page_torrents:
            break

        reached_known = False

        for torrent in page_torrents:
            torrent_id = get_torrent_id(torrent)

            if torrent_id in known:
                reached_known = True
                continue

            torrent["id"] = torrent_id
            torrent["source"] = "nyaa"
            torrent["fankai"] = True
            known[torrent_id] = torrent
            new_count += 1

        print(f"  → {len(page_torrents)} torrents sur la page, {new_count} nouveaux")

        save_json(NYAA_CACHE, known)

        if reached_known:
            print("  → Torrent déjà connu atteint, arrêt de la pagination.")
            break

        page += 1

    for torrent in known.values():
        torrent.setdefault("fankai", True)

    return list(known.values())


def fetch_extra_torrents(ids: list[str], use_cache: bool = True) -> list[dict]:
    """Torrents Nyaa hors utilisateur Fan-Kai, récupérés par ID."""
    cache = load_json(NYAA_EXTRA_CACHE) if use_cache else {}

    for nyaa_id in map(str, ids):
        if nyaa_id in cache:
            continue

        try:
            r = requests.get(
                f"{API_BASE}/nyaa/id/{nyaa_id}",
                timeout=20,
                headers=HEADERS,
            )
            r.raise_for_status()
            data = r.json().get("data")

            if not data:
                raise ValueError("réponse sans champ 'data'")

        except Exception as e:
            print(f"  [!] Torrent Nyaa {nyaa_id} indisponible : {e}")
            continue

        data["id"] = nyaa_id
        data["source"] = "nyaa"
        data["fankai"] = False
        cache[nyaa_id] = data
        save_json(NYAA_EXTRA_CACHE, cache)

    return [cache[str(i)] for i in ids if str(i) in cache]


# ---------------------------------------------------------------------------
# Lecture des fichiers .torrent
# ---------------------------------------------------------------------------

def _bdecode(data: bytes, i: int = 0):
    """Mini décodeur bencode. Retourne (valeur, index_suivant)."""
    c = data[i:i + 1]

    if c == b"i":
        j = data.index(b"e", i)
        return int(data[i + 1:j]), j + 1

    if c == b"l":
        i += 1
        items = []
        while data[i:i + 1] != b"e":
            value, i = _bdecode(data, i)
            items.append(value)
        return items, i + 1

    if c == b"d":
        i += 1
        result = {}
        while data[i:i + 1] != b"e":
            key, i = _bdecode(data, i)
            value, i = _bdecode(data, i)
            result[key] = value
        return result, i + 1

    if c.isdigit():
        j = data.index(b":", i)
        length = int(data[i:j])
        start = j + 1
        return data[start:start + length], start + length

    raise ValueError(f"Bencode invalide à l'offset {i}")


def parse_torrent_bytes(data: bytes, fallback_name: str = "") -> dict:
    """
    Retourne {"infohash", "name", "files": [{"index", "path", "length"}]}.
    'index' = position dans la liste complète des fichiers du torrent.
    """
    if data[:1] != b"d":
        raise ValueError("Ce n'est pas un fichier torrent valide")

    i = 1
    info = None
    raw_info = None

    while data[i:i + 1] != b"e":
        key, i = _bdecode(data, i)
        start = i
        value, i = _bdecode(data, i)

        if key == b"info":
            info = value
            raw_info = data[start:i]

    if info is None or raw_info is None:
        raise ValueError("Section 'info' introuvable")

    def text(value: bytes) -> str:
        return value.decode("utf-8", errors="replace")

    name = text(
        info.get(b"name.utf-8") or info.get(b"name") or fallback_name.encode()
    )

    files = []

    if b"files" in info:
        for index, entry in enumerate(info[b"files"]):
            parts = entry.get(b"path.utf-8") or entry.get(b"path") or []
            path = "/".join([name] + [text(p) for p in parts])
            files.append({
                "index": index,
                "path": path,
                "length": entry.get(b"length", 0),
            })
    else:
        files.append({"index": 0, "path": name, "length": info.get(b"length", 0)})

    return {
        "infohash": hashlib.sha1(raw_info).hexdigest(),
        "name": name,
        "files": files,
    }


def is_fankai_local(name: str, stem: str) -> bool:
    """Les torrents locaux 'One Piece' / 'One.Piece' ne sont pas de Fan-Kai."""
    return not re.search(r"one[ ._]piece", f"{name} {stem}", re.IGNORECASE)


def parse_torrent_file(path: Path) -> dict:
    parsed = parse_torrent_bytes(path.read_bytes(), path.stem)

    return {
        "id": parsed["infohash"],
        "infohash": parsed["infohash"],
        "title": parsed["name"],
        "torrent_name": parsed["name"],
        "files": parsed["files"],
        "source": "local",
        "path": str(path),
        "fankai": is_fankai_local(parsed["name"], path.stem),
    }


def load_local_torrents(directory: Path = LOCAL_TORRENTS_DIR) -> list[dict]:
    if not directory.exists():
        print(f"  [!] Dossier introuvable : {directory}")
        return []

    torrents = []

    for path in sorted(directory.rglob("*.torrent")):
        try:
            torrents.append(parse_torrent_file(path))
        except Exception as e:
            print(f"  [!] {path.name} ignoré : {e}")

    return torrents


def ensure_torrent_files(torrent: dict) -> None:
    """
    Télécharge (une seule fois) le .torrent d'un torrent Nyaa et renseigne
    infohash, torrent_name et files. Sans effet pour un torrent local.
    """
    if "files" in torrent:
        return

    torrent_id = get_torrent_id(torrent)
    cache_path = TORRENT_FILES_DIR / f"{torrent_id}.torrent"

    if cache_path.exists():
        parsed = parse_torrent_bytes(cache_path.read_bytes(), torrent.get("title", ""))
    else:
        url = torrent.get("torrent") or f"https://nyaa.si/download/{torrent_id}.torrent"
        r = requests.get(url, timeout=30, headers=HEADERS)
        r.raise_for_status()

        parsed = parse_torrent_bytes(r.content, torrent.get("title", ""))

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(r.content)

    torrent["infohash"] = parsed["infohash"]
    torrent["torrent_name"] = parsed["name"]
    torrent["files"] = parsed["files"]


def is_video(path: str) -> bool:
    return posixpath.splitext(path)[1].lower() in VIDEO_EXTENSIONS


def get_video_files(torrent: dict) -> list[dict] | None:
    """Fichiers vidéo d'un torrent, ou None si le .torrent est inaccessible."""
    try:
        ensure_torrent_files(torrent)
    except Exception as e:
        print(f"  [!] .torrent indisponible pour {torrent.get('title')} : {e}")
        return None

    return [f for f in torrent["files"] if is_video(f["path"])]


# ---------------------------------------------------------------------------
# Matching torrent -> série
# ---------------------------------------------------------------------------

NONE_LABEL = "Aucune correspondance"

MATCH_RULES = (
    "Un torrent appartient à une série si son titre désigne réellement cette "
    "série (titre, titre original, année). Une ressemblance sur un seul mot "
    f"ne suffit pas. Sinon, choisir « {NONE_LABEL} »."
)


def build_series_criteria(series_list):
    """
    Construit les options du Choice.

    Le nom de l'option EST l'information (titre / titre original / année),
    sans description : ça évite de répéter des noms de champs et des valeurs
    vides pour chaque série, dans chaque question.

    Retourne (criteria, label_to_id).
    """
    criteria = {}
    label_to_id = {}

    for serie in series_list:
        serie_id = str(serie["id"])

        names = []
        for key in ("title", "show_title", "original_title"):
            value = (serie.get(key) or "").strip()
            if value and value not in names:
                names.append(value)

        label = " / ".join(names) or serie_id

        if serie.get("year"):
            label += f" ({serie['year']})"

        if label in label_to_id:
            label += f" [{serie_id}]"

        criteria[label] = None
        label_to_id[label] = serie_id

    criteria[NONE_LABEL] = "Aucune des séries proposées ne correspond au torrent."
    label_to_id[NONE_LABEL] = "none"

    return criteria, label_to_id


def match_torrents_batch(client, torrents, criteria, label_to_id):
    """
    Retourne :
        {"torrent_id": {"series_id": "...", "confidence": 0.98, "probabilities": {...}}}

    Le state contient les règles et les titres UNE fois ; chaque question
    y fait référence par chemin au lieu de recopier le titre et le prompt.
    """
    state = {
        "regles": MATCH_RULES,
        "torrents": [{"title": t.get("title", "")} for t in torrents],
    }

    questions = {}

    for i, _ in enumerate(torrents):
        questions[f"q{i}"] = Choice(
            instructions=(
                f"Selon `regles`, à quelle série appartient le torrent "
                f"`torrents[{i}].title` ?"
            ),
            criteria=criteria,
        )

    response = client.system_one(state=state, questions=questions)

    usage = getattr(response, "usage", None)
    if usage is not None:
        print(f"  tokens : {usage}")

    results = {}

    for i, torrent in enumerate(torrents):
        answer = response.answers[f"q{i}"]

        results[get_torrent_id(torrent)] = {
            "series_id": label_to_id.get(answer.choice, "none"),
            "confidence": answer.confidence,
            "probabilities": answer.probabilities,
        }

    return results


def match_all_torrents(torrents, series, batch_size=BATCH_SIZE, use_cache=True):
    """
    Match les torrents et les ajoute dans la clé 'torrents' de leur série.
    Les torrents déjà matchés (cache) ne sont pas renvoyés à l'API.
    """
    criteria, label_to_id = build_series_criteria(series)
    series_by_id = {str(s["id"]): s for s in series}

    for serie in series:
        serie["torrents"] = []

    cache = load_json(MATCHES_CACHE) if use_cache else {}

    forced = apply_forced_matches(torrents, series)
    torrents = [t for t in torrents if get_torrent_id(t) not in forced]

    pending = []

    for torrent in torrents:
        torrent_id = get_torrent_id(torrent)
        cached = cache.get(torrent_id)

        if cached and cached["series_id"] in series_by_id:
            serie = series_by_id[cached["series_id"]]
            serie["torrents"].append(torrent)

            print(
                f"  [cache] {torrent['title']}"
                f" → {serie['title']}"
                f" ({cached['confidence']:.2%})"
            )
        else:
            pending.append(torrent)

    print(
        f"\n{len(torrents) - len(pending)} torrents depuis le cache, "
        f"{len(pending)} à matcher."
    )

    if not pending:
        return series

    total = len(pending)

    with TypeSafeClient(api_key=API_KEY) as client:
        for start in range(0, total, batch_size):
            batch = pending[start:start + batch_size]

            print(
                f"\nBatch {start // batch_size + 1} : "
                f"{start + 1}-{min(start + batch_size, total)} / {total}"
            )

            results = match_torrents_batch(client, batch, criteria, label_to_id)

            for torrent in batch:
                torrent_id = get_torrent_id(torrent)
                result = results[torrent_id]
                series_id = result["series_id"]

                if series_id == "none":
                    print(
                        f"  [?] {torrent['title']}"
                        f" → aucune correspondance"
                        f" ({result['confidence']:.2%})"
                    )
                    continue

                serie = series_by_id.get(series_id)

                if serie is None:
                    print(f"  [!] Série inconnue {series_id} pour {torrent['title']}")
                    continue

                serie["torrents"].append(torrent)

                # Seuls les vrais matchs sont mis en cache :
                # les "none" seront retentés au prochain lancement.
                cache[torrent_id] = {
                    "series_id": series_id,
                    "confidence": result["confidence"],
                    "title": torrent.get("title", ""),
                    "source": torrent.get("source", "nyaa"),
                }

                print(
                    f"  [✓] {torrent['title']}"
                    f" → {serie['title']}"
                    f" ({result['confidence']:.2%})"
                )

            save_json(MATCHES_CACHE, cache)

    return series


# ---------------------------------------------------------------------------
# Saisons / épisodes d'une série
# ---------------------------------------------------------------------------

def slim_episode(ep: dict) -> dict:
    keys = (
        "id", "episode_number", "title", "aired",
        "original_filename", "formatted_name", "nfo_filename",
    )
    return {k: ep.get(k) for k in keys}


# ---------------------------------------------------------------------------
# Matching fichier vidéo -> épisode
# ---------------------------------------------------------------------------

LOW_CONFIDENCE = 0.6  # en dessous : affiché avec [~] pour relecture

SPECIALS_HINT = (
    "Les films, OAV et spéciaux (« Film 1 », « OAV »…) correspondent aux "
    "épisodes de la saison 0 (S00). Un numéro décimal comme 7,5 ou 19,5 "
    "indique seulement leur place dans l'ordre de diffusion de la série, "
    "ce n'est pas un numéro d'épisode."
)

FILE_RULES = (
    "Chaque fichier vidéo correspond à exactement un épisode de la liste. "
    "Se baser sur le numéro et le titre de l'épisode présents dans le nom "
    "du fichier. " + SPECIALS_HINT
)

SEASON_RULES = (
    "Chaque fichier vidéo appartient à exactement une saison de la liste. "
    "Se baser sur le numéro et le titre de l'épisode présents dans le nom "
    "du fichier, et sur les plages d'épisodes indiquées pour chaque saison. "
    + SPECIALS_HINT
)


def episode_label(season: dict, ep: dict) -> str:
    season_number = season.get("season_number") or 0
    episode_number = ep.get("episode_number") or 0
    return f"S{season_number:02d}E{episode_number:02d} - {ep.get('title') or ''}".strip()


def episode_options(seasons: list[dict]):
    """(criteria, label_to_episode_id) pour les épisodes des saisons données."""
    criteria = {}
    label_to_id = {}

    for season in seasons:
        for ep in season["episodes"]:
            label = episode_label(season, ep)

            if label in label_to_id:
                label += f" [{ep['id']}]"

            criteria[label] = None
            label_to_id[label] = ep["id"]

    return criteria, label_to_id


def season_options(seasons: list[dict]):
    """(criteria, label_to_season_id) avec la plage d'épisodes de chaque saison."""
    criteria = {}
    label_to_id = {}

    for season in seasons:
        numbers = [
            e["episode_number"] for e in season["episodes"]
            if e.get("episode_number") is not None
        ]
        span = f" (épisodes {min(numbers)} à {max(numbers)})" if numbers else ""
        label = f"Saison {season.get('season_number')} - {season.get('title') or ''}{span}"

        if label in label_to_id:
            label += f" [{season['id']}]"

        criteria[label] = None
        label_to_id[label] = season["id"]

    return criteria, label_to_id


def ask_batch(client, rules: str, files: list[dict], criteria_list: list[dict]):
    """
    Une requête Jev : une question par fichier (options propres à chacun).
    Une question à une seule option n'est pas posée : la réponse est connue.
    """
    answers = [None] * len(files)
    to_ask = []

    for i, criteria in enumerate(criteria_list):
        if len(criteria) == 1:
            answers[i] = SimpleNamespace(choice=next(iter(criteria)), confidence=1.0)
        else:
            to_ask.append(i)

    if not to_ask:
        return answers

    state = {
        "regles": rules,
        "fichiers": [{"path": files[i]["path"]} for i in to_ask],
    }

    questions = {
        f"q{k}": Choice(
            instructions=(
                f"Selon `regles`, quel est l'élément correspondant au fichier "
                f"`fichiers[{k}].path` ?"
            ),
            criteria=criteria_list[i],
        )
        for k, i in enumerate(to_ask)
    }

    response = client.system_one(state=state, questions=questions)

    usage = getattr(response, "usage", None)
    if usage is not None:
        print(f"  tokens : {usage}")

    for k, i in enumerate(to_ask):
        answers[i] = response.answers[f"q{k}"]

    return answers


def jev_match_files(client, seasons: list[dict], available: set, files: list[dict]):
    """
    Retourne une liste de (episode_id | None, confidence), une par fichier.
    Seuls les épisodes encore 'available' sont proposés, et il n'y a pas
    d'option « aucun » : Jev doit choisir le plus proche.
    """
    n = len(files)

    pool = []
    for season in seasons:
        episodes = [e for e in season["episodes"] if e["id"] in available]
        if episodes:
            pool.append({**season, "episodes": episodes})

    total = sum(len(s["episodes"]) for s in pool)

    if total == 0:
        return [(None, 0.0)] * n

    if total <= MAX_CHOICE_OPTIONS:
        criteria, label_to_id = episode_options(pool)
        answers = ask_batch(client, FILE_RULES, files, [criteria] * n)
        return [(label_to_id.get(a.choice), a.confidence) for a in answers]

    # Grosse série : 1) la saison, 2) l'épisode dans cette saison
    criteria, season_label_to_id = season_options(pool)
    answers = ask_batch(client, SEASON_RULES, files, [criteria] * n)

    per_season = {s["id"]: episode_options([s]) for s in pool}
    results = [(None, 0.0)] * n
    todo = []

    for i, answer in enumerate(answers):
        season_id = season_label_to_id.get(answer.choice)
        if season_id is not None:
            todo.append((i, season_id))

    if todo:
        answers = ask_batch(
            client,
            FILE_RULES,
            [files[i] for i, _ in todo],
            [per_season[season_id][0] for _, season_id in todo],
        )

        for (i, season_id), answer in zip(todo, answers):
            label_to_id = per_season[season_id][1]
            results[i] = (label_to_id.get(answer.choice), answer.confidence)

    return results


def attach_file(episode: dict, torrent: dict, file: dict) -> None:
    episode["paths"].append({
        "infohash": torrent["infohash"],
        "path": file["path"],
        "file_index": file["index"],
        "formatted_name": episode["formatted_name"],
        "nfo_filename": episode["nfo_filename"],
        "original_filename": episode["original_filename"],
    })


def resolve_episodes(series, batch_size=BATCH_SIZE, refresh=False, use_cache=True,
                     only_series=None):
    """
    Pour chaque série avec des torrents : associe chaque fichier vidéo à un
    épisode. Ordre : 1) nom identique à 'original_filename', 2) cache,
    3) Jev. Dans un torrent, un épisode n'est attribué qu'une fois : dès
    qu'il est pris, il disparaît des choix proposés pour les fichiers restants.
    Résultat : serie["seasons"][...]["episodes"][...]["paths"].
    """
    file_cache = load_json(FILE_MATCHES_CACHE) if use_cache else {}
    seasons_cache = load_json(SEASONS_CACHE)

    with TypeSafeClient(api_key=API_KEY) as client:
        for serie in series:
            if only_series is not None and str(serie["id"]) != str(only_series):
                continue

            if serie["torrents"]:
                print(f"\n=== {serie['title']} ===")

            try:
                seasons = get_series_seasons(serie, seasons_cache, refresh)
            except Exception as e:
                print(f"  [!] {serie['title']} : épisodes indisponibles : {e}")
                continue

            # (le reste est inchangé : by_id, by_filename, serie["seasons"] = seasons,
            #  puis `for torrent in serie["torrents"]:` …)

            by_id = {}
            by_filename = {}

            for season in seasons:
                season["torrents"] = []
                for ep in season["episodes"]:
                    ep["torrents"] = []
                    ep["paths"] = []
                    by_id[ep["id"]] = ep
                    if ep.get("original_filename"):
                        by_filename.setdefault(nfc(ep["original_filename"]), ep)

            serie["seasons"] = seasons

            for torrent in serie["torrents"]:
                files = get_video_files(torrent)

                if files is None:
                    continue

                pattern = torrent.get("path_filters", {}).get(str(serie["id"]))
                if pattern:
                    files = [f for f in files if re.search(pattern, f["path"], re.IGNORECASE)]

                used = set()  # épisodes déjà attribués dans CE torrent
                exact = cached = via_jev = ignored = 0
                rest = []

                # 1) Nom de fichier identique
                for file in files:
                    episode = by_filename.get(nfc(posixpath.basename(file["path"])))

                    if episode is None:
                        rest.append(file)
                    elif episode["id"] in used:
                        ignored += 1
                        print(f"  [?] {posixpath.basename(file['path'])} → doublon, ignoré")
                    else:
                        attach_file(episode, torrent, file)
                        used.add(episode["id"])
                        exact += 1

                # 2) Cache Jev
                pending = []

                for file in rest:
                    key = f"{torrent['infohash']}:{file['index']}:{serie['id']}"
                    entry = file_cache.get(key)

                    if (
                        entry
                        and entry["episode_id"] in by_id
                        and entry["episode_id"] not in used
                    ):
                        attach_file(by_id[entry["episode_id"]], torrent, file)
                        used.add(entry["episode_id"])
                        cached += 1
                    else:
                        pending.append(file)

                # 3) Jev, avec uniquement les épisodes encore libres
                while pending:
                    available = set(by_id) - used

                    if not available:
                        for file in pending:
                            ignored += 1
                            print(
                                f"  [?] {posixpath.basename(file['path'])}"
                                f" → plus aucun épisode disponible"
                            )
                        break

                    chunk, pending = pending[:batch_size], pending[batch_size:]
                    results = jev_match_files(client, seasons, available, chunk)

                    # Deux fichiers du lot ne peuvent pas prendre le même
                    # épisode : le plus sûr gagne, l'autre repart au lot suivant
                    # avec les choix restants.
                    chosen = {}
                    retry = []

                    for file, (episode_id, confidence) in zip(chunk, results):
                        if episode_id is None or episode_id not in available:
                            ignored += 1
                            print(
                                f"  [?] {posixpath.basename(file['path'])}"
                                f" → réponse inexploitable"
                            )
                        elif episode_id not in chosen:
                            chosen[episode_id] = (file, confidence)
                        elif confidence > chosen[episode_id][1]:
                            retry.append(chosen[episode_id][0])
                            chosen[episode_id] = (file, confidence)
                        else:
                            retry.append(file)

                    for episode_id, (file, confidence) in chosen.items():
                        episode = by_id[episode_id]
                        attach_file(episode, torrent, file)
                        used.add(episode_id)
                        via_jev += 1

                        key = f"{torrent['infohash']}:{file['index']}:{serie['id']}"
                        file_cache[key] = {
                            "episode_id": episode_id,
                            "confidence": confidence,
                            "path": file["path"],
                        }

                        mark = "~" if confidence < LOW_CONFIDENCE else "✓"
                        print(
                            f"  [{mark}] {posixpath.basename(file['path'])}"
                            f" → {episode['formatted_name']} ({confidence:.2%})"
                        )

                    pending = retry + pending
                    save_json(FILE_MATCHES_CACHE, file_cache)

                print(
                    f"  {torrent.get('title')} : {len(files)} vidéo(s) — "
                    f"{exact} nom exact, {cached} cache, {via_jev} Jev, "
                    f"{ignored} ignorée(s)"
                )

    return series


# ---------------------------------------------------------------------------
# Wiki (fan-kai.fandom.com)
# ---------------------------------------------------------------------------

WIKI_BASE = "https://fan-kai.fandom.com"
WIKI_API = (
    f"{WIKI_BASE}/fr/api.php"
    "?action=parse&format=json&page=Guide_des_%C3%A9pisodes&prop=text&utf8=1"
)
WIKI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# norm(titre_wiki) -> norm(titre_api)  ou  liste de norm(titre_api)  (1 wiki -> N séries)
MANUAL_OVERRIDES = {
    # Titre japonais dans l'API, titre français sur le wiki
    "l attaque des titans henshu": "shingeki no kyojin henshu",
    # Lastman : suffixe différent entre wiki (FAN-CUT) et API (Henshū)
    "lastman fan cut": "lastman henshu",
    # Hunter x Hunter : ordre inversé entre wiki "(ANNÉE) Kai" et API "Kaï (ANNÉE)"
    "hunter x hunter 1999 kai": "hunter x hunter kai 1999",
    "hunter x hunter 2011 kai": "hunter x hunter kai 2011",
    # Dragon Ball Z Kai -> renommé en Yabai
    "dragon ball z kai": "dragon ball z yabai",
    # One Piece Kai Ultime Pack = même wiki pour le Kai ET le Yabai
    "one piece kai ultime pack": ["one piece kai", "one piece yabai"],
}


def parse_wiki_series(page_html: str) -> dict[str, str]:
    """{titre_wiki: url_wiki} pour toutes les séries de la page."""
    matches = re.findall(
        r'<td align="center"><a href="(/fr/wiki/[^"]+)" title="([^"]+)">[^<]+</a>',
        page_html,
    )
    return {html_lib.unescape(title): WIKI_BASE + href for href, title in matches}


def get_wiki_map(refresh: bool = False) -> dict[str, str]:
    cached = load_json(WIKI_CACHE)

    if cached and not refresh:
        return cached

    try:
        r = requests.get(WIKI_API, timeout=30, headers=WIKI_HEADERS)
        r.raise_for_status()
        data = r.json()
        page_html = data.get("parse", {}).get("text", {}).get("*", "")

        if not page_html:
            raise RuntimeError(f"réponse API vide : {list(data.keys())}")

        wiki_map = parse_wiki_series(page_html)

        if not wiki_map:
            raise RuntimeError("aucune série trouvée dans la page")

    except Exception:
        if cached:
            print("  [!] Wiki inaccessible, utilisation du cache.")
            return cached
        raise

    save_json(WIKI_CACHE, wiki_map)
    return wiki_map


def enrich_with_wiki(series: list[dict], refresh: bool = False) -> None:
    """Renseigne serie["wiki"] (URL de la page wiki) quand le titre correspond."""
    try:
        wiki_map = get_wiki_map(refresh)
    except Exception as e:
        print(f"  [!] Wiki indisponible, champ 'wiki' non renseigné : {e}")
        return

    def targets(wiki_title: str) -> list[str]:
        mapped = MANUAL_OVERRIDES.get(norm(wiki_title), norm(wiki_title))
        return mapped if isinstance(mapped, list) else [mapped]

    norm_to_url = {}
    for title, url in wiki_map.items():
        for target in targets(title):
            norm_to_url[target] = url

    matched_keys = set()
    without_wiki = []

    for serie in series:
        key = norm(serie.get("title") or "")
        url = norm_to_url.get(key)

        if url:
            serie["wiki"] = url
            matched_keys.add(key)
        elif serie["torrents"]:
            without_wiki.append(serie["title"])

    print(f"  {len(wiki_map)} entrées wiki, {len(matched_keys)} séries associées.")

    if without_wiki:
        print(f"  [!] {len(without_wiki)} série(s) avec torrents sans page wiki :")
        for title in without_wiki:
            print(f"      {title}")

    unused = [
        title for title in wiki_map
        if not any(t in matched_keys for t in targets(title))
    ]

    if unused:
        print(f"  [!] {len(unused)} entrée(s) wiki sans série correspondante :")
        for title in unused:
            print(f"      {title}")


# ---------------------------------------------------------------------------
# Export series/<id>.json
# ---------------------------------------------------------------------------

def format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def infohash_from_magnet(magnet: str) -> str | None:
    found = re.search(r"btih:([0-9a-fA-F]{40})", magnet or "")
    return found.group(1).lower() if found else None


def export_torrent(torrent: dict) -> dict:
    torrent_id = get_torrent_id(torrent)
    is_nyaa = torrent.get("source") == "nyaa"

    magnet = torrent.get("magnet")
    infohash = torrent.get("infohash") or infohash_from_magnet(magnet)
    name = torrent.get("torrent_name") or torrent.get("title") or ""

    size = torrent.get("size")
    if not size and torrent.get("files"):
        size = format_size(sum(f["length"] for f in torrent["files"]))

    # Torrent local : magnet reconstruit à partir de l'infohash (sans trackers)
    if not magnet and infohash:
        magnet = f"magnet:?xt=urn:btih:{infohash}&dn={quote(name)}"

    torrent_url = torrent.get("torrent")
    if is_nyaa and not torrent_url:
        torrent_url = f"https://nyaa.si/download/{torrent_id}.torrent"

    pub_date = (torrent.get("time") or "").replace(" UTC", "").strip() or None

    return {
        "nyaa_id": int(torrent_id) if is_nyaa else None,
        "nyaa_url": torrent.get("link") if is_nyaa else None,
        "title": torrent.get("title"),
        "torrent_name": name,
        "torrent_url": torrent_url,
        "magnet": magnet,
        "infohash": infohash,
        "size": size,
        "pub_date": pub_date,
        "seeders": torrent.get("seeders"),
        "fankai": bool(torrent.get("fankai", True)),
    }


def build_series_export(serie: dict) -> dict:
    # Torrents dédoublonnés par infohash (le même torrent peut venir de Nyaa
    # et du dossier local) : on garde celui qui a un ID Nyaa.
    exports = {}
    for torrent in serie["torrents"]:
        exp = export_torrent(torrent)
        key = exp["infohash"] or f"id:{get_torrent_id(torrent)}"

        if key not in exports or (
            exports[key]["nyaa_id"] is None and exp["nyaa_id"] is not None
        ):
            exports[key] = exp

    # Chemins dédoublonnés et triés (sortie stable pour git)
    paths_by_episode = {}
    scope = {}  # infohash -> (ids saisons, ids épisodes)

    for season in serie["seasons"]:
        for ep in season["episodes"]:
            seen = set()
            paths = []

            for path in sorted(ep["paths"], key=lambda p: (p["infohash"], p["file_index"])):
                key = (path["infohash"], path["file_index"])
                if key in seen:
                    continue
                seen.add(key)
                paths.append(path)

                season_ids, episode_ids = scope.setdefault(path["infohash"], (set(), set()))
                season_ids.add(season["id"])
                episode_ids.add(ep["id"])

            paths_by_episode[ep["id"]] = paths

    series_torrents, season_torrents, episode_torrents = [], {}, {}

    ordered = sorted(
        exports.values(),
        key=lambda t: (t["nyaa_id"] is None, t["nyaa_id"] or 0, t["infohash"] or ""),
    )

    for exp in ordered:
        found = scope.get(exp["infohash"])

        if not PLACE_TORRENTS_BY_SCOPE or found is None:
            series_torrents.append(exp)
            continue

        season_ids, episode_ids = found

        if len(episode_ids) == 1:
            episode_torrents.setdefault(next(iter(episode_ids)), []).append(exp)
        elif len(season_ids) == 1:
            season_torrents.setdefault(next(iter(season_ids)), []).append(exp)
        else:
            series_torrents.append(exp)

    seasons = []

    for season in serie["seasons"]:
        episodes = []

        for ep in season["episodes"]:
            episodes.append({
                "id": ep["id"],
                "episode_number": ep["episode_number"],
                "title": ep["title"],
                "aired": ep["aired"],
                "original_filename": ep["original_filename"],
                "formatted_name": ep["formatted_name"],
                "nfo_filename": ep["nfo_filename"],
                "torrents": episode_torrents.get(ep["id"], []),
                "paths": paths_by_episode[ep["id"]],
            })

        seasons.append({
            "id": season["id"],
            "season_number": season["season_number"],
            "title": season["title"],
            "torrents": season_torrents.get(season["id"], []),
            "episodes": episodes,
        })

    export = {
        "id": serie["id"],
        "title": serie["title"],
        "show_title": serie.get("show_title"),
        "torrents": series_torrents,
        "seasons": seasons,
    }

    if serie.get("wiki"):
        export["wiki"] = serie["wiki"]

    return export


def write_series_files(series: list[dict], output_dir: Path = OUTPUT_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    written = unchanged = 0

    for serie in series:
        if not serie["torrents"]:
            continue

        if "seasons" not in serie:
            print(f"  [!] {serie['title']} ignorée : saisons/épisodes indisponibles")
            continue

        text = json.dumps(build_series_export(serie), ensure_ascii=False, indent=2) + "\n"
        path = output_dir / f"{serie['id']}.json"

        if path.exists() and path.read_text(encoding="utf-8") == text:
            unchanged += 1
            continue

        # newline="\n" : pas de CRLF sous Windows (diffs git propres)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)

        written += 1
        print(f"  → {path}")

    print(f"{written} fichier(s) écrit(s), {unchanged} inchangé(s).")


MULTI_SERIES_TORRENTS = [
    {
        "ids": ["2084491"],
        "series": {
            "one piece kai": None,
            "one piece yabai": None,
        },
    },
]

def apply_forced_matches(torrents: list[dict], series: list[dict]) -> set[str]:
    """Ajoute les torrents multi-séries à leurs séries. Retourne leurs IDs."""
    by_norm = {norm(s["title"]): s for s in series}
    forced = set()

    for rule in MULTI_SERIES_TORRENTS:
        ids = set(map(str, rule["ids"]))

        for torrent in torrents:
            torrent_id = get_torrent_id(torrent)

            if torrent_id not in ids:
                continue

            filters = torrent.setdefault("path_filters", {})
            torrent["multi_series"] = True

            for key, pattern in rule["series"].items():
                serie = by_norm.get(key)

                if serie is None:
                    print(f"  [!] Série '{key}' introuvable pour {torrent.get('title')}")
                    continue

                serie["torrents"].append(torrent)

                if pattern:
                    filters[str(serie["id"])] = pattern

                print(f"  [forcé] {torrent.get('title')} → {serie['title']}")

            forced.add(torrent_id)

    return forced

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def summary_line(serie: dict) -> str:
    line = f"{serie['title']} : {len(serie['torrents'])} torrent(s)"

    if "seasons" not in serie:
        return line

    episodes = [e for s in serie["seasons"] for e in s["episodes"]]
    # aired vide mais fichier présent dans un torrent = en réalité déjà sorti
    released = [e for e in episodes if e.get("aired") or e["paths"]]
    missing = [e["formatted_name"] for e in released if not e["paths"]]

    line += f", {len(released) - len(missing)}/{len(released)} épisodes sortis couverts"

    upcoming = len(episodes) - len(released)
    if upcoming:
        line += f" (+{upcoming} à venir)"

    if missing:
        line += " — manquants : " + ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")

    return line


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=["nyaa", "local", "all"],
        default="all",
        help="Origine des torrents (défaut : all)",
    )
    parser.add_argument(
        "--torrents-dir",
        type=Path,
        default=LOCAL_TORRENTS_DIR,
        help="Dossier des .torrent locaux",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore le cache des matchs (lecture uniquement, il sera réécrit)",
    )
    parser.add_argument(
        "--refresh-metadata",
        action="store_true",
        help="Re-télécharge les saisons/épisodes (metadata.fankai.fr) et le wiki",
    )
    parser.add_argument(
        "--series",
        help="ID d'une série : n'associe les fichiers aux épisodes que pour celle-ci",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Dossier des series/<id>.json (défaut : series)",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="N'écrit pas les fichiers series/<id>.json",
    )
    parser.add_argument(
        "--no-episodes",
        action="store_true",
        help="S'arrête après le matching torrent -> série",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore l'empreinte et relance tout le traitement",
    )
    return parser.parse_args()

def torrent_infohash(torrent: dict) -> str:
    return (
        torrent.get("infohash")
        or infohash_from_magnet(str(torrent.get("magnet")))
        or f"id:{get_torrent_id(torrent)}"
    )

def compute_fingerprint(torrents: list[dict], series: list[dict]) -> str:
    """SHA-256 : infohash + (id, last_updated) des séries + code."""
    hashes = sorted({torrent_infohash(t) for t in torrents})
    versions = sorted(f"{s['id']}={series_version(s)}" for s in series)
    here = Path(__file__)
    code = hashlib.sha256(
        here.read_bytes() + here.with_name("fankai_api.py").read_bytes()
    ).hexdigest()
    payload = "\n".join([code, "--", *versions, "--", *hashes])
    return hashlib.sha256(payload.encode()).hexdigest()

# ---------------------------------------------------------------------------
# infohash_map.json et available.json
# ---------------------------------------------------------------------------

INFOHASH_MAP_FILE = Path("infohash_map.json")
AVAILABLE_FILE = Path("available.json")


def write_if_changed(path: Path, text: str) -> bool:
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    return True


def write_infohash_map(torrents: list[dict], path: Path = INFOHASH_MAP_FILE) -> None:
    """{ infohash: titre } pour tous les torrents connus (clés triées = diffs stables)."""
    result = {}

    for torrent in torrents:
        infohash = torrent.get("infohash") or infohash_from_magnet(torrent.get("magnet"))
        title = torrent.get("title")

        if infohash and title:
            result[infohash.lower()] = title

    result = dict(sorted(result.items()))
    text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    changed = write_if_changed(path, text)

    print(f"  {len(result)} entrées → {path}" + ("" if changed else " (inchangé)"))


def serie_has_torrent(data: dict) -> bool:
    if data.get("torrents"):
        return True

    for season in data.get("seasons") or []:
        if season.get("torrents"):
            return True
        for ep in season.get("episodes") or []:
            if ep.get("torrents") or ep.get("paths"):
                return True

    return False


def write_available(output_dir: Path, path: Path = AVAILABLE_FILE) -> None:
    """IDs des séries ayant au moins un torrent, lus depuis series/*.json."""
    ids = []

    for file in sorted(output_dir.glob("*.json")):
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"  [!] {file.name} : {e}")
            continue

        if serie_has_torrent(data):
            ids.append(data["id"])

    try:
        ids.sort()
    except TypeError:  # ids de types mélangés
        ids.sort(key=str)

    changed = write_if_changed(path, json.dumps(ids, indent=2) + "\n")
    print(f"  {len(ids)} série(s) disponible(s) → {path}" + ("" if changed else " (inchangé)"))


def prune_series_files(series: list[dict], output_dir: Path) -> None:
    """Supprime les series/<id>.json dont l'ID n'existe plus dans l'API
    metadata (une série mise à jour change parfois d'ID)."""
    if not series:
        # Sécurité : une réponse vide de l'API ne doit pas tout effacer.
        print("  [!] Liste de séries vide, nettoyage ignoré.")
        return

    current = {f"{s['id']}.json" for s in series}

    for file in output_dir.glob("*.json"):
        if file.name not in current:
            file.unlink()
            print(f"  [-] {file.name} supprimé (ID absent de l'API)")


def main():
    args = parse_args()
    use_cache = not args.no_cache

    print("Chargement des séries Fan-Kai...")
    series = fetch_series()
    print(f"{len(series)} séries chargées.")

    # Reporte les caches si une série a changé d'ID (avant tout matching)
    migrate_series_ids(series, args.output_dir)

    torrents = []

    if args.source in ("nyaa", "all"):
        print("\nRécupération des torrents Nyaa...")
        nyaa = fetch_all_torrents(use_cache=use_cache)
        print(f"{len(nyaa)} torrents Nyaa.")
        torrents.extend(nyaa)

        extras = fetch_extra_torrents(EXTRA_NYAA_IDS, use_cache=use_cache)
        print(f"{len(extras)} torrent(s) Nyaa supplémentaire(s).")
        torrents.extend(extras)

    if args.source in ("local", "all"):
        print(f"\nLecture des torrents locaux ({args.torrents_dir})...")
        local = load_local_torrents(args.torrents_dir)
        print(f"{len(local)} torrents locaux.")
        torrents.extend(local)

    # Dédoublonnage par ID
    unique = {}
    for torrent in torrents:
        unique.setdefault(get_torrent_id(torrent), torrent)
    torrents = list(unique.values())

    full_run = (
        args.series is None
        and args.source == "all"
        and not args.no_episodes
        and not args.no_write
    )
    fingerprint = compute_fingerprint(torrents, series)
    state = load_json(STATE_FILE)

    if (
        full_run
        and use_cache
        and not args.force
        and state.get("fingerprint") == fingerprint
        and any(args.output_dir.glob("*.json"))
        and INFOHASH_MAP_FILE.exists()
        and AVAILABLE_FILE.exists()
    ):
        print(f"\nAucun changement ({len(torrents)} torrents), rien à faire.")
        return

    if full_run:
        print("\nÉcriture de infohash_map.json...")
        write_infohash_map(torrents)

    print("\nMatching des torrents...")
    series = match_all_torrents(
        torrents,
        series,
        batch_size=BATCH_SIZE,
        use_cache=use_cache,
    )

    if not args.no_episodes:
        print("\nAssociation des fichiers aux épisodes...")
        series = resolve_episodes(
            series,
            batch_size=BATCH_SIZE,
            refresh=args.refresh_metadata,
            use_cache=use_cache,
            only_series=args.series,
        )

    if not args.no_episodes and not args.no_write:
        print("\nRécupération des pages wiki...")
        enrich_with_wiki(series, refresh=args.refresh_metadata)

        print("\nÉcriture des fichiers...")
        to_write = series if args.series is None else [
            s for s in series if str(s["id"]) == str(args.series)
        ]
        write_series_files(to_write, args.output_dir)

        if args.series is None:
            prune_series_files(series, args.output_dir)

        print("\nÉcriture de available.json...")
        write_available(args.output_dir)

    if full_run:
        save_json(STATE_FILE, {"fingerprint": fingerprint, "torrents": len(torrents)})

    print("\nTerminé.\n")

    for serie in series:
        if args.series is not None and str(serie["id"]) != str(args.series):
            continue

        if not serie["torrents"]:
            continue

        print(summary_line(serie))


if __name__ == "__main__":
    main()
"""
ÉTAPE 7 - Wiki Enricher
========================
Récupère la page de guide des épisodes depuis fan-kai.fandom.com,
extrait les URLs wiki de chaque série, et les ajoute dans les fichiers
series/{id}.json sous la clé "wiki".

Input  : series/{id}.json
Output : series/{id}.json  (enrichi avec champ "wiki")
"""

import re
import json
import sys
import html as html_lib
import time
import unicodedata
import requests
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

WIKI_API   = ("https://fan-kai.fandom.com/fr/api.php"
              "?action=parse&format=json&page=Guide_des_%C3%A9pisodes&prop=text&utf8=1")
SERIES_DIR = Path(__file__).parent.parent / "series"
DELAY      = 0.3

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

# ── Correspondances manuelles ──────────────────────────────────────────────────
# norm(titre_wiki) → norm(titre_api)  ou  liste de norm(titre_api)  (1 wiki → N séries)
# À compléter si de nouveaux cas apparaissent.
MANUAL_OVERRIDES = {
    # Titre japonais dans l'API, titre français sur le wiki
    "l attaque des titans henshu":  "shingeki no kyojin henshu",
    # Lastman : suffixe différent entre wiki (FAN-CUT) et API (Henshū)
    "lastman fan cut":              "lastman henshu",
    # Hunter x Hunter : ordre inversé entre wiki "(ANNÉE) Kai" et API "Kaï (ANNÉE)"
    "hunter x hunter 1999 kai":    "hunter x hunter kai 1999",
    "hunter x hunter 2011 kai":    "hunter x hunter kai 2011",
    # Dragon Ball Z Kai → renommé en Yabai
    "dragon ball z kai":            "dragon ball z yabai",
    # One Piece Kai Ultime Pack = même wiki pour le Kai ET le Yabai
    "one piece kai ultime pack":    ["one piece kai", "one piece yabai"],
}


# ── Normalisation ──────────────────────────────────────────────────────────────

def norm(s: str) -> str:
    """Normalise un titre pour la comparaison : minuscules, sans diacritiques,
    sans ponctuation, avec les années conservées et les mots entre parenthèses
    non-numériques supprimés (ex: (Triggerforce) → '')."""
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"\([^0-9)]+\)", "", s)      # (Triggerforce) → ''
    s = re.sub(r"\((\d+)\)", r" \1 ", s)    # (1999) → ' 1999 '
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ── Parsing ────────────────────────────────────────────────────────────────────

def fetch_wiki_html() -> str:
    print(f"  [Fetch] API MediaWiki fan-kai.fandom.com")
    r = SESSION.get(WIKI_API, timeout=30)
    r.raise_for_status()
    data = r.json()
    html = data.get("parse", {}).get("text", {}).get("*", "")
    if not html:
        raise RuntimeError(f"Réponse API vide : {list(data.keys())}")
    time.sleep(DELAY)
    return html


def parse_wiki_series(html: str) -> dict[str, str]:
    """Retourne {titre_wiki → URL_wiki} pour toutes les séries de la page."""
    matches = re.findall(
        r'<td align="center"><a href="(/fr/wiki/[^"]+)" title="([^"]+)">[^<]+</a>',
        html,
    )
    return {
        html_lib.unescape(title): "https://fan-kai.fandom.com" + href
        for href, title in matches
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== Étape 7 : Wiki Enricher ===\n")

    html     = fetch_wiki_html()
    wiki_map = parse_wiki_series(html)
    print(f"[Wiki] {len(wiki_map)} séries extraites\n")

    # Construire norm(titre_api) → URL, en appliquant les overrides
    norm_to_url: dict[str, str] = {}
    for title, url in wiki_map.items():
        key    = norm(title)
        mapped = MANUAL_OVERRIDES.get(key, key)
        # Supporte une cible unique (str) ou multiple (list)
        targets = mapped if isinstance(mapped, list) else [mapped]
        for target in targets:
            norm_to_url[target] = url

    # Enrichir chaque fichier série
    series_files = sorted(SERIES_DIR.glob("*.json"))
    if not series_files:
        print(f"[ERR] Aucun fichier dans {SERIES_DIR}/ — lancez d'abord s4")
        return

    updated   = 0
    no_match  = []
    seen_urls = {}   # norm_titre → url, pour détecter les doublons matchés

    for f in series_files:
        data  = json.loads(f.read_text(encoding="utf-8"))
        title = data.get("title") or ""
        key   = norm(title)
        url   = norm_to_url.get(key)

        if url:
            data["wiki"] = url
            seen_urls[key] = url
            updated += 1
        else:
            no_match.append((f.name, title))

        f.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] {updated}/{len(series_files)} fichiers enrichis avec 'wiki'\n")

    if no_match:
        print(f"⚠️  {len(no_match)} série(s) sans correspondance wiki :")
        for fname, t in no_match:
            print(f"   {fname:12s} {t}")

    # Entrées wiki sans aucun fichier série correspondant
    matched_keys = set(seen_urls.keys())
    def _wiki_targets(t):
        mapped = MANUAL_OVERRIDES.get(norm(t), norm(t))
        return mapped if isinstance(mapped, list) else [mapped]

    unmatched_wiki = [
        (t, u) for t, u in wiki_map.items()
        if not any(target in matched_keys for target in _wiki_targets(t))
    ]
    if unmatched_wiki:
        print(f"\n⚠️  {len(unmatched_wiki)} entrée(s) wiki sans série locale :")
        for t, u in unmatched_wiki:
            print(f"   {t}")

    print("\n✅ Étape 7 terminée !")


if __name__ == "__main__":
    main()

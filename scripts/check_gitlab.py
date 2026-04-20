import requests
import json
from urllib.parse import quote

FANKAI_API  = "https://metadata.fankai.fr"
GITLAB_API  = "https://gitlab.com/api/v4/projects/ElPouki%2Ffankai_pack/repository/tree"

series = requests.get(f"{FANKAI_API}/series").json()
series_list = series if isinstance(series, list) else series.get("series", [])

print(f"{len(series_list)} séries trouvées\n")

found = []
missing = []

for s in series_list:
    title = s.get("title") or s.get("serie_title") or ""
    if not title:
        continue
    path = f"pack/{title}"
    url  = f"{GITLAB_API}?path={quote(path)}&ref=main"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and r.json():
            found.append(title)
            print(f"  ✓ {title}")
        else:
            missing.append(title)
            print(f"  ✗ {title} (status={r.status_code})")
    except Exception as e:
        missing.append(title)
        print(f"  ! {title} ({e})")

print(f"\n✓ {len(found)} trouvées, ✗ {len(missing)} manquantes")
if missing:
    print("\nManquantes :")
    for m in missing:
        print(f"  - {m}")
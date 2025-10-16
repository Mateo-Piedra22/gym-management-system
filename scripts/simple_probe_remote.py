import os
import requests
from typing import Dict


def read_properties(path: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                props[k.strip()] = v.strip()
    return props


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base_dir, "symmetricds", "engines", "railway.properties")
    props = read_properties(props_path)
    url = props.get("sync.url")
    print("Probing:", url)
    try:
        r = requests.get(url, timeout=10)
        print("Status:", r.status_code)
        print("Headers:", {k: v for k, v in r.headers.items() if k.lower() in ("server", "content-type")})
        print("Body (first 300 chars):", r.text[:300].replace("\n", " "))
    except Exception as e:
        print("Probe failed:", e)


if __name__ == "__main__":
    main()
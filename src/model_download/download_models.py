"""
Download models from presigned URLs to a mounted RunPod network volume.

Environment Variables:
  PRESIGNED_URLS  - Newline-separated list of presigned URLs (preferred)
  VAE_URL         - Presigned URL for the VAE model (fallback)
  DIT_URL         - Presigned URL for the DiT model (fallback)
  MODELS_DIR      - Destination directory (default: /runpod-volume/models)
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen


def _get_urls() -> list[str]:
    presigned_urls = os.environ.get("PRESIGNED_URLS", "").strip()
    if presigned_urls:
        return [line.strip() for line in presigned_urls.splitlines() if line.strip()]

    urls = []
    for key in ("VAE_URL", "DIT_URL"):
        value = os.environ.get(key)
        if value:
            urls.append(value.strip())

    return [u for u in urls if u]


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    filename = Path(parsed.path).name
    if not filename:
        raise ValueError(f"Could not infer filename from URL: {url}")
    return filename


def _download(url: str, dest_dir: Path) -> Path:
    filename = _filename_from_url(url)
    dest_path = dest_dir / filename
    print(f"Downloading {url} -> {dest_path}", flush=True)

    with urlopen(url, timeout=60) as response, open(dest_path, "wb") as out_file:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out_file.write(chunk)

    size_bytes = dest_path.stat().st_size
    print(f"Downloaded {dest_path} ({size_bytes} bytes)", flush=True)
    return dest_path


def main() -> int:
    models_dir = Path(os.environ.get("MODELS_DIR", "/runpod-volume/models"))
    models_dir.mkdir(parents=True, exist_ok=True)

    urls = _get_urls()
    if not urls:
        print(
            "No presigned URLs provided. Set PRESIGNED_URLS or VAE_URL/DIT_URL.",
            file=sys.stderr,
        )
        return 1

    failures = 0
    for url in urls:
        try:
            _download(url, models_dir)
        except Exception as exc:
            failures += 1
            print(f"Failed to download {url}: {exc}", file=sys.stderr, flush=True)

    if failures:
        print(f"{failures} download(s) failed.", file=sys.stderr)
        return 1

    print("All downloads completed successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

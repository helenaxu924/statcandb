import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path, PosixPath
from typing import Generator, Optional, Union
from urllib.parse import urlparse

import requests

from .pbar_utils import tqdm_if_verbose


def download_file(
    url: str,
    download_path: Optional[Union[str, Path]] = None,
    download_dir: Optional[Union[str, Path]] = None,
    session: Optional[requests.Session] = None,
    verbose: bool = False,
) -> Path:
    """
    Download the contents of a URL to a file.

    If download_path is provided, will write the contents to that path.
    If download_dir is provided, will write the contents to
    download_dir / basename(url)

    Args:
        url: The URL to download
        download_path: The path to download the file to. Must provide this
            or download_dir
        download_dir: The directory to download the file to. Must provide
            this or download_path
        session: Optionally, a requests.Session object to use when downloading
        verbose: If true, print a progressbar

    Returns:
        The path to the downloaded file
    """
    if download_path:
        download_path = Path(download_path)
        download_dir = download_path.parent

    elif download_dir:
        filename = PosixPath(urlparse(url).path).name
        download_dir = Path(download_dir)
        download_path = download_dir / filename

    else:
        raise ValueError("You must provide one of download_path or download_dir")

    session = session or requests.session()

    if not download_dir.exists():
        raise ValueError(f"download_dir must exist: {download_dir}")

    with session.get(url, stream=True) as response:
        response.raise_for_status()
        with open(download_path, "wb") as outfile:
            total_length = int(response.headers.get("content-length", 0))
            with tqdm_if_verbose(
                desc="Downloading",
                total=total_length,
                unit="iB",
                unit_scale=True,
                unit_divisor=1_024,
                leave=False,
                verbose=verbose,
            ) as pbar:
                for chunk in response.iter_content(1_024):
                    outfile.write(chunk)
                    pbar.update(len(chunk))

    return download_path


@contextmanager
def unzip_file(filename: Path) -> Generator[Path, None, None]:
    if filename.suffix == ".zip":
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(filename) as ofile:
                ofile.extractall(tmpdir)
                name = "".join(x for x in filename.name if x.isdigit()) + ".csv"
                yield Path(tmpdir) / name
    else:
        yield filename

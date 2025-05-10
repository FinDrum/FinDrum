import io
import json
import logging
import re
import zipfile
from typing import Generator

import pandas as pd
import requests
import orjson

from crawler import Crawler

class SecCrawler(Crawler):
    """
    Download a ZIP archive published by the SEC, extract every JSON file
    inside it, and yield a pandas DataFrame for each record with columns:

        - ``cik``         (str, 10-digit CIK zero-padded on the left)
        - ``entity_name`` (str | None)
        - ``facts``       (dict | None)

    Example
    -------
    >>> crawler = SecCrawler(url, "you@example.com")
    >>> for df in crawler.fetch():
    ...     print(df.head())
    """

    _URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    _CIK_PATTERN = re.compile(r"CIK(\d+)\.json$", re.IGNORECASE)
    _DEFAULT_CIK = "0000000000"

    def __init__(self, email: str, url: str = None) -> None:
        """
        Parameters
        ----------
        email : str
            Contact e-mail required by the SEC `User-Agent` policy.
        url : str, optional
            Public SEC URL pointing to a ZIP archive.
        """
        if not email:
            raise ValueError("email cannot be empty.")

        self._url = url or self._URL
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": f"FinDrum User ({email})"})

    def fetch(self) -> Generator[pd.DataFrame, None, None]:
        """
        Stream one :class:`pandas.DataFrame` per valid JSON found in the ZIP.

        The generator keeps memory usage low by loading and emitting each
        record individually instead of materialising the entire archive at
        once.

        Yields
        ------
        pandas.DataFrame
            A single-row DataFrame with *cik*, *entity_name* and *facts*.
        """
        response = self._session.get(self._url, timeout=30)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            for member in archive.infolist():
                if not member.filename.lower().endswith(".json"):
                    continue

                try:
                    with archive.open(member) as file:
                        raw_record = orjson.loads(file.read())

                    yield pd.DataFrame(
                        [{
                            "cik": SecCrawler._extract_cik(member.filename),
                            "entity_name": raw_record.get("entityName"),
                            "facts":        raw_record.get("facts"),
                        }]
                    )

                except (json.JSONDecodeError, KeyError) as exc:
                    logging.warning(
                        "Could not process %s → %s",
                        member.filename,
                        exc,
                    )
                except Exception:
                    logging.exception("Unexpected error while processing %s", member.filename)

    @staticmethod
    def _extract_cik(filename: str) -> str:
        match = SecCrawler._CIK_PATTERN.search(filename)
        return match.group(1).zfill(10) if match else SecCrawler._DEFAULT_CIK
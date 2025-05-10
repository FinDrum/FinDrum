import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from processor import Processor

class SecProcessor(Processor):
    _DEFAULT_FRAME_PREFIX: str = "CY"

    def __init__(self, *, frame_prefix: Optional[str] = None) -> None:
        self._frame_prefix = frame_prefix or self._DEFAULT_FRAME_PREFIX

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Flatten SEC 'facts' data into a tidy DataFrame.

        This method processes a DataFrame containing nested 'facts' dictionaries
        from the SEC API, extracting relevant information into a flat structure
        suitable for analysis.

        Parameters
        ----------
        data : pandas.DataFrame
            Input DataFrame with at least the columns:
                - 'cik' : str
                    Central Index Key, a unique identifier for entities.
                - 'facts' : dict
                    Nested dictionary containing financial facts.

        Returns
        -------
        pandas.DataFrame
            A flattened DataFrame with columns:
                - 'cik' : str
                - 'entity_name' : str or None
                - 'frame' : str
                - 'tag' : str
                - 'unit' : str
                - 'start' : str or None
                - 'end' : str or None
                - 'val' : float or None
                - 'accn' : str or None
                - 'fy' : int or None
                - 'fp' : str or None
                - 'form' : str or None
                - 'filed' : str or None

        Raises
        ------
        ValueError
            If the 'facts' column is missing from the input DataFrame.

        Examples
        --------
        >>> processor = SecProcessor()
        >>> df = pd.DataFrame({
        ...     'cik': ['0000000000'],
        ...     'facts': [{'us-gaap': {'Revenues': {'units': {'USD': [{'val': 1000, 'frame': 'CY2020'}]}}}}]
        ... })
        >>> flat_df = processor.process(df)
        >>> print(flat_df.head())
        ```
        """
        if "facts" not in data.columns:
            raise ValueError("Input DataFrame must include a 'facts' column.")

        logging.info("Processing SEC data (%s rows)", len(data))

        rows = data.apply(lambda row: self._extract_facts(row), axis=1)
        flat = pd.DataFrame([item for sublist in rows if sublist for item in sublist])

        if flat.empty:
            logging.info("No facts matched frame_prefix='%s'", self._frame_prefix)

        return flat

    def _extract_facts(self, row):
        """
        Extract and flatten 'facts' from a single DataFrame row.

        Parameters
        ----------
        row : pandas.Series
            A row from the input DataFrame containing 'cik', 'entitiyName' and 'facts'.

        Returns
        -------
        list of dict
            A list of dictionaries, each representing a flattened fact.
        """
        cik = row.get("cik")
        entity_name = row.get("entityName")
        facts = row.get("facts")

        if not facts:
            logging.warning("'facts' is empty for cik=%s. Skipping row.", cik)
            return []

        return self._collect_rows(cik, entity_name, facts)

    def _collect_rows(self, cik: str, entity_name: Optional[str], facts: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Recursively extract and flatten fact records from the SEC 'facts' structure.

        Parameters
        ----------
        cik : str
            Central Index Key of the entity.
        entity_name : str or None
            Name of the entity.
        facts : dict
            Nested dictionary containing financial facts.

        Returns
        -------
        list of dict
            A list of dictionaries, each representing a flattened fact.
        """
        output: List[Dict[str, Any]] = []
        for taxonomy in facts.values():
            for tag, tag_data in taxonomy.items():
                for unit, records in tag_data.get("units", {}).items():
                    for rec in records:
                        frame: str = rec.get("frame", "")
                        if not frame.startswith(self._frame_prefix):
                            continue

                        output.append(
                            {
                                "cik": cik,
                                "entity_name": entity_name,
                                "frame": frame,
                                "tag": tag,
                                "unit": unit,
                                "start": rec.get("start"),
                                "end": rec.get("end"),
                                "val": rec.get("val"),
                                "accn": rec.get("accn"),
                                "fy": rec.get("fy"),
                                "fp": rec.get("fp"),
                                "form": rec.get("form"),
                                "filed": rec.get("filed"),
                            }
                        )
        return output
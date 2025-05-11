from abc import ABC, abstractmethod
import pandas as pd

class Reader(ABC):
    @abstractmethod
    def read(self, path: str) -> pd.DataFrame:
        pass # pragma: no cover
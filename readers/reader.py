from abc import ABC, abstractmethod
import pandas as pd

class Reader(ABC):
    # Patron Builder
    @abstractmethod
    def read(self, path: str) -> pd.DataFrame:
        pass
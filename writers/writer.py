from abc import ABC, abstractmethod
import pandas as pd

class Writer(ABC):
    # Patron Builder
    @abstractmethod
    def write(self, filename: str, data: pd.DataFrame) -> None:
        pass
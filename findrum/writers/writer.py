from abc import ABC, abstractmethod
import pandas as pd

class Writer(ABC):
    @abstractmethod
    def write(self, filename: str, data: pd.DataFrame) -> None:
        pass # pragma: no cover
from abc import ABC, abstractmethod
import pandas as pd

class Storage(ABC):
    @abstractmethod
    def store(self, data: pd.DataFrame) -> None:
        pass
from abc import ABC, abstractmethod
from typing import Any, Sequence


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise IndexError("Storage is empty, cannot remove data")
        data = self.storage.pop(0)
        return (data)


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | Sequence[int | float]) -> None:
        valid = self.validate(data)
        if valid:
            if isinstance(data, (int, float)):
                new_data = str(data)
                self.storage.append((self.rank, new_data))
                self.rank += 1
            else:
                for item in data:
                    new_item = str(item)
                    self.storage.append((self.rank, new_item))
                    self.rank += 1
        else:
            raise TypeError("Improper numeric data")


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            for st in data:
                if not isinstance(st, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        valid = self.validate(data)

        if valid:
            if isinstance(data, str):
                self.storage.append((self.rank, str(data)))
                self.rank += 1
            else:
                for item in data:
                    self.storage.append((self.rank, str(item)))
                    self.rank += 1
        else:
            raise TypeError("Improper text data")


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    for key, value in item.items():
                        if (not isinstance(key, str) or
                                not isinstance(value, str)):
                            return False
                else:
                    return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        valid = self.validate(data)

        if valid:
            if isinstance(data, dict):
                for key, value in data.items():
                    self.storage.append((self.rank, f"{key}:{value}"))
                    self.rank += 1
            else:
                for item in data:
                    values = list(item.values())
                    t_values = f"{values[0]}: {values[1]}"
                    self.storage.append((self.rank, f"{t_values}"))
                    self.rank += 1
        else:
            raise TypeError("Improper log data")


def data_processor() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")

    numeric = NumericProcessor()

    print(f" Trying to validate input '42': {numeric.validate(42)}")
    print(f" Trying to validate input 'Hello': {numeric.validate('hello')}")
    print(" Test invalid ingestion of string 'foo' without prior \
validation:")

    try:
        numeric.ingest("foo")
    except TypeError as t:
        print(f" Got exception: {t}")

    num_data = [1, 2, 3, 4, 5]
    numeric.ingest(num_data)
    print(f" Processing data: {num_data}")

    print(" Extracting 3 values...")
    for i in range(3):
        num_output = numeric.output()
        print(f" Numeric value {num_output[0]}: {num_output[1]}")

    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f" Trying to validate input '42': {text.validate(42)}")
    text_data = ['Hello', 'Nexus', 'World']
    text.ingest(text_data)
    print(f" Processing data: {text_data}")
    print(" Extracting 1 value...")
    text_output = text.output()
    print(f" Text value {text_output[0]}: {text_output[1]}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
    log_data = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    log.ingest(log_data)
    print(f" Processing data: {log_data}")
    print(" Extraction 2 values...")
    for i in range(2):
        log_output = log.output()
        print(f" Log entry {log_output[0]}: {log_output[1]}")


if __name__ == "__main__":
    data_processor()

from typing import Protocol, Any, Iterable


class ClickHouseClientProtocol(Protocol):
    def insert(
        self, table: str, rows: Iterable[tuple], column_names: list[str]
    ) -> Any: ...

    def query(self, query: str, parameters: dict | None = None) -> Any: ...

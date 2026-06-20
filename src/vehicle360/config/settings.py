from dataclasses import dataclass
import json
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    catalog: str = "vehicle360"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    control_schema: str = "control"

    @property
    def bronze_prefix(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}"

    @property
    def silver_prefix(self) -> str:
        return f"{self.catalog}.{self.silver_schema}"

    @property
    def gold_prefix(self) -> str:
        return f"{self.catalog}.{self.gold_schema}"

    @property
    def control_prefix(self) -> str:
        return f"{self.catalog}.{self.control_schema}"


def load_settings(config_path: str | None = None) -> Settings:
    if not config_path:
        return Settings()
    path = Path(config_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    return Settings(
        catalog=data.get("catalog", "vehicle360"),
        bronze_schema=data.get("bronze_schema", "bronze"),
        silver_schema=data.get("silver_schema", "silver"),
        gold_schema=data.get("gold_schema", "gold"),
        control_schema=data.get("control_schema", "control"),
    )

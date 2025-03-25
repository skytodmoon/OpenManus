from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from ..base import BaseTool

class DataLoader(BaseTool):
    """智能数据加载器｜支持文件/数据库/API"""

    name = "etl_loader"
    parameters = {
        "type": "object",
        "properties": {
            "source_type": {
                "type": "string",
                "enum": ["file", "database", "api"],
                "default": "file"
            },
            "path": {"type": "string"},
            "db_config": {
                "type": "object",
                "properties": {
                    "conn_str": {"type": "string"},
                    "table_name": {"type": "string"},
                    "query": {"type": "string"}
                }
            }
        },
        "required": ["source_type"]
    }

    async def execute(self, config: dict) -> pd.DataFrame:
        if config["source_type"] == "file":
            return self._load_file(config["path"])
        elif config["source_type"] == "database":
            return self._load_database(config["db_config"])
        raise ValueError("暂不支持该数据源类型")

    def _load_file(self, path: str) -> pd.DataFrame:
        file_path = Path(path)
        suffix = file_path.suffix.lower()

        loaders = {
            ".csv": lambda: pd.read_csv(file_path),
            ".xlsx": lambda: pd.read_excel(file_path),
            ".parquet": lambda: pd.read_parquet(file_path),
            ".json": lambda: pd.read_json(file_path)
        }

        if suffix in loaders:
            return loaders[suffix]()
        raise ValueError(f"不支持的文件格式: {suffix}")

    def _load_database(self, config: dict) -> pd.DataFrame:
        engine = create_engine(config["conn_str"])
        if "query" in config:
            return pd.read_sql(config["query"], engine)
        return pd.read_sql_table(config["table_name"], engine)
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from ..base import BaseTool

class DataSaver(BaseTool):
    """智能数据存储工具｜支持多格式输出"""

    name = "etl_saver"
    parameters = {
        "type": "object",
        "properties": {
            "output_format": {
                "type": "string",
                "enum": ["csv", "parquet", "database"],
                "default": "csv"
            },
            "db_config": {
                "type": "object",
                "properties": {
                    "conn_str": {"type": "string"},
                    "table_name": {"type": "string"}
                }
            }
        }
    }

    async def execute(self, df: pd.DataFrame, config: dict) -> str:
        output_format = config.get("output_format", "csv")

        if output_format == "csv":
            return self._save_csv(df)
        elif output_format == "parquet":
            return self._save_parquet(df)
        elif output_format == "database":
            return self._save_database(df, config["db_config"])
        raise ValueError("不支持的输出格式")

    def _save_csv(self, df: pd.DataFrame) -> str:
        output_dir = Path("workspace/cleaned_data")
        output_dir.mkdir(exist_ok=True)
        path = output_dir / f"cleaned_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(path, index=False)
        return str(path)

    def _save_parquet(self, df: pd.DataFrame) -> str:
        output_dir = Path("workspace/cleaned_data")
        output_dir.mkdir(exist_ok=True)
        path = output_dir / f"cleaned_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet"
        df.to_parquet(path)
        return str(path)

    def _save_database(self, df: pd.DataFrame, config: dict) -> str:
        engine = create_engine(config["conn_str"])
        df.to_sql(config["table_name"], engine, if_exists="append", index=False)
        return f"{config['table_name']} in database"
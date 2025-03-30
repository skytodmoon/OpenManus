import os
from datetime import datetime
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from ..base import BaseTool

class DataSaver(BaseTool):
    """智能数据存储器｜多格式输出支持"""

    name: str = "etl_saver"
    description: str = "将处理后的数据保存至本地文件或数据库"
    parameters: dict = {
        "type": "object",
        "properties": {
            "output_format": {
                "type": "string",
                "enum": ["csv", "parquet", "database"],
                "description": "输出格式选择（csv/parquet/数据库）",
                "default": "csv"
            },
            "compression": {
                "type": "string",
                "enum": ["none", "gzip", "snappy"],
                "description": "文件压缩格式（仅parquet有效）",
                "default": "none"
            }
        },
        "required": []
    }

    async def execute(self, df: pd.DataFrame, config: dict = None) -> str:
        """保存处理后的数据"""
        config = config or {}
        # 确保使用output_format参数
        output_format = config.get("output_format", "csv")
        output_dir = config.get("output_dir", "workspace/cleaned_data")

        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        if output_format == "csv":
            output_path = f"{output_dir}/cleaned_{timestamp}.csv"
            df.to_csv(output_path, index=False)
        elif output_format == "excel":
            output_path = f"{output_dir}/cleaned_{timestamp}.xlsx"
            df.to_excel(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        return output_path

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

    # async def _save_database(self, df: pd.DataFrame, config: dict) -> str:
    #     """保存数据到数据库"""
    #     required_keys = ["db_url", "table_name"]
    #     if not all(key in config for key in required_keys):
    #         raise ValueError(f"数据库配置缺少必要参数，需要: {required_keys}")
    #
    #     if not hasattr(self, 'engine'):
    #         self.engine = create_async_engine(config["db_url"])
    #
    #     async with self.engine.begin() as conn:
    #         await conn.run_sync(
    #             lambda sync_conn: df.to_sql(
    #                 name=config["table_name"],
    #                 con=sync_conn,
    #                 if_exists=config.get("if_exists", "append"),
    #                 index=False
    #             )
    #         )
    #     return f"数据已保存至表 {config['table_name']}"
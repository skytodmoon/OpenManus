import logging
import uuid

import pandas as pd

from ..base import BaseTool
from pathlib import Path
import json
from datetime import datetime
from typing import Dict

# 新增导入
from ...logger import logger

class MetadataRecorder(BaseTool):
    """元数据记录器"""

    name: str = "etl_metadata"
    description: str = "记录ETL流程元数据信息"
    parameters: dict = {
        "type": "object",
        "properties": {
            "pipeline_id": {"type": "string"},
            "data_source": {"type": "string"},
            "steps": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["pipeline_id", "data_source"]
    }
    def _validate_metadata(self, meta: dict) -> Dict:
        """根据parameters定义进行验证"""
        # 从类属性获取必填字段
        required_fields = self.parameters.get("required", [])
        for field in required_fields:
            if field not in meta:
                raise ValueError(f"缺少必要字段: {field}")
        return meta

    def _trace_data_lineage(self, input_path: str) -> dict:
        """追踪数据血缘关系"""
        if not input_path:
            return {}

        return {
            "input_path": input_path,
            "source_type": "file",  # 可以根据实际情况扩展
            "extracted_at": pd.Timestamp.now().isoformat()
        }
    # 增加数据血缘追踪
    async def execute(self, meta: dict) -> bool:
        """增强版元数据记录"""
        full_meta = {
            **meta,
            "lineage": self._trace_data_lineage(meta.get("input_path")),
            "data_quality": self._assess_data_quality(meta.get("df"))
        }
        return await self._save_metadata(full_meta)

    async def _save_metadata(self, meta: dict) -> bool:
        """实现元数据存储逻辑"""
        try:
            # 确保目录存在
            meta_dir = Path("workspace/metadata")
            meta_dir.mkdir(parents=True, exist_ok=True)

            pipeline_id = meta.get("pipeline_id", str(uuid.uuid4()))
            meta_file = meta_dir / f"{pipeline_id}.json"

            with open(meta_file, "w") as f:
                json.dump(meta, f)
            return True
        except Exception as e:
            logging.error(f"Failed to save metadata: {str(e)}")
            return False

    def _assess_data_quality(self, df: pd.DataFrame) -> dict:
        """评估数据质量"""
        if df is None or not isinstance(df, pd.DataFrame):
            return {
                "error": "Invalid DataFrame",
                "quality_score": 0
            }

        quality_metrics = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "null_percentage": df.isnull().mean().mean(),
            "duplicate_rows": df.duplicated().sum(),
            "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()}
        }

        # 计算质量评分 (0-100)
        quality_score = max(0, 100 - (quality_metrics["null_percentage"] * 100))
        quality_metrics["quality_score"] = round(quality_score, 2)

        return quality_metrics
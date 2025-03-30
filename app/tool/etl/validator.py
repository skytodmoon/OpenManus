from ..base import BaseTool
import pandas as pd

class DataValidator(BaseTool):
    """数据质量验证器｜自动规则检测"""

    name: str = "etl_validator"
    description: str = "执行数据质量校验，确保数据符合业务规则"
    parameters: dict = {
        "type": "object",
        "properties": {
            "schema": {
                "type": "object",
                "description": "数据schema定义（包含字段类型和约束）"
            },
            "rules": {
                "type": "array",
                "items": {"type": "string"},
                "description": "自定义验证规则列表"
            }
        },
        "required": ["schema"]
    }

    def _validate_schema(self, df: pd.DataFrame, schema: dict) -> dict:
        """验证数据结构"""
        report = {"errors": [], "warnings": []}

        # 基本schema校验（示例实现）
        if schema:
            # 检查列是否存在
            for col in schema.get("columns", {}):
                if col not in df.columns:
                    report["errors"].append(f"缺失必需列: {col}")

            # 检查数据类型
            for col, dtype in schema.get("columns", {}).items():
                if col in df.columns and not df[col].dtype == dtype:
                    report["errors"].append(f"列'{col}'类型不匹配: 期望{dtype}, 实际{df[col].dtype}")

        return report  # 确保返回包含errors的字典

    async def execute(self, df: pd.DataFrame, config: dict = None) -> dict:
        config = config or {}
        schema_check = self._validate_schema(df, config.get("schema", {})) or {"errors": []}
        value_check = self._validate_values(df, config.get("rules", [])) or {"errors": []}

        return {
            "schema_errors": len(schema_check["errors"]),
            "value_errors": len(value_check["errors"]),
            "error_count": len(schema_check["errors"]) + len(value_check["errors"])
        }

    def _validate_values(self, df: pd.DataFrame, rules: list = None) -> dict:
        """验证数据值是否符合业务规则"""
        report = {"errors": [], "warnings": []}

        # 基本值验证
        for col in df.columns:
            # 检查空值 - 使用.values.sum()避免.item()问题
            null_count = df[col].isnull().values.sum()
            if null_count > 0:
                report["errors"].append(f"列'{col}'包含{null_count}个空值")

            # 检查数值列的异常值
            if pd.api.types.is_numeric_dtype(df[col]):
                # 使用.values.any()避免Series歧义
                if (df[col] < 0).values.any():
                    report["warnings"].append(f"列'{col}'包含负值")

        # 自定义规则验证
        if rules:
            for rule in rules:
                try:
                    # 使用eval的安全检查
                    if not pd.eval(rule, local_dict={"df": df}):
                        report["errors"].append(f"违反规则: {rule}")
                except Exception as e:

                    report["errors"].append(f"规则验证失败: {rule} ({str(e)})")

        return report
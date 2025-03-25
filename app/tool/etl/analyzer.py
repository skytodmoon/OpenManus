import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from ..base import BaseTool

class DataAnalyzer(BaseTool):
    """多维数据分析器｜生成深度分析报告"""

    name = "etl_analyzer"
    parameters = {
        "type": "object",
        "properties": {
            "analysis_level": {
                "type": "integer",
                "minimum": 1,
                "maximum": 3,
                "default": 2
            }
        }
    }

    async def execute(self, df: pd.DataFrame, config: dict) -> dict:
        report = {"basic": self.basic_analysis(df)}

        if config.get("analysis_level", 2) >= 2:
            report["advanced"] = self.advanced_analysis(df)

        if config.get("analysis_level", 2) >= 3:
            report["predictive"] = self.predictive_analysis(df)

        return report

    def basic_analysis(self, df: pd.DataFrame) -> dict:
        return {
            "data_shape": df.shape,
            "dtypes": df.dtypes.astype(str).to_dict(),
            "missing_values": df.isna().sum().to_dict(),
            "unique_counts": df.nunique().to_dict()
        }

    def advanced_analysis(self, df: pd.DataFrame) -> dict:
        numeric_df = df.select_dtypes(include=np.number)
        return {
            "correlation_matrix": numeric_df.corr().to_dict(),
            "skewness": numeric_df.skew().to_dict(),
            "kurtosis": numeric_df.kurtosis().to_dict()
        }

    def predictive_analysis(self, df: pd.DataFrame) -> dict:
        try:
            target = self._detect_target(df)
            X = df.drop(columns=[target])
            y = df[target]

            if pd.api.types.is_numeric_dtype(y):
                model = RandomForestRegressor()
            else:
                model = RandomForestClassifier()
                y = LabelEncoder().fit_transform(y)

            model.fit(X, y)
            return {
                "target": target,
                "feature_importance": dict(zip(X.columns, model.feature_importances_))
            }
        except Exception as e:
            return {"error": str(e)}

    def _detect_target(self, df: pd.DataFrame) -> str:
        # 自动选择最后一个数值列或第一个分类列作为目标
        numeric_cols = df.select_dtypes(include=np.number).columns
        if len(numeric_cols) > 0:
            return numeric_cols[-1]
        return df.columns[0]
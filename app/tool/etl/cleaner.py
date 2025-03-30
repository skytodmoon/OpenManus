from concurrent.futures import ThreadPoolExecutor

import pandas as pd

import numpy as np
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from ..base import BaseTool

class DataCleaner(BaseTool):
    """智能数据清洗器｜支持多种清洗策略"""

    name: str = "etl_cleaner"
    description: str = "执行自动化数据清洗流程，处理缺失值和异常值"
    parameters: dict = {
        "type": "object",
        "properties": {
            "missing_strategy": {
                "type": "string",
                "enum": ["drop", "simple_fill", "model_fill"],
                "description": "缺失值处理策略（丢弃/简单填充/模型填充）",
                "default": "drop"
            },
            "outlier_sensitivity": {
                "type": "number",
                "minimum": 1.0,
                "maximum": 3.0,
                "description": "异常值检测敏感度（1.0-宽松 3.0-严格）",
                "default": 2.0
            }
        },
        "required": []
    }

    # 增加并行处理和类型推断优化
    async def execute(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        # 先处理列重复问题
        df = df.loc[:, ~df.columns.duplicated()]

        # 改为串行处理确保顺序
        if config:
            df = self.handle_missing(df, config)
            df = self.handle_outliers(df, config)

        return df


    def handle_missing(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
        strategy = config.get("handle_missing", "drop")

        if strategy == "drop":
            # 确保检查所有列（包括处理后的列）
            cols_to_check = [c for c in df.columns if not c.endswith('.1')]  # 排除重复列
            return df.dropna(how='any', subset=cols_to_check)
        elif strategy == "simple_fill":
            return self._simple_imputation(df)
        elif strategy == "model_fill":
            return self._model_based_imputation(df)
        return df

    def _simple_imputation(self, df: pd.DataFrame) -> pd.DataFrame:
        num_cols = df.select_dtypes(include=np.number).columns
        cat_cols = df.select_dtypes(exclude=np.number).columns

        df[num_cols] = df[num_cols].fillna(df[num_cols].median())
        df[cat_cols] = df[cat_cols].fillna(df[cat_cols].mode().iloc[0])
        return df

    def _model_based_imputation(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = df.select_dtypes(include=np.number).columns
        imputer = IterativeImputer()
        df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        return df

    def handle_outliers(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
        strategy = config.get("outlier_strategy", "iqr")
        numeric_cols = df.select_dtypes(include=np.number).columns

        for col in numeric_cols:
            if strategy == "zscore":
                df = self._zscore_filter(df, col)
            else:
                df = self._iqr_filter(df, col)
        return df

    def _zscore_filter(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        zscore = (df[col] - df[col].mean()) / df[col].std()
        return df[(zscore.abs() < 3)]

    def _iqr_filter(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        return df[(df[col] >= q1 - 1.5*iqr) & (df[col] <= q3 + 1.5*iqr)]

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化DataFrame的数据类型以减少内存使用"""
        if df.empty:
            return df

        # 转换数值类型
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

        # 转换浮点类型
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')

        # 转换字符串类型
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df[col]) < 0.5:
                df[col] = df[col].astype('category')

        return df  # 只需返回优化后的DataFrame
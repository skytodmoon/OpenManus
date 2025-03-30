from functools import lru_cache

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from ..base import BaseTool

# 在文件顶部新增导入
from sklearn.exceptions import NotFittedError
from ...logger import logger


class DataAnalyzer(BaseTool):
    """多维数据分析器｜生成深度分析报告"""

    name: str = "etl_analyzer"
    description: str = "执行数据探索性分析（EDA），生成统计报告及预测模型"
    parameters: dict = {
        "type": "object",
        "properties": {
            "explore_depth": {
                "type": "integer",
                "minimum": 1,
                "maximum": 3,
                "description": "分析深度级别（1-基础统计 2-高级分析 3-预测建模）",
                "default": 2
            }
        },
        "required": []
    }

    async def execute(self, df: pd.DataFrame, config: dict) -> dict:
        depth = config.get("explore_depth", 1)

        # 根据探索深度分层执行
        return {
            "basic": self.basic_analysis(df) if depth >= 1 else {},
            "advanced": self.advanced_analysis(df) if depth >= 2 else {},
            "predictive": self.predictive_analysis(df, config) if depth >=3 else {}
        }

    # 移除@lru_cache装饰器
    def basic_analysis(self, df: pd.DataFrame) -> dict:
        logger.info("开始基础分析......")
        """基础数据分析"""
        return {
            "data_shape": df.shape,
            "dtypes": df.dtypes.astype(str).to_dict(),
            "stats": df.describe().to_dict(),
            "missing_values": df.isna().sum().to_dict()
        }


    def advanced_analysis(self, df: pd.DataFrame) -> dict:
        logger.info("开始高级分析......")
        numeric_df = df.select_dtypes(include=np.number)

        categorical_stats = {
            col: {
                "value_counts": df[col].value_counts().to_dict(),
                "top_value": df[col].mode()[0] if not df[col].mode().empty else None
            } for col in df.select_dtypes(exclude=np.number).columns
        }

        time_analysis = self._temporal_analysis(df)
        # 新增聚类分析
        from sklearn.cluster import KMeans
        numeric_df = df.select_dtypes(include=np.number)
        kmeans = KMeans(n_clusters=3).fit(numeric_df)
        # 统一数据结构版本
        return {
            "correlation": {
                "matrix": numeric_df.corr().to_dict(),
                "plot_data": numeric_df.corr().values.tolist()
            },
            "statistics": {
                "skewness": numeric_df.skew().to_dict(),
                "kurtosis": numeric_df.kurtosis().to_dict()
            },
            "categorical": categorical_stats,  # 新增分类数据统计
            "temporal": time_analysis,  # 新增时间序列分析
            "clusters": {
                "labels": kmeans.labels_.tolist(),
                "centers": kmeans.cluster_centers_.tolist()
        }
        }

    # 将方法移动到类的主体层级
    def _detect_target(self, df: pd.DataFrame) -> str:
        """更智能的目标列检测"""
        # 1. 优先检查列名包含'target'/'label'的列
        target_candidates = [col for col in df.columns if 'target' in col.lower() or 'label' in col.lower()]
        if target_candidates:
            return target_candidates[0]

        # 2. 原有逻辑作为fallback
        numeric_cols = df.select_dtypes(include=np.number).columns
        if len(numeric_cols) > 0:
            return numeric_cols[-1]  # 通常最后一列是目标列

        return df.columns[0]  # 默认返回第一列

    def _temporal_analysis(self, df: pd.DataFrame) -> dict:
        """增强时间序列分析"""
        time_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        if not time_cols:
            return {}

        return {
            col: {
                "min_date": df[col].min().isoformat(),
                "max_date": df[col].max().isoformat(),
                "time_interval": (df[col].max() - df[col].min()).days,
                "seasonality": self._check_seasonality(df[col]),  # 新增季节性检测
                "trend": self._detect_trend(df[col])  # 新增趋势分析
            } for col in time_cols
        }

    def predictive_analysis(self, df: pd.DataFrame, config: dict) -> dict:
        logger.info("开始预测分析......")
        try:
            # 保持原有逻辑
            algorithm = config.get("algorithm", "xgboost")
            # 过滤非数值列（包括时间戳）
            numeric_df = df.select_dtypes(include=[np.number])
            if numeric_df.empty:
                logger.warning("数值型列为空，无法进行预测分析")  # 添加日志
                return {"error": "无可用数值型列进行建模"}

            # 只使用数值列进行分析
            target = self._detect_target(numeric_df)

            if target not in numeric_df.columns:
                logger.error(f"目标列{target}不存在于数值型列")
                return {"error": f"目标列{target}无效"}

            X = numeric_df.drop(columns=[target])
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
            logger.error(f"预测分析异常: {str(e)}")
            return {"error": str(e)}

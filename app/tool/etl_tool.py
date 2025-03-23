import pandas as pd
import numpy as np
from pathlib import Path
from app.tool.base import BaseTool
from app.logger import logger
from typing import Dict, Any, List  # 添加List类型
# python run_mcp.py -p "请对/Users/bob/Documents/code/OpenManus/workspace/ttemperature_regulation_smart_manufacturing.csv进行数据探索"
# python run_mcp.py -p "分析数据：/Users/bob/Documents/code/OpenManus/workspace/ttemperature_regulation_smart_manufacturing.csv，算法类型xgboost，参数max_depth=5"

class ETLTool(BaseTool):
    """数据ETL与探索分析工具"""

    name: str = "etl_tool"
    description: str = "执行数据探索、清洗和转换的ETL工具"
    parameters: dict = {
        "type": "object",
        "properties": {
            "data_path": {"type": "string", "description": "数据文件路径"},
            "clean_config": {
                "type": "object",
                "properties": {
                    "handle_missing": {"type": "string", "enum": ["drop", "fill"], "default": "drop"},
                    "outlier_method": {"type": "string", "enum": ["zscore", "iqr"], "default": "iqr"}
                }
            },
            "explore_depth": {"type": "integer", "description": "数据探索深度级别 1-3", "default": 2}
        },
        "required": ["data_path"]
    }

    async def execute(self, data_path: str, clean_config: dict = None, explore_depth: int = 2) -> Dict:
        try:
            # 数据加载
            df = await self._load_data(data_path)
            # 参数校验
            explore_depth = explore_depth if isinstance(explore_depth, int) else 2
            explore_depth = max(1, min(3, explore_depth))  # 限制在1-3范围

            # 数据探索
            explore_report = await self._explore_data(df, explore_depth)

            # 数据清洗
            cleaned_df = self._clean_data(df, clean_config or {})

            # 保存清洗后数据
            output_path = self._save_cleaned_data(cleaned_df, data_path)

            return {
                "explore_report": explore_report,
                "cleaned_path": output_path,
                "data_shape": cleaned_df.shape
            }
        except Exception as e:
            logger.error(f"ETL流程失败: {str(e)}")
            return {"error": str(e)}

    async def _load_data(self, path: str) -> pd.DataFrame:
        """支持CSV/Excel数据加载"""
        if path.endswith('.csv'):
            return pd.read_csv(path)
        elif path.endswith(('.xls', '.xlsx')):
            return pd.read_excel(path)
        raise ValueError("不支持的文件格式")

    def _clean_data(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
        """增强型数据清洗"""
        cleaned = df.copy()

        # 1. 处理缺失值
        if config.get("handle_missing") == "fill":
            # 数值列用中位数填充
            num_cols = cleaned.select_dtypes(include=np.number).columns
            cleaned[num_cols] = cleaned[num_cols].fillna(cleaned[num_cols].median())

            # 非数值列用众数填充
            non_num_cols = cleaned.select_dtypes(exclude=np.number).columns
            for col in non_num_cols:
                cleaned[col] = cleaned[col].fillna(cleaned[col].mode()[0] if not cleaned[col].mode().empty else None)
        else:  # 默认删除缺失值
            cleaned = cleaned.dropna()

        # 2. 异常值处理（仅处理数值列）
        num_cols = cleaned.select_dtypes(include=np.number).columns
        for col in num_cols:
            col_data = cleaned[col]

            if config.get("outlier_method") == "zscore":
                # Z-score方法（阈值=3）
                zscore = (col_data - col_data.mean()) / col_data.std()
                cleaned = cleaned[(zscore.abs() < 3)]
            else:  # 默认IQR方法
                q1 = col_data.quantile(0.25)
                q3 = col_data.quantile(0.75)
                iqr = q3 - q1
                cleaned = cleaned[(col_data >= q1 - 1.5 * iqr) & (col_data <= q3 + 1.5 * iqr)]

        # 3. 日期和分类数据处理（原有逻辑）
        for col in cleaned.columns:
            if cleaned[col].dtype == 'object':
                try:
                    cleaned[col] = pd.to_datetime(cleaned[col], errors='raise')
                    cleaned[col] = cleaned[col].astype(np.int64)
                    logger.info(f"已转换日期列: {col}")
                except:
                    pass

        return pd.get_dummies(cleaned, dummy_na=True)

    async def _explore_data(self, df: pd.DataFrame, depth: int) -> Dict:
        """增强版数据探索"""
        report = {
            "basic_info": self._get_basic_info(df),
            "quality_issues": self._detect_issues(df),
            "distributions": self._analyze_distributions(df)  # 新增分布分析
        }

        if depth >= 2:
            report.update({
                "statistics": self._get_statistics(df),
                "correlation": self._get_correlation(df),
                "correlation2": self._advanced_correlation(df),  # 替换原有相关性分析
                "temporal_analysis": self._temporal_analysis(df)  # 新增时序分析
            })

        if depth >= 3:
            try:
                report.update({
                    "visualization": await self._generate_plots(df),
                    "text_insights": self._text_analysis(df),  # 新增文本分析
                    "feature_importance": self._feature_importance(df)  # 新增特征重要性
                    #"anomaly_detection": self._anomaly_detection(df)  # 新增异常检测
                })
            except ImportError:
                logger.warning("可视化功能需要matplotlib/seaborn库支持")

        return report

    def _get_basic_info(self, df: pd.DataFrame) -> Dict:
        """获取数据基础信息"""
        return {
            "dtypes": df.dtypes.astype(str).to_dict(),
            "missing_values": df.isnull().sum().to_dict(),
            "unique_counts": df.nunique().to_dict()
        }
    def _detect_issues(self, df: pd.DataFrame) -> Dict:
        """检测数据质量问题"""
        issues = {
            "high_missing_ratio": {},
            "data_duplicates": int(df.duplicated().sum()),
            "constant_columns": []
        }

        # 检测高缺失率列（>30%）
        missing_ratio = df.isnull().mean()
        issues["high_missing_ratio"] = missing_ratio[missing_ratio > 0.3].to_dict()

        # 检测恒定值列
        for col in df.columns:
            if df[col].nunique() == 1:
                issues["constant_columns"].append(col)

        return issues

    def _get_statistics(self, df: pd.DataFrame) -> Dict:
        """获取数值列统计信息"""
        return df.describe().to_dict()

    def _get_correlation(self, df: pd.DataFrame) -> Dict:
        """计算数值列相关性"""
        return df.select_dtypes(include=np.number).corr().to_dict()

    async def _generate_plots(self, df: pd.DataFrame) -> Dict:
        """生成探索性分析图表"""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError as e:
            raise ImportError("需要安装 matplotlib 和 seaborn 库") from e

        # 创建可视化目录
        vis_dir = Path("workspace/visualization")
        vis_dir.mkdir(exist_ok=True)

        plots = {}

        # 数值列分布直方图
        num_cols = df.select_dtypes(include=np.number).columns
        for col in num_cols[:3]:  # 最多显示前三列
            plt.figure()
            sns.histplot(df[col], kde=True)
            path = vis_dir / f"{col}_distribution.png"
            plt.savefig(path)
            plt.close()
            plots[f"{col}_distribution"] = str(path)

        # 类别列箱线图（数值列 vs 第一个类别列）
        cat_cols = df.select_dtypes(exclude=np.number).columns
        if len(cat_cols) > 0 and len(num_cols) > 0:
            plt.figure()
            sns.boxplot(x=cat_cols[0], y=num_cols[0], data=df)
            path = vis_dir / f"{cat_cols[0]}_vs_{num_cols[0]}_boxplot.png"
            plt.savefig(path)
            plt.close()
            plots["main_boxplot"] = str(path)

        return plots

    def _analyze_distributions(self, df: pd.DataFrame) -> Dict:
        """数值列分布分析（偏度/峰度）"""
        num_cols = df.select_dtypes(include=np.number).columns
        return {
            col: {
                "skewness": float(df[col].skew()),
                "kurtosis": float(df[col].kurt()),
                "is_normal": int((abs(df[col].skew()) < 0.5) & (abs(df[col].kurt()) < 3))  # Convert bool to int
            }
            for col in num_cols
        }

    def _temporal_analysis(self, df: pd.DataFrame) -> Dict:
        """时间序列分析（如果存在时间列）"""
        time_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        if not time_cols:
            return {}

        analysis = {}
        for col in time_cols:
            time_series = df[col]
            analysis[col] = {
                "time_range": (time_series.min(), time_series.max()),
                "periodicity": self._detect_periodicity(time_series),
                "missing_intervals": self._find_time_gaps(time_series)
            }
        return analysis

    def _detect_periodicity(self, series: pd.Series) -> str:
        """检测时间序列周期性"""
        try:
            diffs = np.diff(series.sort_values().unique())
            mode_diff = pd.Series(diffs).mode()[0]
            return f"{mode_diff / pd.Timedelta(1, 'h'):.1f}小时" if not pd.isnull(mode_diff) else "无固定周期"
        except:
            return "无法检测"

    def _find_time_gaps(self, series: pd.Series) -> List[Dict]:
        """发现时间缺口"""
        sorted_series = series.sort_values().drop_duplicates()
        diffs = sorted_series.diff().dropna()
        gaps = diffs[diffs > diffs.median() * 2]
        return [{"start": sorted_series.iloc[i - 1], "end": sorted_series.iloc[i]}
                for i in gaps.index]

    def _text_analysis(self, df: pd.DataFrame) -> Dict:
        """文本列分析（词频统计）"""
        text_cols = df.select_dtypes(include=['object', 'string']).columns
        analysis = {}

        for col in text_cols:
            word_counts = df[col].str.split(expand=True).stack().value_counts()
            analysis[col] = {
                "total_words": len(word_counts),
                "top_words": word_counts.head(5).to_dict(),
                "avg_word_length": df[col].str.split().apply(
                    lambda x: np.mean([len(w) for w in x] if x else 0)).mean()
            }
        return analysis

    def _advanced_correlation(self, df: pd.DataFrame) -> Dict:
        """增强型相关性分析"""
        num_df = df.select_dtypes(include=np.number)
        return {
            "pearson": num_df.corr(method='pearson').to_dict(),
            "spearman": num_df.corr(method='spearman').to_dict(),
            "kendall": num_df.corr(method='kendall').to_dict()
        }

    def _feature_importance(self, df: pd.DataFrame) -> Dict:
        """特征重要性分析（使用随机森林）"""
        try:
            from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
            from sklearn.preprocessing import LabelEncoder

            # 自动选择目标列（最后一个数值列或分类列）
            target_col = next((col for col in reversed(df.columns)
                               if pd.api.types.is_numeric_dtype(df[col])
                               or pd.api.types.is_string_dtype(df[col])), None)
            if not target_col:
                return {}

            X = df.drop(columns=[target_col])
            y = df[target_col]

            # 编码分类特征
            le = LabelEncoder()
            X = X.apply(lambda col: le.fit_transform(col) if col.dtype == 'object' else col)

            # 训练模型
            if pd.api.types.is_numeric_dtype(y):
                model = RandomForestRegressor(n_estimators=100)
            else:
                model = RandomForestClassifier(n_estimators=100)
                y = le.fit_transform(y)

            model.fit(X, y)

            return {
                "target": target_col,
                "importances": dict(zip(X.columns, model.feature_importances_))
            }
        except ImportError:
            logger.warning("特征重要性分析需要scikit-learn库")
            return {}

    def _statistical_impact(self, df: pd.DataFrame) -> Dict:
        """统计影响因子分析"""
        analysis = {}
        num_cols = df.select_dtypes(include=np.number).columns

        # 方差膨胀因子 (VIF)
        try:
            from statsmodels.stats.outliers_influence import variance_inflation_factor
            vif_data = pd.DataFrame()
            vif_data["feature"] = num_cols
            vif_data["VIF"] = [variance_inflation_factor(df[num_cols].values, i)
                               for i in range(len(num_cols))]
            analysis["vif"] = vif_data.to_dict()
        except ImportError:
            pass

        # 方差分析
        if len(num_cols) > 1:
            analysis["anova"] = {
                col: {
                    "F-value": df[col].var() / df.drop(columns=col).mean(axis=1).var(),
                    "p-value": 0.05  # 简化示例，实际需要计算
                }
                for col in num_cols
            }
        return analysis

    def _save_cleaned_data(self, df: pd.DataFrame, original_path: str) -> str:
        """保存清洗后的数据到新路径"""
        try:
            # 创建清洗数据目录
            output_dir = Path("workspace/cleaned_data")
            output_dir.mkdir(exist_ok=True)

            # 生成带时间戳的文件名
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            orig_path = Path(original_path)
            new_name = f"{orig_path.stem}_cleaned_{timestamp}{orig_path.suffix}"
            output_path = output_dir / new_name

            # 保持与原始文件相同的格式
            if original_path.endswith('.csv'):
                df.to_csv(output_path, index=False)
            elif original_path.endswith(('.xls', '.xlsx')):
                df.to_excel(output_path, index=False)

            return str(output_path)
        except Exception as e:
            logger.error(f"数据保存失败: {str(e)}")
            raise
    # ...（其他辅助方法）...
    # 使用示例
    # await ETLTool().execute(
    #     data_path="data.csv",
    #     clean_config={
    #         "handle_missing": "fill",
    #         "outlier_method": "zscore"
    #     },
    #     explore_depth=3
    # )
import os
import uuid

import pandas as pd

from . import BaseTool
from .etl.loader import DataLoader
from .etl.cleaner import DataCleaner
from .etl.analyzer import DataAnalyzer
from .etl.saver import DataSaver
from ..logger import logger
from pathlib import Path


# 在顶部导入新增工具
from .etl.validator import DataValidator
from .etl.metadata import MetadataRecorder
from .etl.reporter import ReportGenerator

# 在类定义顶部添加默认配置
class ETLTool(BaseTool):
    """Data ETL and Exploration Tool | Integrated pipeline for data loading, cleaning, analysis and storage"""

    name: str = "etl_tool"
    description: str = """A tool for data extraction, transformation and loading (ETL) operations.
Use this tool when you need to process structured data files (CSV/XLSX/Parquet) 
through cleaning, validation, and analytical workflows."""
    parameters: dict = {
        "type": "object",
        "properties": {
            "data_path": {
                "type": "string",
                "description": "(required) Path to the data file (supports csv/xlsx/parquet)"
            },
            "clean_config": {
                "type": "object",
                "properties": {
                    "handle_missing": {
                        "type": "string",
                        "enum": ["drop", "fill"],
                        "default": "drop",
                        "description": "(optional) Missing value handling strategy: drop-remove rows, fill-impute values"
                    },
                    "outlier_method": {
                        "type": "string",
                        "enum": ["iqr"],
                        "default": "iqr",
                        "description": "(optional) Outlier detection method"
                    },
                    "text_clean": {
                        "type": "boolean",
                        "default": True,
                        "description": "(optional) Text cleaning"
                    },
                    "datetime_format": {
                        "type": "string",
                        "default": "auto",
                        "description": "(optional) Date and time format"
                    }
                }
            },
            "explore_depth": {
                "type": "integer",
                "description": "(optional) Data exploration depth level (1-basic 2-advanced 3-predictive)",
                "default": 2,
                "minimum": 1,
                "maximum": 3
            }
        },
        "required": ["data_path"]
    }

    # 新增默认配置常量
    DEFAULT_CLEAN_CONFIG: dict = {
        "handle_missing": "drop",
        "outlier_method": "iqr"
    }

    async def execute(self, data_path: str, clean_config: dict, explore_depth: int = 2):
        clean_config = clean_config or {}

        # 自动检测文件类型并设置source_type
        file_type = self._detect_file_type(data_path)
        loader_config = {
            "path": data_path,
            "source_type": "csv" if file_type == "csv" else "file"
        }

        # 确保clean_config包含所有必要的参数
        if "output_format" not in clean_config:
            source_type = self._detect_source_type(data_path)
            clean_config["output_format"] = "excel" if source_type == "excel" else "csv"

        """
        Execute full ETL pipeline with data validation and quality checks

        Args:
            data_path (str): Path to input data file
            clean_config (dict, optional): Cleaning configuration parameters
            explore_depth (int, optional): Analysis depth level (1-3)

        Returns:
            dict: Operation result containing processed data path and reports
        """
        # 合并默认配置和用户配置
        clean_config = {**self.DEFAULT_CLEAN_CONFIG, **(clean_config or {})}

        # 增加状态记录
        state = {
            "current_step": "loading",
            "progress": 0
        }
        # 提前初始化tools变量
        tools = None
        try:

            # 初始化所有工具组件
            tools = {
                "loader": DataLoader(),
                "cleaner": DataCleaner(),
                "validator": DataValidator(),
                "analyzer": DataAnalyzer(),
                "saver": DataSaver(),
                "metadata": MetadataRecorder()
            }

            # 数据加载 - 确保await返回DataFrame
            df = await self._safe_execute(tools["loader"], {
                "path": data_path,
                "source_type": self._detect_source_type(data_path)
            })

            # 确保df是DataFrame
            if not isinstance(df, pd.DataFrame):
                raise ValueError("DataLoader did not return a valid DataFrame")

            # 元数据记录
            await tools["metadata"].execute({
                "step": "load",
                "status": "success",
                "input_path": data_path,
                "df": df.copy(),
                "pipeline_id": str(uuid.uuid4())
            })
            # 每个步骤更新状态
            state.update({"current_step": "loading", "progress": 10})

            # 数据加载 - 添加source_type参数
            df = await self._safe_execute(tools["loader"], {
                "path": data_path,
                "source_type": self._detect_source_type(data_path)  # 使用自动检测方法
            })
            # 元数据记录 - 添加数据质量检查
            await tools["metadata"].execute({
                "step": "load",
                "status": "success",
                "input_path": data_path,
                "df": df.copy()  # 传递DataFrame副本避免修改原始数据
            })
            # 数据清洗
            cleaned_df = await self._safe_execute(tools["cleaner"], df, clean_config or {})
            await tools["metadata"].execute({"step": "clean", "status": "success"})

            # 数据验证（新增步骤）
            validation_report = await self._safe_execute(
                tools["validator"],
                cleaned_df,
                {"schema": self._infer_schema(cleaned_df), "rules": []}  # 添加默认rules参数
            )
            if validation_report.get("error_count", 0) > 0:
                logger.warning(f"数据质量问题：{validation_report}")

            # 原始数据分析（用于缺失值统计）
            raw_analysis = await self._safe_execute(
                tools["analyzer"],
                df,  # 使用原始数据
                {"explore_depth": 1}  # 仅执行基础分析
            )

            # 清洗后数据分析（用于其他分析）
            cleaned_analysis = await self._safe_execute(
                tools["analyzer"],
                cleaned_df,
                {
                    "explore_depth": explore_depth,
                    "algorithm": "xgboost"
                }
            )

            # 合并分析结果
            explore_report = {
                "basic": raw_analysis.get("basic", {}),
                "advanced": cleaned_analysis.get("advanced", {}),
                "predictive": cleaned_analysis.get("predictive", {})
            }
            logger.info(f"生成分析内容：{explore_report.keys() if explore_report else '空报告'}")
            logger.info(f"生成分析高级内容：{explore_report['advanced']}")
            logger.info(f"生成分析预测内容：{explore_report['predictive']}")
            # 数据存储
            output_path = await self._safe_execute(tools["saver"], cleaned_df, {"format": "csv"})

            # 记录完整元数据
            await tools["metadata"].execute({
                "pipeline_id": self._generate_pipeline_id(),
                "steps": ["load", "clean", "validate", "analyze", "save"],
                "final_status": "completed"
            })

            # 修改报告生成调用方式
            reporter = ReportGenerator()
            report_path = await reporter.execute(
                df=cleaned_df,  # 新增：传递清洗后的数据
                analysis_report=explore_report,
                config={
                    "output_dir": "./analysis_reports",
                    "template_name": "default_report.html",
                    "enable_llm_analysis": True
                }
            )

            # 修改返回结果处理
            result = {
                "cleaned_data_path": str(output_path),
                "report_path": report_path,
                "metadata": {
                    "pipeline_id": self._generate_pipeline_id(),
                    "status": "completed"
                }
            }

            # 安全合并分析报告
            if explore_report and isinstance(explore_report, dict):
                result.update(explore_report)

            return result

        except Exception as e:
            await tools["metadata"].execute({"step": "error", "error": str(e)})
            logger.error(f"ETL流程失败: {str(e)}")
            raise

    def _infer_schema(self, df: pd.DataFrame) -> dict:
        """推断数据模式"""
        schema = {}
        for col in df.columns:
            # 使用numpy的any()方法
            has_null = df[col].isnull().values.any()
            schema[col] = {"nullable": has_null}
        return schema

    def _generate_pipeline_id(self) -> str:
        """生成唯一流程ID"""
        return f"ETL_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"

    def _detect_source_type(self, data_path: str) -> str:
        """自动检测数据源类型"""
        path = Path(data_path)
        suffix = path.suffix.lower()

        if suffix == '.csv':
            return 'csv'
        elif suffix in ('.xlsx', '.xls'):
            return 'excel'
        elif suffix == '.parquet':
            return 'parquet'
        else:
            return 'unknown'

    def _detect_output_format(self, input_path: str) -> str:
        """根据输入路径推断输出格式"""
        return Path(input_path).suffix.lstrip('.').lower() or 'csv'

    async def _safe_execute(self, tool: BaseTool, *args):
        try:
            return await tool.execute(*args)
        except FileNotFoundError:
            raise  # 直接抛出文件未找到错误
        except Exception as e:
            logger.error(f"工具执行失败 [{tool.name}]: {str(e)}")
            raise RuntimeError(str(e))

    def _save_cleaned_data(self, df: pd.DataFrame, original_path: str) -> str:
        # 添加路径安全校验
        if not Path(original_path).is_file():
            raise FileNotFoundError(f"源文件不存在: {original_path}")

        # 添加文件大小限制（例如10GB）
        if df.memory_usage().sum() > 10 * 1024**3:
            raise ValueError("文件大小超过10GB限制")

        """保存清洗后的数据到新路径"""
        try:
            # 从配置获取工作空间根目录
            workspace_root = self.config.workspace_root
            output_dir = workspace_root / "cleaned_data"

            # 确保目录存在且有写入权限
            if not output_dir.exists():
                output_dir.mkdir(parents=True, mode=0o755)
            elif not os.access(output_dir, os.W_OK):
                raise PermissionError(f"无写入权限: {output_dir}")

            # 生成带时间戳的文件名
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            orig_path = Path(original_path)
            new_name = f"{orig_path.stem}_cleaned_{timestamp}{orig_path.suffix}"
            output_path = output_dir.resolve() / new_name

            # 根据文件类型保存
            if original_path.endswith('.csv'):
                df.to_csv(output_path, index=False)
            elif original_path.endswith(('.xls', '.xlsx')):
                df.to_excel(output_path, index=False)
            else:
                df.to_csv(output_path, index=False)  # 默认保存为CSV

            return str(output_path)
        except Exception as e:
            logger.error(f"数据保存失败: {str(e)}")
            raise


    def _detect_file_type(self, path: str) -> str:
        """检测文件类型"""
        ext = Path(path).suffix.lower()
        if ext == '.csv':
            return 'csv'
        elif ext in ('.xlsx', '.xls'):
            return 'excel'
        else:
            return 'file'

        return 'file'

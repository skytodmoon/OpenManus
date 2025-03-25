from tenacity import retry, stop_after_attempt, wait_exponential
from . import BaseTool
from .etl.loader import DataLoader
from .etl.cleaner import DataCleaner
from .etl.analyzer import DataAnalyzer
from .etl.saver import DataSaver
from ..logger import logger


class ETLTool(BaseTool):
    """数据ETL与探索分析工具｜集成数据加载、清洗、分析、存储全流程"""

    name: str = "etl_tool"
    description: str = "执行数据探索、清洗和转换的ETL工具"
    parameters: dict = {
        "type": "object",
        "properties": {
            "data_path": {
                "type": "string",
                "description": "数据文件路径（支持csv/xlsx/parquet）"
            },
            "clean_config": {
                "type": "object",
                "properties": {
                    "handle_missing": {
                        "type": "string",
                        "enum": ["drop", "fill"],
                        "default": "drop",
                        "description": "缺失值处理策略：drop-删除缺失值 fill-填充缺失值"
                    },
                    "outlier_method": {
                        "type": "string",
                        "enum": ["zscore", "iqr"],
                        "default": "iqr",
                        "description": "异常值检测方法：zscore-Z分数法 iqr-四分位距法"
                    }
                }
            },
            "explore_depth": {
                "type": "integer",
                "description": "数据探索深度级别（1-基础分析 2-高级分析 3-预测分析）",
                "default": 2,
                "minimum": 1,
                "maximum": 3
            }
        },
        "required": ["data_path"]
    }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before=lambda _: logger.info("ETL流程重试中...")
    )
    async def execute(self, data_path: str, clean_config: dict = None, explore_depth: int = 2) -> Dict:
        """
        执行完整ETL流程
        Args:
            data_path: 数据文件路径（支持本地文件路径或数据库连接字符串）
            clean_config: 清洗配置字典（可选）
            explore_depth: 分析深度级别（默认2）

        Returns:
            Dict: 包含分析报告、清洗后数据路径、数据形状的字典

        Example:
            await ETLTool().execute(
                data_path="data.csv",
                clean_config={"handle_missing": "fill"},
                explore_depth=3
            )
        """
        try:
            # 初始化工具组件
            tools = {
                "loader": DataLoader(),
                "cleaner": DataCleaner(),
                "analyzer": DataAnalyzer(),
                "saver": DataSaver()
            }

            # 执行数据加载
            logger.info(f"🔍 开始加载数据：{data_path}")
            df = await self._safe_execute(tools["loader"], {
                "source_type": self._detect_source_type(data_path),
                "path": data_path
            })

            # 执行数据清洗
            logger.info("🧹 执行数据清洗...")
            cleaned_df = await self._safe_execute(tools["cleaner"], df, clean_config or {})

            # 执行数据分析
            logger.info("📊 生成分析报告...")
            report = await self._safe_execute(tools["analyzer"], cleaned_df, {
                "analysis_level": explore_depth
            })

            # 保存清洗数据
            logger.info("💾 保存处理结果...")
            output_path = await self._safe_execute(tools["saver"], cleaned_df, {
                "output_format": self._detect_output_format(data_path)
            })

            return {
                "explore_report": report,
                "cleaned_path": output_path,
                "data_shape": cleaned_df.shape
            }

        except Exception as e:
            logger.error(f"ETL流程最终失败: {str(e)}")
            return {"error": str(e)}
        finally:
            logger.info("✅ ETL流程执行完毕")

    def _detect_source_type(self, path: str) -> str:
        """智能识别数据源类型"""
        if path.startswith(("mysql://", "postgresql://")):
            return "database"
        if path.startswith("http"):
            return "api"
        return "file"

    def _detect_output_format(self, input_path: str) -> str:
        """根据输入路径推断输出格式"""
        return Path(input_path).suffix.lstrip('.').lower() or 'csv'

    async def _safe_execute(self, tool: BaseTool, *args) -> Any:
        """带错误处理的工具执行方法"""
        try:
            return await tool.execute(*args)
        except Exception as e:
            logger.error(f"工具执行失败 [{tool.__class__.__name__}]: {str(e)}")
            raise
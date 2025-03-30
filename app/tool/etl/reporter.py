from ..base import BaseTool
import pandas as pd
import plotly.express as px
from jinja2 import Environment, FileSystemLoader, TemplateError
from pathlib import Path
import uuid
import logging
from ...logger import logger


class ReportGenerator(BaseTool):
    """ETL可视化报告生成器｜生成交互式分析报告"""

    name: str = "etl_reporter"
    description: str = "生成包含可视化图表的HTML格式ETL分析报告"
    parameters: dict = {
        "type": "object",
        "properties": {
            "output_dir": {
                "type": "string",
                "default": "./reports",
                "description": "报告输出目录"
            },
            "template_name": {
                "type": "string",
                "default": "default_report.html",
                "description": "Jinja2模板名称"
            },
            "max_charts": {
                "type": "integer",
                "default": 30,
                "description": "最大生成图表数量"
            }
        }
    }

    async def execute(self, df: pd.DataFrame, analysis_report: dict, config: dict) -> dict:
        """生成分析报告"""
        config = config or {}
        analysis_report = config.get("analysis_report", {})

        try:

            output_dir = Path(config.get("output_dir", "./reports")).resolve()
            output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"输出目录已确认: {output_dir}")
            # 合并基础数据和分析结果
            full_report = {
                "basic": {
                    "data_shape": df.shape,  # 直接从传入的DataFrame获取
                    "columns": list(df.columns),
                    "missing_values": df.isna().sum().to_dict()
                },
                **analysis_report  # 合并分析器生成的结果
            }
            # 添加数据结构兼容性转换
            if "advanced" in analysis_report and "correlation_matrix" in analysis_report["advanced"]:
                analysis_report["advanced"]["correlation"] = {
                    "matrix": analysis_report["advanced"]["correlation_matrix"]
                }
            # 生成可视化图表（使用传入的df）
            visuals = await self._generate_visuals(
                report=full_report,
                df=df,  # 使用清洗后的数据
                max_charts=config.get("max_charts", 30)
            )

            # # 模板路径处理
            # template_dir = Path(__file__).parent.parent / "templates/etl"
            # if not template_dir.exists():
            #     raise FileNotFoundError(f"模板目录不存在: {template_dir}")

            # 模板渲染
            env = Environment(loader=FileSystemLoader("../templates/etl"))
            template = env.get_template(config.get("template_name", "default_report.html"))
            if not template:
                raise FileNotFoundError(f"模板文件不存在: {config.get('template_name', 'default_report.html')}")
            html_content = template.render(
                report=analysis_report or {"error": "无分析数据"},
                visuals=visuals or {"error": "无可视化内容"},
            )

            # 内容验证
            self._validate_report_content(html_content, analysis_report, visuals)

            # 生成唯一文件名
            report_name = f"report_{pd.Timestamp.now().strftime('%Y%m%d')}_{uuid.uuid4().hex[:6]}.html"
            report_path = output_dir / report_name

            # 二进制写入防止编码问题
            report_path.write_bytes(html_content.encode('utf-8'))
            logger.info(f"报告已保存: {report_path} ({len(html_content) // 1024}KB)")

            return str(report_path)

        except (TemplateError, IOError) as e:
            logger.error(f"模板处理失败: {type(e).__name__} - {str(e)}")
            raise
        except pd.errors.EmptyDataError as e:
            logger.error(f"空数据错误: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"报告生成失败: {type(e).__name__} - {str(e)}")
            raise

    async def _generate_visuals(self, report: dict, df: pd.DataFrame = None,
                                max_charts: int = 10) -> dict:
        """生成Plotly可视化图表（优先使用传入的DataFrame）"""
        visuals = {}

        try:

            # 基础统计可视化
            if report.get("basic") and not df.empty:
                numeric_cols = df.select_dtypes(include='number').columns.tolist()
                for i, col in enumerate(numeric_cols[:max_charts]):
                    try:
                        fig = px.histogram(df, x=col, title=f"{col}分布直方图")
                        visuals[f"{col}_dist"] = fig.to_html(full_html=False)
                    except Exception as e:
                        logger.warning(f"字段 {col} 可视化失败: {str(e)}")

            # 更新高级分析可视化部分
            if "advanced" in report and "correlation" in report["advanced"]:
                corr_data = report["advanced"]["correlation"]
                if "matrix" in corr_data:  # 兼容新旧数据结构
                    fig = px.imshow(
                        pd.DataFrame(corr_data["matrix"]),
                        labels=dict(x="特征", y="特征", color="相关性"),
                        title="特征相关性热力图"
                    )
                    visuals["correlation_heatmap"] = fig.to_html(full_html=False)

            # 预测分析可视化
            if report.get("predictive"):
                self._add_predictive_visuals(report["predictive"], visuals)

        except Exception as e:
            logger.error(f"可视化生成失败: {str(e)}")
            return {}

        return visuals

    def _load_data(self, data_path: str) -> pd.DataFrame:
        """加载数据集"""
        try:
            if data_path.endswith(".parquet"):
                return pd.read_parquet(data_path)
            return pd.read_csv(data_path)
        except Exception as e:
            raise ValueError(f"数据加载失败: {data_path} | 错误: {str(e)}")

    def _add_advanced_visuals(self, advanced: dict, visuals: dict):
        """处理高级分析图表"""
        try:
            if "correlation_matrix" in advanced:
                corr_matrix = pd.DataFrame(advanced["correlation_matrix"])
                fig = px.imshow(corr_matrix, labels=dict(x="特征", y="特征", color="相关性"))
                visuals["correlation_heatmap"] = fig.to_html(full_html=False)
        except Exception as e:
            logger.warning(f"相关性矩阵可视化失败: {str(e)}")

    def _add_predictive_visuals(self, predictive: dict, visuals: dict):
        """处理预测分析图表"""
        try:
            if "feature_importance" in predictive:
                importance = predictive["feature_importance"]
                df = pd.DataFrame({
                    "feature": importance.keys(),
                    "importance": importance.values()
                })
                fig = px.bar(df, x='feature', y='importance', title="特征重要性排序")
                visuals["feature_importance"] = fig.to_html(full_html=False)
        except Exception as e:
            logger.warning(f"特征重要性可视化失败: {str(e)}")

    def _validate_report_content(self, html: str, report: dict, visuals: dict):
        """验证报告内容有效性"""
        if len(html) < 1024:
            raise ValueError("生成的报告内容过短（小于1KB）")

        if not any(keyword in html for keyword in ["数据维度", "特征分布", "分析结果"]):
            logger.warning("报告可能缺少核心内容")

        logger.info(f"报告验证通过 - 分析字段: {len(report)}, 可视化图表: {len(visuals)}")

from ..base import BaseTool
import pandas as pd
import plotly.express as px
from jinja2 import Environment, FileSystemLoader, TemplateError
from pathlib import Path
import uuid
import logging
from ...logger import logger
from typing import ClassVar
import plotly.graph_objects as go

from typing import ClassVar, Type
from types import ModuleType
import datetime

# 在类定义前添加类型别名（如果需要）
class ReportGenerator(BaseTool):
    # 明确注解类型
    go: ClassVar[Type[ModuleType]] = go
    px: ClassVar[Type[ModuleType]] = px

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
                "default": 50,
                "description": "最大生成图表数量"
            },
            "enable_llm_analysis": {
                "type": "boolean",
                "default": True,
                "description": "是否启用大模型智能分析"
            },
            "max_analysis_charts": {
                "type": "integer",
                "default": 50,
                "description": "最大分析图表数量"
            }
        }
    }

    async def execute(self, df: pd.DataFrame, analysis_report: dict, config: dict) -> dict:
        logger.info(f"开始生成报告 - 输入数据形状: {df.shape if df is not None else '无数据'}")
        logger.debug(f"初始分析报告内容: {self._get_nested_keys(analysis_report)}")
        logger.debug(f"配置参数: {config}")

        try:
            config = config or {}
            # 增加输入数据验证
            if df is None or df.empty:
                logger.error("输入数据为空或无效")
                raise ValueError("输入数据为空")

            # 重构报告数据合并逻辑
            full_report = {
                "basic": {
                    "data_shape": df.shape,
                    "columns": list(df.columns),
                    "missing_values": df.isna().sum().to_dict(),
                    **analysis_report.get("basic", {})
                },
                "advanced": analysis_report.get("advanced", {}),
                "predictive": analysis_report.get("predictive", {}),
                "metadata": {
                    "report_generator": "v2",
                    "timestamp": pd.Timestamp.now().isoformat()
                }
            }

            #logger.info(f"合并后的报告结构: {self._get_nested_keys(full_report)}")
            logger.debug(f"基础数据详情: {full_report['basic']}")

            # 增加输出目录验证
            output_dir = Path(config.get("output_dir", "./reports")).resolve()
            logger.info(f"准备输出到目录: {output_dir}")
            if not output_dir.exists():
                output_dir.mkdir(parents=True)
                logger.info(f"已创建输出目录: {output_dir}")

            # 增加模板加载日志
            template_dir = Path("../templates/etl").resolve()
            logger.info(f"加载模板从: {template_dir}")
            env = Environment(loader=FileSystemLoader(str(template_dir)))

            template_name = config.get("template_name", "default_report.html")
            logger.info(f"尝试加载模板: {template_name}")
            template = env.get_template(template_name)

            # 增加可视化生成日志
            logger.info("开始生成可视化内容...")
            visuals = await self._generate_visuals(
                report=full_report,
                df=df,
                max_charts=config.get("max_charts", 50)
            )
            logger.info(f"生成可视化完成, 共 {len(visuals)} 个图表")

            # 增加渲染前验证
            if not visuals:
                logger.warning("未生成任何可视化图表!")

            # 新增LLM分析结果合并
            if config.get("enable_llm_analysis", True) and  ('advanced' in full_report or 'predictive' in full_report):  # 新增条件判断
                try:
                    llm_analysis = await self._add_llm_analysis(visuals, full_report)  # 确保传递完整报告
                    full_report["llm_analysis"] = llm_analysis  # 将结果合并到报告中
                except Exception as e:
                    logger.error(f"LLM分析失败: {str(e)}")
                    full_report["llm_analysis"] = {"error": "智能分析失败"}

            html_content = template.render(
                report=full_report,
                visuals=visuals,
                llm_analysis=full_report.get("llm_analysis", {}),  # 使用安全处理后的结果
                data_preview=df.head(10).to_dict('records')
            )

            # 增加内容验证
            self._validate_report_content(html_content, full_report, visuals)

            # 生成报告文件
            report_name = f"report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html"
            report_path = output_dir / report_name
            report_path.write_bytes(html_content.encode('utf-8'))

            logger.success(f"报告生成成功! 路径: {report_path}")
            logger.debug(f"报告大小: {len(html_content)//1024}KB")

            return str(report_path)

        except Exception as e:
            logger.error(f"报告生成失败: {type(e).__name__}", exc_info=True)
            raise

    async def _generate_visuals(self, report: dict, df: pd.DataFrame = None,
                               max_charts: int = 10) -> dict:
        logger.info(f"开始生成可视化图表，报告内容")
        """生成Plotly可视化图表（优先使用传入的DataFrame）"""
        visuals = {}

        if not df.empty:
            # 记录初始图表数量
            initial_count = len(visuals)
            logger.debug(f"初始图表数量: {initial_count}")

            # 基础统计可视化
            self._add_basic_visuals(report, df, visuals, max_charts)
            new_basic = len(visuals) - initial_count
            logger.info(f"生成基础统计图表 {new_basic} 个，类型包含直方图/箱线图")

            # 高级分析可视化
            if "advanced" in report:
                adv_count = len(visuals)
                self._add_advanced_visuals(report["advanced"], df, visuals, max_charts)
                new_adv = len(visuals) - adv_count
                logger.debug(f"新增高级图表 {new_adv} 个，包含相关性/统计指标/分类/时序图表")

            # 预测分析可视化
            if "predictive" in report:
                pred_count = len(visuals)
                self._add_predictive_visuals(report["predictive"], visuals)
                new_pred = len(visuals) - pred_count
                logger.debug(f"新增预测分析图表 {new_pred} 个")

            # 最终验证
            logger.success(f"图表生成完成，总计 {len(visuals)} 个图表")
            if len(visuals) == 0:
                logger.warning("未生成任何可视化图表，请检查输入数据或分析报告")
            else:
                logger.debug(f"生成图表列表: {list(visuals.keys())[:10]}{'...' if len(visuals)>10 else ''}")

        return visuals
    # Todo：处理前后数据对比
    # def _add_preprocessing_visuals(...):
    #     """清洗前后对比图"""
    #     try:
    #         fig = make_subplots(rows=1, cols=2)
    #         fig.add_trace(go.Histogram(x=original_data, name='原始数据'), row=1, col=1)
    #         fig.add_trace(go.Histogram(x=cleaned_data, name='清洗后'), row=1, col=2)
    #         visuals["preprocessing_compare"] = fig.to_html()
    #     except Exception as e:
    #         logger.warning(f"预处理对比图生成失败: {str(e)}")

    def _add_basic_visuals(self, report: dict, df: pd.DataFrame, visuals: dict, max_charts: int):
        """添加基础统计可视化图表"""
        numeric_cols = df.select_dtypes(include='number').columns.tolist()
        for col in numeric_cols[:max_charts]:
            try:
                # 数值型字段分布直方图
                fig = px.histogram(df, x=col, title=f"{col}分布直方图")
                visuals[f"{col}_dist"] = fig.to_html(full_html=False)

                # 数值型字段箱线图
                fig = px.box(df, y=col, title=f"{col}箱线图")
                visuals[f"{col}_box"] = fig.to_html(full_html=False)
            except Exception as e:
                logger.warning(f"基础字段 {col} 可视化失败: {str(e)}")
        # 在基础图表后新增分布对比
        if len(numeric_cols) > 1:
            try:
                fig = px.violin(df, y=numeric_cols[:5],
                                title="数值字段分布对比")
                visuals["distribution_comparison"] = fig.to_html()
            except Exception as e:
                logger.warning(f"分布对比图生成失败: {str(e)}")

    def _add_advanced_visuals(self, advanced: dict, df: pd.DataFrame, visuals: dict, max_charts: int):
        """添加高级分析可视化图表"""
        # 相关性分析
        if "correlation" in advanced:
            self._add_correlation_visuals(advanced["correlation"], visuals)

        # 统计指标
        if "statistics" in advanced:
            self._add_statistics_visuals(advanced["statistics"], visuals)

        # 分类数据
        if "categorical" in advanced:
            self._add_categorical_visuals(advanced["categorical"], visuals, max_charts)

        # 时间序列
        if "temporal" in advanced:
            self._add_temporal_visuals(advanced["temporal"], visuals)

    def _add_correlation_visuals(self, correlation: dict, visuals: dict):
        """添加相关性分析图表"""
        try:
            if "matrix" in correlation:
                logger.info("开始生成matrix...")
                fig = px.imshow(
                    pd.DataFrame(correlation["matrix"]),
                    labels=dict(x="特征", y="特征", color="相关性"),
                    title="特征相关性热力图"
                )
                visuals["correlation_heatmap"] = fig.to_html(full_html=False)
        except Exception as e:
            logger.warning(f"相关性矩阵可视化失败: {str(e)}")

    def _add_statistics_visuals(self, statistics: dict, visuals: dict):
        """添加统计指标图表"""
        try:
            # 从statistics参数创建DataFrame
            stats_df = pd.DataFrame(statistics.get('statistics', []))  # 新增数据源处理

            # stats_df = pd.DataFrame([
            #     {
            #         '特征': col,
            #         '偏度': stats.get('skewness', 0),
            #         '峰度': stats.get('kurtosis', 0)
            #     }
            #     for col, stats in statistics.items()
            # ])
            if stats_df.empty:
                logger.warning("统计指标DataFrame为空")
                return
            logger.info("开始生成statistics...")
            # 使用类变量访问
            fig = self.go.Figure(data=[self.go.Table(
                header=dict(values=['特征', '偏度', '峰度']),
                cells=dict(values=[
                    stats_df['特征'],
                    stats_df['偏度'].round(3),
                    stats_df['峰度'].round(3)
                ])
            )])
            fig.update_layout(title="数值特征分布形态统计")
            visuals["statistics_table"] = fig.to_html(full_html=False)

            # 偏度峰度散点图
            fig = px.scatter(
                stats_df,
                x='偏度',
                y='峰度',
                hover_name='特征',
                title="特征偏度与峰度分布"
            )
            visuals["skewness_kurtosis"] = fig.to_html(full_html=False)
        except Exception as e:
            logger.warning(f"统计指标可视化失败: {str(e)}")

    def _add_categorical_visuals(self, categorical: dict, visuals: dict, max_charts: int):
        """添加分类数据图表"""
        for col, stats in list(categorical.items())[:max_charts]:
            try:
                # 只展示前20个最常见的值，避免图表过于拥挤
                value_counts = dict(sorted(stats["value_counts"].items(),
                                         key=lambda x: x[1], reverse=True)[:20])

                fig = px.bar(
                    x=list(value_counts.keys()),
                    y=list(value_counts.values()),
                    title=f"{col}值分布 (Top 20)"
                )
                visuals[f"{col}_value_counts"] = fig.to_html()
            except Exception as e:
                logger.warning(f"分类字段 {col} 可视化失败: {str(e)}")

    def _add_temporal_visuals(self, temporal: dict, visuals: dict):
        """添加时间序列图表"""
        for col, analysis in temporal.items():
            # 新增季节性分解图
            if "seasonality" in analysis:
                try:
                    fig = px.line(
                        x=analysis["seasonality"]["dates"],
                        y=analysis["seasonality"]["values"],
                        title=f"{col}季节性分解"
                    )
                    visuals[f"{col}_seasonality"] = fig.to_html()
                except Exception as e:
                    logger.warning(f"时间序列 {col} 季节性可视化失败: {str(e)}")

            if "trend" in analysis:
                try:
                    fig = px.line(
                        x=analysis["trend"]["dates"],
                        y=analysis["trend"]["values"],
                        title=f"{col}时间趋势"
                    )
                    visuals[f"{col}_trend"] = fig.to_html()
                except Exception as e:
                    logger.warning(f"时间序列 {col} 趋势可视化失败: {str(e)}")

    def _add_predictive_visuals(self, predictive: dict, visuals: dict):
        """添加预测分析图表"""
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

    def _validate_report_content(self, html: str, full_report: dict, visuals: dict):
        """验证报告内容有效性"""
        if len(html) < 1024:
            raise ValueError("生成的报告内容过短（小于1KB）")

        if not any(keyword in html for keyword in ["数据维度", "特征分布", "分析结果"]):
            logger.warning("报告可能缺少核心内容")

        # 合并报告后添加内容跟踪日志
        logger.info(f"报告内容初始化 - Basic字段: {list(full_report['basic'].keys())}")
        if 'advanced' in full_report:
            logger.debug(f"Advanced分析包含: {list(full_report['advanced'].keys())}")
        if 'predictive' in full_report:
            logger.debug(f"Predictive分析包含: {len(full_report['predictive'].get('feature_importance', {}))}个特征")

        try:
            # 数据结构标准化后添加日志
            if "advanced" in full_report:
                logger.info("标准化高级分析数据结构")
                if "correlation" in full_report["advanced"]:
                    logger.debug(f"相关性矩阵维度: {len(full_report['advanced']['correlation']['matrix'])}x{len(full_report['advanced']['correlation']['matrix'])}")

            # 生成可视化前记录基础信息
            logger.debug(f"基础数据维度: {full_report['basic']['data_shape']} | 包含列: {full_report['basic']['columns']}")


            # 添加可视化生成日志
            logger.info(f"生成可视化图表完成 | 图表类型分布: {self._get_visuals_summary(visuals)}")
            logger.debug(f"可视化图表列表: {list(visuals.keys())}")

            # 模板渲染前记录报告摘要
            logger.info("报告内容摘要: " + self._generate_report_summary(full_report))
            logger.debug(f"完整报告结构: {list(full_report.keys())}")

        except (TemplateError, IOError) as e:
            logger.error(f"模板处理失败: {type(e).__name__} - {str(e)}")
            raise
        except pd.errors.EmptyDataError as e:
            logger.error(f"空数据错误: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"报告生成失败: {type(e).__name__} - {str(e)}")
            raise

    def _get_visuals_summary(self, visuals: dict) -> dict:
        """获取可视化图表类型统计"""
        from collections import defaultdict
        summary = defaultdict(int)
        for key in visuals:
            if '_dist' in key:
                summary['distribution'] += 1
            elif 'heatmap' in key:
                summary['heatmap'] += 1
            elif 'importance' in key:
                summary['feature_importance'] += 1
        return dict(summary)

    def _generate_report_summary(self, report: dict) -> str:
        """生成报告摘要"""
        parts = []
        if 'basic' in report:
            parts.append(f"基础统计({len(report['basic'])}项)")
        if 'advanced' in report:
            parts.append(f"高级分析({len(report['advanced'])}项)")
        if 'predictive' in report:
            parts.append(f"预测模型({len(report['predictive'])}项)")
        return " | ".join(parts)

    def _get_nested_keys(self, d, prefix='', is_last=False, is_root=True):
        """生成树状结构键路径"""
        if not isinstance(d, dict):
            return ""

        lines = []
        keys = list(d.keys())
        for i, key in enumerate(keys):
            is_last_key = i == len(keys) - 1
            connector = '└── ' if is_last_key else '├── '

            if is_root:
                lines.append(f"{prefix}{connector}{key}")
                new_prefix = ''
            else:
                lines.append(f"{prefix}{connector}{key}")
                new_prefix = '    ' if is_last else '│   '

            if isinstance(d[key], dict):
                lines.append(self._get_nested_keys(
                    d[key],
                    prefix=new_prefix,
                    is_last=is_last_key,
                    is_root=False
                ))
        return '\n'.join(lines)
        return '\n'.join(lines)

    def _extract_key_stats(self, report: dict, chart_id: str) -> str:
        """从报告中提取具体数值的统计信息"""
        try:
            # 安全格式化数值
            def safe_format(value):
                try:
                    return f"{float(value):.3f}" if value is not None else "N/A"
                except (TypeError, ValueError):
                    return str(value)

            # 根据图表ID确定分析模块
            if '_dist' in chart_id or '_box' in chart_id:
                col = chart_id.split('_')[0]
                stats = report['basic'].get(col, {})
                return f"均值: {safe_format(stats.get('mean'))} | 标准差: {safe_format(stats.get('std'))}"

            if chart_id == "correlation_heatmap":
                matrix = report['advanced']['correlation']['matrix']
                values = [v for row in matrix.values() for v in row.values() if abs(v) < 1]
                top_corr = max(values, key=abs) if values else 0
                return f"最大相关性: {safe_format(top_corr)} (排除完全相关)"

            if chart_id == "statistics_table":
                stats = report['advanced']['statistics']['statistics']
                return "\n".join([f"{s['特征']} 偏度: {safe_format(s.get('偏度'))} | 峰度: {safe_format(s.get('峰度'))}"
                                  for s in stats])

            if '_value_counts' in chart_id:
                col = chart_id.split('_')[0]
                counts = report['advanced']['categorical'][col]['value_counts']
                top_item = next(iter(counts.items()), (None, 0))
                return f"最常见值: {top_item[0]} ({safe_format(top_item[1])}次)"

        except Exception as e:
            logger.warning(f"提取关键统计失败: {str(e)}")
            return "统计信息提取异常"

    def _generate_llm_summary(self, analysis: dict) -> str:
        """生成结构化综合分析总结"""
        if not analysis or "error" in analysis:
            return "暂无综合分析内容"

        summary = ["<h4>关键发现</h4><ul>"]
        for chart_id, text in analysis.items():
            if chart_id != "error":
                summary.append(f"<li><b>{chart_id.split('_')[0]}</b>: {text[:120]}...</li>")
        summary.append("</ul>")

        return "".join(summary)

    async def _add_llm_analysis(self, visuals: dict, report: dict) -> dict:
        """使用大模型分析可视化图表"""
        from app.agent.cot import CoTAgent

        analysis = {}
        try:
            if not isinstance(visuals, dict) or not visuals:
                return analysis

            logger.info(f"开始LLM分析，共{len(visuals)}个图表需要处理")
            for chart_id in visuals:
                logger.info(f"开始分析图表: {chart_id}")

                # 提取完整的图表元数据
                chart_meta = {
                    "title": self._get_chart_title(chart_id, report),
                    "chart_type": self._get_chart_type(chart_id),
                    "column": chart_id.split('_')[0],
                    "data_shape": report['basic']['data_shape'],
                    "key_stats": self._extract_key_stats(report, chart_id)
                }

                # 构建结构化提示词
                prompt = f"""基于以下数据特征生成专业分析报告：
                    
            **图表元数据**
            - 标题：{chart_meta['title']}
            - 类型：{chart_meta['chart_type']}
            - 关联字段：{chart_meta['column']}
            - 数据维度：{chart_meta['data_shape'][0]}行×{chart_meta['data_shape'][1]}列
        
            **核心统计**
            {chart_meta['key_stats']}
        
            **分析要求**
            1. 趋势分析（100字内）：描述数据分布的主要模式
            2. 异常检测（80字内）：指出潜在异常点及判断依据
            3. 业务建议（120字内）：提出可落地的改进措施
            4. 风险预警（60字内）：对于后续进一步分析的思路和指导
    
            请直接输出最终分析结果，不要包含思考过程或推理步骤。"""

                try:
                    agent = CoTAgent()
                    # 新增结果处理逻辑，确保只返回格式化内容
                    raw_result = await agent.run(prompt)
                    analysis[chart_id] = self._format_llm_output(raw_result)
                except Exception as e:
                    logger.warning(f"图表分析异常: {str(e)}")
                    continue

            return analysis
        except Exception as e:
            logger.error(f"LLM分析失败: {str(e)}", exc_info=True)
            return {"error": "智能分析服务暂时不可用"}

    def _get_chart_title(self, chart_id: str, report: dict) -> str:
        """从报告数据结构中获取图表标题"""
        # 根据chart_id匹配报告中的字段
        if '_dist' in chart_id:
            col = chart_id.replace('_dist', '')
            return f"{col}字段分布直方图"
        elif '_box' in chart_id:
            col = chart_id.replace('_box', '')
            return f"{col}字段箱线图"
        # 其他图表类型匹配逻辑...
        return "数据分析图表"

    def _get_chart_type(self, chart_id: str) -> str:
        """解析图表类型"""
        if 'heatmap' in chart_id: return '相关性热力图'
        if 'box' in chart_id: return '箱线图'
        if 'dist' in chart_id: return '分布直方图'
        return '通用分析图'

    def _format_llm_output(self, raw_output: str) -> str:
        """优化后的LLM输出格式化方法，稳定提取Answer部分"""
        # 尝试提取Answer标记后的内容
        answer_start = raw_output.find("Answer:")
        if answer_start != -1:
            # 提取Answer:之后的所有内容
            answer_content = raw_output[answer_start + len("Answer:"):].strip()
            # 去除可能的多余标记和空白
            return answer_content.split("Terminated:")[0].strip()

        # 如果没有Answer标记，尝试提取四个标准部分
        sections = ["趋势分析", "异常检测", "业务建议", "风险预警"]
        result = []

        for section in sections:
            start_idx = raw_output.find(f"**{section}**")
            if start_idx != -1:
                end_idx = raw_output.find("**", start_idx + len(section) + 4)
                content = raw_output[start_idx:end_idx if end_idx != -1 else None].strip()
                # 去除重复的章节标题
                clean_content = content.replace(f"**{section}**", "").strip()
                result.append(f"**{section}**：{clean_content}")

        return "\n\n".join(result) if result else raw_output
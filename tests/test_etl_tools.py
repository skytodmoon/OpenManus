# Add project root to Python path
import sys
from pathlib import Path

import numpy as np

from app.tool.etl.analyzer import DataAnalyzer

sys.path.append(str(Path(__file__).parent.parent))
import pytest
import pandas as pd


from app.tool import etl
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 原导入（需修改）
from app.tool.etl_tool import ETLTool

# 新导入路径应为
from app.tool.etl.loader import DataLoader

@pytest.mark.asyncio
async def test_workspace_file_analysis():  # 确保函数名以test_开头
    """测试对/workspace/test.csv文件的基础ETL分析"""
    test_path = Path(__file__).parent.parent / "workspace/test.csv"
    if not test_path.exists():
        pytest.skip("测试文件不存在")

    etl = ETLTool()
    result = await etl.execute(
        data_path=str(test_path),
        clean_config={
            "handle_missing": "fill",
            "outlier_method": "iqr"
        },
        explore_depth=3
    )

    # 新增验证点
    assert "basic" in result
    assert "data_shape" in result["basic"]
    assert len(result["basic"]["data_shape"]) == 2

    # 验证清洗结果
    cleaned_df = pd.read_csv(result['cleaned_data_path'])
    assert not cleaned_df.empty
    assert 'Timestamp' in cleaned_df.columns
    assert cleaned_df['Current Temperature (\xb0C)'].max() > 100  # 异常值处理验证

    # 验证报告生成
    assert Path(result['report_path']).exists()

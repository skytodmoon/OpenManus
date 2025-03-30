# Add project root to Python path
import sys
from pathlib import Path
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


@pytest.fixture
def sample_csv(tmp_path):
    csv_path = tmp_path / "test.csv"
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', None],
        'value': [10.5, 20.3, 9999],
        'date': ['2023-01-01', 'invalid_date', '2023-01-03']
    })
    df.to_csv(csv_path, index=False)
    return str(csv_path)


@pytest.fixture
def sample_xlsx(tmp_path):
    """生成测试用Excel文件"""
    xlsx_path = tmp_path / "test.xlsx"
    df = pd.DataFrame({
        'product': ['A', 'B', 'C'],
        'price': [100, 200, None],
        'sales': [50, 150, -20]
    })
    df.to_excel(xlsx_path, index=False)
    return str(xlsx_path)


@pytest.mark.asyncio
async def test_basic_etl_flow(sample_csv, tmp_path):
    """测试完整ETL流程"""
    etl = ETLTool()
    result = await etl.execute(
        data_path=sample_csv,
        clean_config={
            "handle_missing": "drop",
            "outlier_method": "iqr"
        },
        explore_depth=2
    )

    # 验证输出文件
    output_path = Path(result['cleaned_data_path'])
    assert output_path.exists()
    assert output_path.suffix == '.csv'

    # 验证数据清洗结果
    cleaned_df = pd.read_csv(output_path)
    assert len(cleaned_df) == 2  # 缺失值处理
    assert 'value' in cleaned_df.columns
    assert cleaned_df['value'].max() < 100  # 异常值处理

    # 验证报告生成
    report_path = Path(result['report_path'])
    assert report_path.exists()
    assert report_path.suffix == '.html'


@pytest.mark.asyncio
async def test_file_format_support(sample_xlsx):
    """测试不同文件格式支持"""
    etl = ETLTool()
    result = await etl.execute(
        data_path=sample_xlsx,
        clean_config={
            "handle_missing": "fill",
            "output_format": "excel"  # 明确指定输出格式
        }
    )

    output_path = Path(result['cleaned_data_path'])
    assert output_path.suffix == '.xlsx'  # 验证输出格式
    df = pd.read_excel(output_path)
    assert not df.isnull().values.any()


@pytest.mark.asyncio
async def test_data_validation(tmp_path):
    """测试数据验证功能"""
    invalid_csv = tmp_path / "invalid.csv"
    # 生成包含两个明确错误的数据：空值和非法类型
    pd.DataFrame({
        'valid_col': [1, 2, 3],
        'invalid_col': [None, 'invalid_str', 999]  # 包含空值和字符串类型的非法值
    }).to_csv(invalid_csv, index=False)

    etl = ETLTool()
    result = await etl.execute(
        data_path=str(invalid_csv),
        clean_config={
            "handle_missing": "keep",  # 保留空值用于验证
            "schema_validation": True  # 启用schema验证
        }
    )

    # 关键断言：确保error_count存在且符合预期
    assert "error_count" in result
    assert result["error_count"] >= 2  # 预期至少空值和类型错误两个错误


@pytest.mark.asyncio
async def test_error_handling():
    """测试错误处理机制"""
    etl = ETLTool()
    with pytest.raises(FileNotFoundError):
        await etl.execute(data_path="/invalid/path.csv")


@pytest.mark.asyncio
async def test_large_file_handling(tmp_path):
    """测试大文件处理能力"""
    # 生成1MB测试文件（约50,000行）
    large_csv = tmp_path / "large.csv"
    df = pd.DataFrame({'data': range(50000)})
    df.to_csv(large_csv, index=False)

    result = await etl.execute(data_path=str(large_csv))
    assert Path(result['cleaned_data_path']).stat().st_size < 1e6  # 1MB以下


@pytest.mark.asyncio
async def test_metadata_recording(sample_csv):
    """测试元数据记录功能"""
    etl = ETLTool()
    result = await etl.execute(sample_csv)

    assert "metadata" in result
    assert result['metadata']['final_status'] == "completed"
    assert len(result['metadata']['steps']) == 5


@pytest.mark.asyncio
async def test_data_exploration_analysis(sample_csv):
    """测试数据探索分析功能"""
    etl = ETLTool()
    result = await etl.execute(
        data_path=sample_csv,
        explore_depth=2  # 测试高级分析
    )

    # 验证分析结果包含基本统计信息
    assert 'statistics' in result
    assert 'correlations' in result
    assert 'xgboost_feature_importance' in result  # 验证算法配置生效

@pytest.mark.parametrize("explore_depth", [1, 2, 3])
@pytest.mark.asyncio
async def test_exploration_depth(sample_csv, explore_depth):
    """测试不同探索深度"""
    etl = ETLTool()
    result = await etl.execute(
        data_path=sample_csv,
        explore_depth=explore_depth
    )

    # 验证不同深度返回不同分析结果
    if explore_depth == 1:
        assert 'predictive_models' not in result
    elif explore_depth == 3:
        assert 'predictive_models' in result

@pytest.mark.asyncio
async def test_extreme_value_handling(tmp_path):
    """测试极端值处理"""
    extreme_csv = tmp_path / "extreme.csv"
    pd.DataFrame({
        'value': [1e10, -1e10, 0, None, 999999]
    }).to_csv(extreme_csv, index=False)

    result = await etl.execute(
        data_path=str(extreme_csv),
        clean_config={"outlier_method": "iqr"}
    )

    df = pd.read_csv(result['cleaned_data_path'])
    assert len(df) < 5  # 验证异常值被处理

@pytest.mark.asyncio
async def test_performance_benchmark(sample_csv):
    """测试性能基准"""
    import time
    start = time.time()

    await etl.execute(
        data_path=sample_csv,
        explore_depth=2
    )

    duration = time.time() - start
    assert duration < 5.0  # 执行时间应小于5秒


@pytest.mark.asyncio
async def test_workspace_file_analysis():
    """测试对/workspace/test.csv文件的基础ETL分析"""
    test_path = Path(__file__).parent.parent / "workspace/test.csv"
    if not test_path.exists():
        pytest.skip("测试文件不存在")

    etl = ETLTool()
    result = await etl.execute(
        data_path=str(test_path),
        clean_config={
            "handle_missing": "drop",
            "outlier_method": "iqr"
        },
        explore_depth=2
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


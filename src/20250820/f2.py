# quantileRegression.py
# 使用分位数回归进行R7预测
import pandas as pd
import numpy as np
from getData import getRawData
from sklearn.linear_model import QuantileRegressor
from sklearn.metrics import mean_absolute_percentage_error
import warnings
warnings.filterwarnings('ignore')

def quantileRegression(df, quantiles=[0.3, 0.5, 0.7, 0.9]):
    """
    使用分位数回归预测R7/R3比值
    
    参数:
    df: 输入数据框
    quantiles: 要计算的分位数列表
    
    返回:
    包含各分位数回归结果的DataFrame
    """
    # 复制数据框
    result_df = df.copy()
    
    # 舍弃指定列
    columns_to_drop = ['users_count', 'total_revenue_d1']
    for col in columns_to_drop:
        if col in result_df.columns:
            result_df = result_df.drop(columns=[col])
    
    # 确定groupby的依据列（除了total_revenue_d3、total_revenue_d7和install_day）
    revenue_cols = ['total_revenue_d3', 'total_revenue_d7']
    exclude_cols = revenue_cols + ['install_day']
    groupby_cols = [col for col in result_df.columns if col not in exclude_cols]
    
    grouped_results = []
    
    for group_values, group_data in result_df.groupby(groupby_cols):
        # 过滤掉r3为0的数据点
        valid_data = group_data[group_data['total_revenue_d3'] > 0].copy()
        
        if len(valid_data) >= 10:  # 分位数回归需要足够的样本
            X = valid_data[['total_revenue_d3']].values
            y = valid_data['total_revenue_d7'].values
            
            result_row = {}
            if len(groupby_cols) == 1:
                result_row[groupby_cols[0]] = group_values
            else:
                for i, col in enumerate(groupby_cols):
                    result_row[col] = group_values[i]
            
            # 计算各分位数的回归结果
            for q in quantiles:
                try:
                    # 分位数回归
                    qr = QuantileRegressor(quantile=q, alpha=0.01, solver='highs')
                    qr.fit(X, y)
                    slope = qr.coef_[0]
                    intercept = qr.intercept_
                    
                    # 合理性检查
                    if slope <= 0 or slope > 10:
                        # 如果回归结果不合理，使用经验分位数
                        ratios = y / X.flatten()
                        slope = np.quantile(ratios, q)
                        intercept = 0
                    
                    result_row[f'slope_q{int(q*100)}'] = slope
                    result_row[f'intercept_q{int(q*100)}'] = intercept
                    
                except Exception as e:
                    # 如果分位数回归失败，使用经验分位数
                    ratios = y / X.flatten()
                    result_row[f'slope_q{int(q*100)}'] = np.quantile(ratios, q)
                    result_row[f'intercept_q{int(q*100)}'] = 0
            
            # 添加样本数量信息
            result_row['sample_count'] = len(valid_data)
            
            grouped_results.append(result_row)
        
        elif len(valid_data) > 0:
            # 样本不足时，使用简单的经验分位数
            ratios = valid_data['total_revenue_d7'] / valid_data['total_revenue_d3']
            
            result_row = {}
            if len(groupby_cols) == 1:
                result_row[groupby_cols[0]] = group_values
            else:
                for i, col in enumerate(groupby_cols):
                    result_row[col] = group_values[i]
            
            for q in quantiles:
                result_row[f'slope_q{int(q*100)}'] = np.quantile(ratios, q)
                result_row[f'intercept_q{int(q*100)}'] = 0
            
            result_row['sample_count'] = len(valid_data)
            grouped_results.append(result_row)
    
    return pd.DataFrame(grouped_results)

def predictWithQuantileRegression(df, qr_df, quantile=50):
    """
    使用分位数回归结果进行预测并计算误差
    
    参数:
    df: 原始数据
    qr_df: 分位数回归结果
    quantile: 使用哪个分位数进行预测（默认50，即中位数）
    
    返回:
    包含预测误差的DataFrame
    """
    # 确定索引列
    slope_col = f'slope_q{quantile}'
    intercept_col = f'intercept_q{quantile}'
    
    if slope_col not in qr_df.columns:
        raise ValueError(f"分位数 {quantile} 的结果不存在")
    
    index_cols = [col for col in qr_df.columns 
                  if not col.startswith(('slope_q', 'intercept_q')) and col != 'sample_count']
    
    # 合并数据
    merged_df = df.merge(qr_df[index_cols + [slope_col, intercept_col]], 
                        on=index_cols, how='left')
    
    # 计算预测值
    merged_df['predicted_revenue_d7'] = (merged_df['total_revenue_d3'] * merged_df[slope_col] + 
                                        merged_df[intercept_col])
    
    # 将install_day转换为日期格式，并计算周
    merged_df['install_date'] = pd.to_datetime(merged_df['install_day'], format='%Y%m%d')
    merged_df['week'] = merged_df['install_date'].dt.isocalendar().week
    merged_df['year'] = merged_df['install_date'].dt.year
    merged_df['year_week'] = merged_df['year'].astype(str) + '_W' + merged_df['week'].astype(str).str.zfill(2)
    
    # 计算误差（MAPE）
    merged_df['error'] = np.where(
        merged_df['total_revenue_d7'] > 0,
        np.abs(merged_df['total_revenue_d7'] - merged_df['predicted_revenue_d7']) / merged_df['total_revenue_d7'],
        0
    )
    
    # 按照索引列分组，计算每组的平均误差
    grouped_results = []
    
    for group_values, group_data in merged_df.groupby(index_cols):
        # 计算该组的平均绝对百分比误差
        mape = group_data['error'].mean()
        
        # 按周汇总数据，计算按周的误差
        weekly_data = group_data.groupby(['year_week']).agg({
            'total_revenue_d7': 'sum',
            'predicted_revenue_d7': 'sum'
        }).reset_index()
        
        # 计算按周汇总后的误差
        weekly_data['weekly_error'] = np.where(
            weekly_data['total_revenue_d7'] > 0,
            np.abs(weekly_data['total_revenue_d7'] - weekly_data['predicted_revenue_d7']) / weekly_data['total_revenue_d7'],
            0
        )
        
        # 计算按周汇总的平均误差
        weekly_mape = weekly_data['weekly_error'].mean()
        
        # 构建结果行
        result_row = {}
        if len(index_cols) == 1:
            result_row[index_cols[0]] = group_values
        else:
            for i, col in enumerate(index_cols):
                result_row[col] = group_values[i]
        
        result_row['mape'] = mape
        result_row['weekly_mape'] = weekly_mape
        result_row['quantile_used'] = quantile
        result_row['sample_count'] = len(group_data)
        
        grouped_results.append(result_row)
    
    return pd.DataFrame(grouped_results)

def compareQuantiles(df, qr_df, quantiles=[30, 50, 70, 90]):
    """
    比较不同分位数的预测效果
    
    参数:
    df: 原始数据
    qr_df: 分位数回归结果
    quantiles: 要比较的分位数列表
    
    返回:
    包含各分位数预测效果对比的DataFrame
    """
    comparison_results = []
    
    for q in quantiles:
        try:
            error_result = predictWithQuantileRegression(df, qr_df, quantile=q)
            
            # 计算总体统计
            overall_stats = {
                'quantile': q,
                'avg_mape': error_result['mape'].mean(),
                'avg_weekly_mape': error_result['weekly_mape'].mean(),
                'median_mape': error_result['mape'].median(),
                'median_weekly_mape': error_result['weekly_mape'].median(),
                'groups_count': len(error_result)
            }
            
            comparison_results.append(overall_stats)
            
        except Exception as e:
            print(f"分位数 {q} 计算失败: {e}")
    
    return pd.DataFrame(comparison_results)

def analyzeQuantileRegression(df, qr_df):
    """
    分析分位数回归结果的统计特征
    
    参数:
    df: 原始数据
    qr_df: 分位数回归结果
    
    返回:
    分析结果的字典
    """
    analysis = {}
    
    # 基本统计
    analysis['total_groups'] = len(qr_df)
    analysis['avg_sample_count'] = qr_df['sample_count'].mean()
    analysis['median_sample_count'] = qr_df['sample_count'].median()
    
    # 各分位数斜率的统计
    slope_cols = [col for col in qr_df.columns if col.startswith('slope_q')]
    
    slope_stats = {}
    for col in slope_cols:
        quantile = col.replace('slope_q', '')
        slope_stats[f'q{quantile}'] = {
            'mean': qr_df[col].mean(),
            'median': qr_df[col].median(),
            'std': qr_df[col].std(),
            'min': qr_df[col].min(),
            'max': qr_df[col].max()
        }
    
    analysis['slope_statistics'] = slope_stats
    
    return analysis

def mainQuantileRegression():
    """
    主函数：执行完整的分位数回归分析流程
    """
    print("=== 分位数回归分析开始 ===")
    
    # 获取数据
    rawDf0, rawDf1, rawDf2 = getRawData()
    datasets = [
        ('rawDf0', rawDf0),
        ('rawDf1', rawDf1), 
        ('rawDf2', rawDf2)
    ]
    
    # 定义要使用的分位数
    quantiles = [0.3, 0.5, 0.7, 0.9]
    
    for dataset_name, df in datasets:
        print(f"\n=== 处理 {dataset_name} ===")
        print(f"数据形状: {df.shape}")
        
        # 1. 执行分位数回归
        print("执行分位数回归...")
        qr_result = quantileRegression(df, quantiles=quantiles)
        print(f"分位数回归结果: {qr_result.shape[0]} 个分组")
        
        # 2. 分析回归结果
        print("分析回归结果...")
        analysis = analyzeQuantileRegression(df, qr_result)
        print(f"平均样本数: {analysis['avg_sample_count']:.1f}")
        print("各分位数斜率统计:")
        for q, stats in analysis['slope_statistics'].items():
            print(f"  {q}: 均值={stats['mean']:.3f}, 中位数={stats['median']:.3f}, 标准差={stats['std']:.3f}")
        
        # 3. 比较不同分位数的预测效果
        print("比较不同分位数预测效果...")
        comparison = compareQuantiles(df, qr_result, quantiles=[30, 50, 70, 90])
        print("分位数预测效果对比:")
        print(comparison.round(4))
        
        # 4. 使用最佳分位数进行详细预测
        best_quantile = comparison.loc[comparison['avg_weekly_mape'].idxmin(), 'quantile']
        print(f"最佳分位数: Q{best_quantile}")
        
        detailed_error = predictWithQuantileRegression(df, qr_result, quantile=int(best_quantile))
        
        # 5. 保存结果
        print("保存结果...")
        
        # 保存分位数回归参数
        qr_result_sorted = qr_result.sort_values('sample_count', ascending=False).reset_index(drop=True)
        qr_result_sorted.to_csv(f'/src/data/quantile_regression_{dataset_name}.csv', index=False)
        print(f"分位数回归参数已保存: quantile_regression_{dataset_name}.csv")
        
        # 保存预测效果对比
        comparison.to_csv(f'/src/data/quantile_comparison_{dataset_name}.csv', index=False)
        print(f"分位数对比已保存: quantile_comparison_{dataset_name}.csv")
        
        # 保存详细误差结果
        detailed_error_sorted = detailed_error.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
        detailed_error_sorted.to_csv(f'/src/data/quantile_error_{dataset_name}.csv', index=False)
        print(f"详细误差结果已保存: quantile_error_{dataset_name}.csv")
        
        # 6. 输出前10个结果供查看
        print(f"\n{dataset_name} 分位数回归结果 (前10行):")
        print(qr_result.head(10))
        
        print(f"\n{dataset_name} 预测误差结果 (按weekly_mape排序，前10行):")
        print(detailed_error_sorted.head(10))
        
        print(f"\n{dataset_name} 预测误差结果 (按weekly_mape排序，后10行):")
        print(detailed_error_sorted.tail(10))

def getQuantileRegressionResults():
    """
    便捷函数：获取分位数回归的结果
    
    返回:
    三个数据集的分位数回归结果
    """
    rawDf0, rawDf1, rawDf2 = getRawData()
    
    qr0 = quantileRegression(rawDf0)
    qr1 = quantileRegression(rawDf1)
    qr2 = quantileRegression(rawDf2)
    
    return qr0, qr1, qr2

if __name__ == "__main__":
    mainQuantileRegression()
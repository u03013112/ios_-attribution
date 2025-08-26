# hierarchical_regression.py
# 基于用户分组的多种回归方法综合对比分析
import os
import pandas as pd
import numpy as np
from getData import getGroupData,getAosGpirCountryMediaGroupR3Data
from sklearn.linear_model import QuantileRegressor, HuberRegressor, RANSACRegressor, LinearRegression
from sklearn.metrics import mean_absolute_percentage_error
import warnings
warnings.filterwarnings('ignore')

class RegressionMethod:
    """
    回归方法基类
    """
    def __init__(self, name, description):
        self.name = name
        self.description = description
    
    def fit(self, df):
        """
        拟合方法，子类需要实现
        返回: 包含回归参数的DataFrame
        """
        raise NotImplementedError
    
    def predict(self, df, model_params):
        """
        预测方法，子类需要实现
        返回: 包含预测值的DataFrame
        """
        raise NotImplementedError

class SimpleRatioMethod(RegressionMethod):
    """
    简单比值平均方法 - 按分组拟合
    """
    def __init__(self):
        super().__init__("simple_ratio", "简单比值平均")
    
    def fit(self, df):
        result_df = df.copy()
        
        # 舍弃指定列
        columns_to_drop = ['users_count', 'total_revenue_d1']
        for col in columns_to_drop:
            if col in result_df.columns:
                result_df = result_df.drop(columns=[col])
        
        # 确定groupby的依据列（包含分组信息）
        revenue_cols = ['total_revenue_d3', 'total_revenue_d7']
        exclude_cols = revenue_cols + ['install_day']
        groupby_cols = [col for col in result_df.columns if col not in exclude_cols]
        
        # 按照分组列重新汇总数据（包括revenue_d3_min, revenue_d3_max）
        grouped_data = result_df.groupby(groupby_cols).agg({
            'total_revenue_d3': 'sum',
            'total_revenue_d7': 'sum'
        }).reset_index()
        
        # 计算汇总后的r7r3比值
        grouped_data['slope'] = np.where(
            grouped_data['total_revenue_d3'] > 0,
            grouped_data['total_revenue_d7'] / grouped_data['total_revenue_d3'],
            0
        )
        grouped_data['intercept'] = 0
        grouped_data['method'] = self.name
        
        # 只保留需要的列
        final_cols = groupby_cols + ['slope', 'intercept', 'method']
        return grouped_data[final_cols]
    
    def predict(self, df, model_params):
        # 确定索引列
        index_cols = [col for col in model_params.columns 
                     if col not in ['slope', 'intercept', 'method']]
        
        # 合并数据
        merged_df = df.merge(model_params[index_cols + ['slope', 'intercept']], 
                            on=index_cols, how='left')
        
        # 计算预测值
        merged_df['predicted_revenue_d7'] = (merged_df['total_revenue_d3'] * merged_df['slope'] + 
                                            merged_df['intercept'])
        
        return merged_df

class QuantileRegressionMethod(RegressionMethod):
    """
    分位数回归方法 - 按分组拟合
    """
    def __init__(self, quantile=0.5):
        super().__init__(f"quantile_{int(quantile*100)}", f"分位数回归(Q{int(quantile*100)})")
        self.quantile = quantile
    
    def fit(self, df):
        result_df = df.copy()
        
        # 舍弃指定列
        columns_to_drop = ['users_count', 'total_revenue_d1']
        for col in columns_to_drop:
            if col in result_df.columns:
                result_df = result_df.drop(columns=[col])
        
        # 确定groupby的依据列（包含分组信息）
        revenue_cols = ['total_revenue_d3', 'total_revenue_d7']
        exclude_cols = revenue_cols + ['install_day']
        groupby_cols = [col for col in result_df.columns if col not in exclude_cols]
        
        grouped_results = []
        
        for group_values, group_data in result_df.groupby(groupby_cols):
            valid_data = group_data[group_data['total_revenue_d3'] > 0].copy()
            
            if len(valid_data) >= 5:
                X = valid_data[['total_revenue_d3']].values
                y = valid_data['total_revenue_d7'].values
                
                try:
                    qr = QuantileRegressor(quantile=self.quantile, alpha=0.01, solver='highs')
                    qr.fit(X, y)
                    slope = qr.coef_[0]
                    intercept = qr.intercept_
                    
                    # 合理性检查
                    if slope <= 0 or slope > 10:
                        ratios = y / X.flatten()
                        slope = np.quantile(ratios, self.quantile)
                        intercept = 0
                        
                except Exception:
                    ratios = y / X.flatten()
                    slope = np.quantile(ratios, self.quantile)
                    intercept = 0
                
                result_row = {}
                if len(groupby_cols) == 1:
                    result_row[groupby_cols[0]] = group_values
                else:
                    for i, col in enumerate(groupby_cols):
                        result_row[col] = group_values[i]
                
                result_row['slope'] = slope
                result_row['intercept'] = intercept
                result_row['method'] = self.name
                result_row['sample_count'] = len(valid_data)
                
                grouped_results.append(result_row)
        
        return pd.DataFrame(grouped_results)
    
    def predict(self, df, model_params):
        # 确定索引列
        index_cols = [col for col in model_params.columns 
                     if col not in ['slope', 'intercept', 'method', 'sample_count']]
        
        # 合并数据
        merged_df = df.merge(model_params[index_cols + ['slope', 'intercept']], 
                            on=index_cols, how='left')
        
        # 计算预测值
        merged_df['predicted_revenue_d7'] = (merged_df['total_revenue_d3'] * merged_df['slope'] + 
                                            merged_df['intercept'])
        
        return merged_df

class RobustRegressionMethod(RegressionMethod):
    """
    鲁棒回归方法 - 按分组拟合
    """
    def __init__(self, method_type='huber'):
        self.method_type = method_type
        super().__init__(f"robust_{method_type}", f"鲁棒回归({method_type.upper()})")
    
    def fit(self, df):
        result_df = df.copy()
        
        # 舍弃指定列
        columns_to_drop = ['users_count', 'total_revenue_d1']
        for col in columns_to_drop:
            if col in result_df.columns:
                result_df = result_df.drop(columns=[col])
        
        # 确定groupby的依据列（包含分组信息）
        revenue_cols = ['total_revenue_d3', 'total_revenue_d7']
        exclude_cols = revenue_cols + ['install_day']
        groupby_cols = [col for col in result_df.columns if col not in exclude_cols]
        
        grouped_results = []
        
        for group_values, group_data in result_df.groupby(groupby_cols):
            valid_data = group_data[group_data['total_revenue_d3'] > 0].copy()
            
            if len(valid_data) >= 5:
                X = valid_data[['total_revenue_d3']].values
                y = valid_data['total_revenue_d7'].values
                
                try:
                    if self.method_type == 'huber':
                        model = HuberRegressor(epsilon=1.35, alpha=0.01)
                    elif self.method_type == 'ransac':
                        model = RANSACRegressor(random_state=42, min_samples=max(2, len(valid_data)//3))
                    else:
                        model = LinearRegression()
                    
                    model.fit(X, y)
                    
                    if self.method_type == 'ransac':
                        slope = model.estimator_.coef_[0]
                        intercept = model.estimator_.intercept_
                    else:
                        slope = model.coef_[0]
                        intercept = model.intercept_
                    
                    # 合理性检查
                    if slope <= 0 or slope > 10:
                        ratios = y / X.flatten()
                        slope = np.mean(ratios)
                        intercept = 0
                        
                except Exception:
                    ratios = y / X.flatten()
                    slope = np.mean(ratios)
                    intercept = 0
                
                result_row = {}
                if len(groupby_cols) == 1:
                    result_row[groupby_cols[0]] = group_values
                else:
                    for i, col in enumerate(groupby_cols):
                        result_row[col] = group_values[i]
                
                result_row['slope'] = slope
                result_row['intercept'] = intercept
                result_row['method'] = self.name
                result_row['sample_count'] = len(valid_data)
                
                grouped_results.append(result_row)
        
        return pd.DataFrame(grouped_results)
    
    def predict(self, df, model_params):
        # 确定索引列
        index_cols = [col for col in model_params.columns 
                     if col not in ['slope', 'intercept', 'method', 'sample_count']]
        
        # 合并数据
        merged_df = df.merge(model_params[index_cols + ['slope', 'intercept']], 
                            on=index_cols, how='left')
        
        # 计算预测值
        merged_df['predicted_revenue_d7'] = (merged_df['total_revenue_d3'] * merged_df['slope'] + 
                                            merged_df['intercept'])
        
        return merged_df

class WeightedRegressionMethod(RegressionMethod):
    """
    加权回归方法 - 按分组拟合
    """
    def __init__(self):
        super().__init__("weighted", "加权回归")
    
    def fit(self, df):
        result_df = df.copy()
        
        # 舍弃指定列，但保留users_count用于加权
        columns_to_drop = ['total_revenue_d1']
        for col in columns_to_drop:
            if col in result_df.columns:
                result_df = result_df.drop(columns=[col])
        
        # 确定groupby的依据列（包含分组信息）
        revenue_cols = ['total_revenue_d3', 'total_revenue_d7']
        exclude_cols = revenue_cols + ['install_day', 'users_count']
        groupby_cols = [col for col in result_df.columns if col not in exclude_cols]
        
        grouped_results = []
        
        for group_values, group_data in result_df.groupby(groupby_cols):
            valid_data = group_data[group_data['total_revenue_d3'] > 0].copy()
            
            if len(valid_data) >= 3:
                X = valid_data[['total_revenue_d3']].values
                y = valid_data['total_revenue_d7'].values
                
                # 计算权重
                if 'users_count' in valid_data.columns:
                    weights = np.sqrt(valid_data['users_count'].values)
                else:
                    weights = np.sqrt(X.flatten())
                
                try:
                    model = LinearRegression()
                    model.fit(X, y, sample_weight=weights)
                    slope = model.coef_[0]
                    intercept = model.intercept_
                    
                    # 合理性检查
                    if slope <= 0 or slope > 10:
                        ratios = y / X.flatten()
                        slope = np.average(ratios, weights=weights)
                        intercept = 0
                        
                except Exception:
                    ratios = y / X.flatten()
                    slope = np.average(ratios, weights=weights)
                    intercept = 0
                
                result_row = {}
                if len(groupby_cols) == 1:
                    result_row[groupby_cols[0]] = group_values
                else:
                    for i, col in enumerate(groupby_cols):
                        result_row[col] = group_values[i]
                
                result_row['slope'] = slope
                result_row['intercept'] = intercept
                result_row['method'] = self.name
                result_row['sample_count'] = len(valid_data)
                
                grouped_results.append(result_row)
        
        return pd.DataFrame(grouped_results)
    
    def predict(self, df, model_params):
        # 确定索引列
        index_cols = [col for col in model_params.columns 
                     if col not in ['slope', 'intercept', 'method', 'sample_count']]
        
        # 合并数据
        merged_df = df.merge(model_params[index_cols + ['slope', 'intercept']], 
                            on=index_cols, how='left')
        
        # 计算预测值
        merged_df['predicted_revenue_d7'] = (merged_df['total_revenue_d3'] * merged_df['slope'] + 
                                            merged_df['intercept'])
        
        return merged_df

def split_train_test(df, split_date='20250615'):
    """
    按日期切分训练集和测试集
    
    参数:
    df: 输入数据框
    split_date: 切分日期，格式为'YYYYMMDD'
    
    返回:
    train_df, test_df
    """
    train_df = df[df['install_day'] < split_date].copy()
    test_df = df[df['install_day'] >= split_date].copy()
    
    print(f"训练集: {len(train_df)} 行, 日期范围: {train_df['install_day'].min()} - {train_df['install_day'].max()}")
    print(f"测试集: {len(test_df)} 行, 日期范围: {test_df['install_day'].min()} - {test_df['install_day'].max()}")
    
    return train_df, test_df

def calculate_prediction_errors_with_aggregation(test_df, predictions):
    """
    计算预测误差 - 先汇总分组再计算误差
    
    关键：将分组数据汇总后再计算误差，保持与原来相同的结果格式
    """
    # 将install_day转换为日期格式，并计算周
    predictions['install_date'] = pd.to_datetime(predictions['install_day'], format='%Y%m%d')
    predictions['week'] = predictions['install_date'].dt.isocalendar().week
    predictions['year'] = predictions['install_date'].dt.year
    predictions['year_week'] = predictions['year'].astype(str) + '_W' + predictions['week'].astype(str).str.zfill(2)
    
    # ⭐ 关键步骤：确定最终分组列（排除收入分档列）
    revenue_cols = ['total_revenue_d3', 'total_revenue_d7', 'predicted_revenue_d7']
    exclude_cols = revenue_cols + ['install_day', 'install_date', 'week', 'year', 'year_week', 
                                  'revenue_d3_min', 'revenue_d3_max']  # ⭐ 排除分组列
    exclude_cols += ['slope', 'intercept']
    if 'users_count' in predictions.columns:
        exclude_cols.append('users_count')
    if 'total_revenue_d1' in predictions.columns:
        exclude_cols.append('total_revenue_d1')
    
    # 最终分组列（与原来的rawDf相同：国家、媒体、campaign等）
    final_groupby_cols = [col for col in predictions.columns if col not in exclude_cols]
    
    print(f"最终分组列（与原来相同）: {final_groupby_cols}")
    print(f"排除的列: {exclude_cols}")
    
    # ⭐ 第一步：按天和最终维度汇总（合并所有收入分档）
    daily_aggregated = predictions.groupby(final_groupby_cols + ['install_day', 'year_week']).agg({
        'total_revenue_d3': 'sum',      # 合并所有收入档位的R3
        'total_revenue_d7': 'sum',      # 合并所有收入档位的R7
        'predicted_revenue_d7': 'sum'   # 合并所有收入档位的预测R7
    }).reset_index()
    
    print(f"汇总后数据量: {len(daily_aggregated)} (原始分组数据: {len(predictions)})")
    
    # 计算每日误差（MAPE）
    daily_aggregated['error'] = np.where(
        daily_aggregated['total_revenue_d7'] > 0,
        np.abs(daily_aggregated['total_revenue_d7'] - daily_aggregated['predicted_revenue_d7']) / daily_aggregated['total_revenue_d7'],
        0
    )
    
    # ⭐ 第二步：按最终维度分组，计算每组的平均误差（与原来格式完全一样）
    grouped_results = []
    
    for group_values, group_data in daily_aggregated.groupby(final_groupby_cols):
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
        
        # ⭐ 构建结果行（与原来格式完全一样）
        result_row = {}
        if len(final_groupby_cols) == 1:
            result_row[final_groupby_cols[0]] = group_values
        else:
            for i, col in enumerate(final_groupby_cols):
                result_row[col] = group_values[i]
        
        result_row['daily_mape'] = mape
        result_row['weekly_mape'] = weekly_mape
        result_row['sample_count'] = len(group_data)
        result_row['total_revenue_d7'] = group_data['total_revenue_d7'].sum()
        result_row['predicted_revenue_d7'] = group_data['predicted_revenue_d7'].sum()
        
        grouped_results.append(result_row)
    
    return pd.DataFrame(grouped_results)

def hierarchical_regression_analysis():
    """
    基于分组数据的综合回归分析主函数
    """
    print("=== 基于用户分组的综合回归方法对比分析 ===")
    
    # # 获取分组数据
    # groupDf0, groupDf1, groupDf2 = getGroupData()
    # datasets = [
    #     ('groupDf0', groupDf0),
    #     ('groupDf1', groupDf1), 
    #     ('groupDf2', groupDf2)
    # ]

    groupDf2 = getAosGpirCountryMediaGroupR3Data(startDay='20250101', endDay='20250810',N=2)
    groupDf4 = getAosGpirCountryMediaGroupR3Data(startDay='20250101', endDay='20250810',N=4)
    groupDf8 = getAosGpirCountryMediaGroupR3Data(startDay='20250101', endDay='20250810',N=8)
    groupDf16 = getAosGpirCountryMediaGroupR3Data(startDay='20250101', endDay='20250810',N=16)
    groupDf32 = getAosGpirCountryMediaGroupR3Data(startDay='20250101', endDay='20250810',N=32)

    datasets = [
        ('groupDf2', groupDf2),
        ('groupDf4', groupDf4), 
        ('groupDf8', groupDf8),
        ('groupDf16', groupDf16),
        ('groupDf32', groupDf32)
    ]

    
    # 定义回归方法
    methods = [
        SimpleRatioMethod(),
        QuantileRegressionMethod(quantile=0.5),
        QuantileRegressionMethod(quantile=0.3),
        QuantileRegressionMethod(quantile=0.7),
        RobustRegressionMethod(method_type='huber'),
        RobustRegressionMethod(method_type='ransac'),
        WeightedRegressionMethod()
    ]
    
    # 分析每个数据集
    for dataset_name, df in datasets:
        print(f"\n=== 分析 {dataset_name} ===")
        print(f"原始数据形状: {df.shape}")
        
        # 检查是否包含分组列
        if 'revenue_d3_min' not in df.columns or 'revenue_d3_max' not in df.columns:
            print(f"警告: {dataset_name} 缺少分组列 revenue_d3_min 或 revenue_d3_max，跳过分析")
            continue
        
        print(f"分组信息: revenue_d3_min 范围 {df['revenue_d3_min'].min()}-{df['revenue_d3_min'].max()}")
        print(f"分组信息: revenue_d3_max 范围 {df['revenue_d3_max'].min()}-{df['revenue_d3_max'].max()}")
        
        # 1. 切分训练集和测试集
        train_df, test_df = split_train_test(df, split_date='20250615')
        
        if len(train_df) == 0 or len(test_df) == 0:
            print(f"警告: {dataset_name} 训练集或测试集为空，跳过分析")
            continue
        
        # 2. 存储所有方法的结果
        all_results = []
        method_models = {}
        
        # 3. 对每种方法进行训练和预测
        for method in methods:
            print(f"\n--- 测试方法: {method.name} ({method.description}) ---")
            
            try:
                # 训练模型（按分组拟合）
                model_params = method.fit(train_df)
                
                if len(model_params) == 0:
                    print(f"警告: {method.name} 训练失败，跳过")
                    continue
                
                print(f"训练完成，得到 {len(model_params)} 个分组组合的参数")
                
                # 在测试集上预测
                predictions = method.predict(test_df, model_params)
                
                # 计算误差（汇总分组后计算）
                error_results = calculate_prediction_errors_with_aggregation(test_df, predictions)
                
                if len(error_results) == 0:
                    print(f"警告: {method.name} 预测失败，跳过")
                    continue
                
                # 添加方法标识
                error_results['method'] = method.name
                error_results['method_description'] = method.description
                
                # 计算总体统计
                avg_daily_mape = error_results['daily_mape'].mean()
                avg_weekly_mape = error_results['weekly_mape'].mean()
                median_weekly_mape = error_results['weekly_mape'].median()
                good_groups = len(error_results[error_results['weekly_mape'] <= 0.2])
                total_groups = len(error_results)
                
                print(f"结果: 平均日误差={avg_daily_mape:.3f}, 平均周误差={avg_weekly_mape:.3f}")
                print(f"好组合: {good_groups}/{total_groups} ({good_groups/total_groups*100:.1f}%)")
                
                # 保存结果
                all_results.append(error_results)
                method_models[method.name] = {
                    'model_params': model_params,
                    'error_results': error_results,
                    'summary': {
                        'avg_daily_mape': avg_daily_mape,
                        'avg_weekly_mape': avg_weekly_mape,
                        'median_weekly_mape': median_weekly_mape,
                        'good_groups': good_groups,
                        'total_groups': total_groups,
                        'good_ratio': good_groups/total_groups if total_groups > 0 else 0
                    }
                }
                
            except Exception as e:
                print(f"错误: {method.name} 执行失败 - {str(e)}")
                continue
        
        # 4. 汇总所有结果
        if all_results:
            combined_results = pd.concat(all_results, ignore_index=True)
            
            # 5. 创建方法对比汇总表
            method_summary = []
            for method_name, method_data in method_models.items():
                summary = method_data['summary'].copy()
                summary['method'] = method_name
                method_summary.append(summary)
            
            method_summary_df = pd.DataFrame(method_summary)
            method_summary_df = method_summary_df.sort_values('avg_weekly_mape').reset_index(drop=True)
            
            # 6. 为每个组合找到最佳方法
            # 确定分组列（不包含分组信息）
            group_cols = [col for col in combined_results.columns 
                         if col not in ['daily_mape', 'weekly_mape', 'sample_count', 
                                       'total_revenue_d7', 'predicted_revenue_d7', 
                                       'method', 'method_description']]
            
            print('>>分组列:', group_cols)
            best_method_per_group = []
            
            # 按组合分组，找到每个组合的最佳方法
            for group_values, group_data in combined_results.groupby(group_cols):
                best_row = group_data.loc[group_data['weekly_mape'].idxmin()].copy()
                
                # 添加是否需要进一步建模的标识
                best_row['needs_further_modeling'] = best_row['weekly_mape'] > 0.2
                best_row['is_good_performance'] = best_row['weekly_mape'] <= 0.1
                best_row['is_acceptable_performance'] = best_row['weekly_mape'] <= 0.2
                
                best_method_per_group.append(best_row)
            
            best_methods_df = pd.DataFrame(best_method_per_group)
            
            # 7. 保存结果
            print(f"\n=== 保存 {dataset_name} 结果 ===")
            
            # 保存详细结果
            combined_results.sort_values(by = group_cols, inplace=True)
            combined_results.to_csv(f'/src/data/hierarchical_results_{dataset_name}.csv', index=False)
            print(f"详细结果已保存: hierarchical_results_{dataset_name}.csv")
            
            # 保存方法对比汇总
            method_summary_df.to_csv(f'/src/data/hierarchical_method_summary_{dataset_name}.csv', index=False)
            print(f"方法对比汇总已保存: hierarchical_method_summary_{dataset_name}.csv")
            
            # 保存最佳方法分配
            best_methods_df.to_csv(f'/src/data/hierarchical_best_methods_{dataset_name}.csv', index=False)
            print(f"最佳方法分配已保存: hierarchical_best_methods_{dataset_name}.csv")
            
            # 8. 输出关键统计信息
            print(f"\n=== {dataset_name} 关键统计 ===")
            print("方法对比汇总:")
            print(method_summary_df[['method', 'avg_weekly_mape', 'good_ratio']].round(3))
            
            print(f"\n组合表现统计:")
            total_combinations = len(best_methods_df)
            good_combinations = len(best_methods_df[best_methods_df['is_good_performance']])
            acceptable_combinations = len(best_methods_df[best_methods_df['is_acceptable_performance']])
            need_further = len(best_methods_df[best_methods_df['needs_further_modeling']])
            
            print(f"总组合数: {total_combinations}")
            print(f"表现良好 (≤10%误差): {good_combinations} ({good_combinations/total_combinations*100:.1f}%)")
            print(f"表现可接受 (≤20%误差): {acceptable_combinations} ({acceptable_combinations/total_combinations*100:.1f}%)")
            print(f"需要进一步建模 (>20%误差): {need_further} ({need_further/total_combinations*100:.1f}%)")
            
            print(f"\n最佳方法分布:")
            method_distribution = best_methods_df['method'].value_counts()
            for method, count in method_distribution.items():
                print(f"{method}: {count} 个组合 ({count/total_combinations*100:.1f}%)")
            
            # 9. 分组效果分析
            print(f"\n=== 分组建模效果分析 ===")
            
            # 分析不同收入档位的表现
            if 'revenue_d3_min' in df.columns and 'revenue_d3_max' in df.columns:
                # 创建收入档位标识
                df_with_bins = df.copy()
                df_with_bins['revenue_bin'] = df_with_bins['revenue_d3_min'].astype(str) + '-' + df_with_bins['revenue_d3_max'].astype(str)
                
                # 统计各档位的数据量
                bin_stats = df_with_bins.groupby('revenue_bin').agg({
                    'total_revenue_d3': ['count', 'sum'],
                    'total_revenue_d7': 'sum'
                }).round(2)
                
                print("各收入档位统计:")
                print(bin_stats.head(10))
                
                # 分析各档位的预测效果（如果有足够数据）
                if len(best_methods_df) > 0:
                    # 这里可以添加更详细的分档效果分析
                    print(f"分组建模完成，共处理 {len(df_with_bins['revenue_bin'].unique())} 个收入档位")
        
        else:
            print(f"警告: {dataset_name} 没有成功的方法结果")

def compare_with_aggregated_results():
    """
    比较分组建模与汇总建模的效果差异
    """
    print("\n=== 分组建模 vs 汇总建模效果对比 ===")
    
    # 这个函数可以用来对比两种方法的效果
    # 需要同时运行 comprehensive_regression.py 和 hierarchical_regression.py
    # 然后比较结果文件
    
    datasets = ['groupDf0', 'groupDf1', 'groupDf2']
    
    for dataset_name in datasets:
        try:
            # 读取分组建模结果
            hierarchical_file = f'/src/data/hierarchical_method_summary_{dataset_name}.csv'
            hierarchical_results = pd.read_csv(hierarchical_file)
            
            # 读取汇总建模结果（如果存在）
            aggregated_file = f'/src/data/method_summary_{dataset_name.replace("group", "raw")}.csv'
            if os.path.exists(aggregated_file):
                aggregated_results = pd.read_csv(aggregated_file)
                
                print(f"\n{dataset_name} 效果对比:")
                print("分组建模最佳方法:")
                best_hierarchical = hierarchical_results.loc[0]
                print(f"  方法: {best_hierarchical['method']}, 周误差: {best_hierarchical['avg_weekly_mape']:.3f}")
                
                print("汇总建模最佳方法:")
                best_aggregated = aggregated_results.loc[0]
                print(f"  方法: {best_aggregated['method']}, 周误差: {best_aggregated['avg_weekly_mape']:.3f}")
                
                improvement = (best_aggregated['avg_weekly_mape'] - best_hierarchical['avg_weekly_mape']) / best_aggregated['avg_weekly_mape'] * 100
                print(f"改进幅度: {improvement:.1f}%")
            
        except Exception as e:
            print(f"对比 {dataset_name} 时出错: {e}")

def analyze_revenue_bin_performance():
    """
    分析不同收入档位的建模效果
    """
    print("\n=== 收入档位建模效果分析 ===")
    
    try:
        # 获取分组数据进行分析
        groupDf0, groupDf1, groupDf2 = getGroupData()
        datasets = [('groupDf0', groupDf0), ('groupDf1', groupDf1), ('groupDf2', groupDf2)]
        
        for dataset_name, df in datasets:
            if 'revenue_d3_min' in df.columns and 'revenue_d3_max' in df.columns:
                print(f"\n{dataset_name} 收入档位分析:")
                
                # 创建收入档位
                df['revenue_bin'] = df['revenue_d3_min'].astype(str) + '-' + df['revenue_d3_max'].astype(str)
                
                # 统计各档位
                bin_analysis = df.groupby('revenue_bin').agg({
                    'total_revenue_d3': ['count', 'sum', 'mean'],
                    'total_revenue_d7': ['sum', 'mean'],
                    'install_day': 'nunique'  # 天数
                }).round(2)
                
                bin_analysis.columns = ['观测次数', 'R3总和', 'R3均值', 'R7总和', 'R7均值', '天数']
                
                # 计算R7/R3比值
                bin_analysis['R7/R3比值'] = (bin_analysis['R7总和'] / bin_analysis['R3总和']).round(3)
                
                # 按R3总和排序，显示重要档位
                bin_analysis_sorted = bin_analysis.sort_values('R3总和', ascending=False)
                
                print("重要收入档位 (按R3总和排序):")
                print(bin_analysis_sorted.head(10))
                
                print(f"\n档位统计: 共 {len(bin_analysis)} 个档位")
                print(f"数据覆盖: {bin_analysis['天数'].sum()} 个档位-天组合")
                
    except Exception as e:
        print(f"收入档位分析出错: {e}")

def generate_summary_report():
    filenames = [
        ('rawDf', '/src/data/best_methods_rawDf.csv'),
        ('groupDf2', '/src/data/hierarchical_best_methods_groupDf2.csv'),
        ('groupDf4', '/src/data/hierarchical_best_methods_groupDf4.csv'), 
        ('groupDf8', '/src/data/hierarchical_best_methods_groupDf8.csv'),
        ('groupDf16', '/src/data/hierarchical_best_methods_groupDf16.csv'),
        ('groupDf32', '/src/data/hierarchical_best_methods_groupDf32.csv')
    ]

    print("=== 生成汇总报告 ===")
    
    # 存储所有数据框
    dataframes = []
    
    # 读取每个文件
    for name, filepath in filenames:
        if os.path.exists(filepath):
            print(f"读取文件: {filepath}")
            df = pd.read_csv(filepath)
            
            # 检查必要的列是否存在
            required_cols = ['app_package', 'country_group', 'mediasource', 'weekly_mape']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"警告: {name} 缺少列: {missing_cols}")
                continue
            
            # 只保留需要的列
            df_subset = df[required_cols].copy()
            
            # 重命名weekly_mape列，添加前缀
            df_subset = df_subset.rename(columns={'weekly_mape': f'weekly_mape_{name}'})
            
            print(f"{name}: {len(df_subset)} 行数据")
            
            dataframes.append((name, df_subset))
        else:
            print(f"文件不存在: {filepath}")
    
    if not dataframes:
        print("没有找到任何有效的数据文件")
        return
    
    # 开始合并数据
    print(f"\n开始合并 {len(dataframes)} 个数据集...")
    
    # 使用第一个数据框作为基础
    base_name, merged_df = dataframes[0]
    print(f"基础数据集: {base_name}")
    
    # 逐个合并其他数据框
    for name, df in dataframes[1:]:
        print(f"合并 {name}...")
        
        # 按照 app_package, country_group, mediasource 进行合并
        merged_df = merged_df.merge(
            df, 
            on=['app_package', 'country_group', 'mediasource'], 
            how='outer'
        )
        
        print(f"合并后行数: {len(merged_df)}")
    
    merged_df.to_csv('/src/data/summary_report.csv', index=False)
    print("\n汇总报告已保存: summary_report.csv")
    
    
    
    

if __name__ == "__main__":
    # # 执行主要分析
    # hierarchical_regression_analysis()
    
    # # 执行补充分析
    # analyze_revenue_bin_performance()
    
    # 生成总结报告
    generate_summary_report()

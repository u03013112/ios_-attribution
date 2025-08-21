# comprehensive_regression.py
# 多种回归方法综合对比分析
import pandas as pd
import numpy as np
from getData import getRawData
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
    简单比值平均方法
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
        
        # 确定groupby的依据列
        revenue_cols = ['total_revenue_d3', 'total_revenue_d7']
        exclude_cols = revenue_cols + ['install_day']
        groupby_cols = [col for col in result_df.columns if col not in exclude_cols]
        
        # 按照新的分组列重新汇总数据
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
    分位数回归方法
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
        
        # 确定groupby的依据列
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
    鲁棒回归方法
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
        
        # 确定groupby的依据列
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
    加权回归方法
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
        
        # 确定groupby的依据列
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

def calculate_prediction_errors(test_df, predictions):
    """
    计算预测误差
    """
    # 将install_day转换为日期格式，并计算周
    predictions['install_date'] = pd.to_datetime(predictions['install_day'], format='%Y%m%d')
    predictions['week'] = predictions['install_date'].dt.isocalendar().week
    predictions['year'] = predictions['install_date'].dt.year
    predictions['year_week'] = predictions['year'].astype(str) + '_W' + predictions['week'].astype(str).str.zfill(2)
    
    # 计算误差（MAPE）
    predictions['error'] = np.where(
        predictions['total_revenue_d7'] > 0,
        np.abs(predictions['total_revenue_d7'] - predictions['predicted_revenue_d7']) / predictions['total_revenue_d7'],
        0
    )
    
    # 确定分组列
    revenue_cols = ['total_revenue_d3', 'total_revenue_d7', 'predicted_revenue_d7']
    exclude_cols = revenue_cols + ['install_day', 'install_date', 'week', 'year', 'year_week', 'error']
    if 'users_count' in predictions.columns:
        exclude_cols.append('users_count')
    if 'total_revenue_d1' in predictions.columns:
        exclude_cols.append('total_revenue_d1')
    
    groupby_cols = [col for col in predictions.columns if col not in exclude_cols]
    
    # 按照索引列分组，计算每组的平均误差
    grouped_results = []
    
    for group_values, group_data in predictions.groupby(groupby_cols):
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
        if len(groupby_cols) == 1:
            result_row[groupby_cols[0]] = group_values
        else:
            for i, col in enumerate(groupby_cols):
                result_row[col] = group_values[i]
        
        result_row['daily_mape'] = mape
        result_row['weekly_mape'] = weekly_mape
        result_row['sample_count'] = len(group_data)
        result_row['total_revenue_d7'] = group_data['total_revenue_d7'].sum()
        result_row['predicted_revenue_d7'] = group_data['predicted_revenue_d7'].sum()
        
        grouped_results.append(result_row)
    
    return pd.DataFrame(grouped_results)

def comprehensive_regression_analysis():
    """
    综合回归分析主函数
    """
    print("=== 综合回归方法对比分析 ===")
    
    # 获取数据
    rawDf0, rawDf1, rawDf2 = getRawData()
    datasets = [
        ('rawDf0', rawDf0),
        ('rawDf1', rawDf1), 
        ('rawDf2', rawDf2)
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
                # 训练模型
                model_params = method.fit(train_df)
                
                if len(model_params) == 0:
                    print(f"警告: {method.name} 训练失败，跳过")
                    continue
                
                print(f"训练完成，得到 {len(model_params)} 个组合的参数")
                
                # 在测试集上预测
                predictions = method.predict(test_df, model_params)
                
                # 计算误差
                error_results = calculate_prediction_errors(test_df, predictions)
                
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
                good_groups = len(error_results[error_results['weekly_mape'] <= 0.3])
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
            # 确定分组列
            group_cols = [col for col in combined_results.columns 
                         if col not in ['daily_mape', 'weekly_mape', 'sample_count', 
                                       'total_revenue_d7', 'predicted_revenue_d7', 
                                       'method', 'method_description']]
            
            best_method_per_group = []
            
            # 按组合分组，找到每个组合的最佳方法
            for group_values, group_data in combined_results.groupby(group_cols):
                best_row = group_data.loc[group_data['weekly_mape'].idxmin()].copy()
                
                # 添加是否需要分层建模的标识
                best_row['needs_hierarchical'] = best_row['weekly_mape'] > 0.2
                best_row['is_good_performance'] = best_row['weekly_mape'] <= 0.1
                best_row['is_acceptable_performance'] = best_row['weekly_mape'] <= 0.2
                
                best_method_per_group.append(best_row)
            
            best_methods_df = pd.DataFrame(best_method_per_group)
            
            # 7. 保存结果
            print(f"\n=== 保存 {dataset_name} 结果 ===")
            
            # 保存详细结果
            combined_results.to_csv(f'/src/data/comprehensive_results_{dataset_name}.csv', index=False)
            print(f"详细结果已保存: comprehensive_results_{dataset_name}.csv")
            
            # 保存方法对比汇总
            method_summary_df.to_csv(f'/src/data/method_summary_{dataset_name}.csv', index=False)
            print(f"方法对比汇总已保存: method_summary_{dataset_name}.csv")
            
            # 保存最佳方法分配
            best_methods_df.to_csv(f'/src/data/best_methods_{dataset_name}.csv', index=False)
            print(f"最佳方法分配已保存: best_methods_{dataset_name}.csv")
            
            # 8. 输出关键统计信息
            print(f"\n=== {dataset_name} 关键统计 ===")
            print("方法对比汇总:")
            print(method_summary_df[['method', 'avg_weekly_mape', 'good_ratio']].round(3))
            
            print(f"\n组合表现统计:")
            total_combinations = len(best_methods_df)
            good_combinations = len(best_methods_df[best_methods_df['is_good_performance']])
            acceptable_combinations = len(best_methods_df[best_methods_df['is_acceptable_performance']])
            need_hierarchical = len(best_methods_df[best_methods_df['needs_hierarchical']])
            
            print(f"总组合数: {total_combinations}")
            print(f"表现良好 (≤10%误差): {good_combinations} ({good_combinations/total_combinations*100:.1f}%)")
            print(f"表现可接受 (≤20%误差): {acceptable_combinations} ({acceptable_combinations/total_combinations*100:.1f}%)")
            print(f"需要分层建模 (>20%误差): {need_hierarchical} ({need_hierarchical/total_combinations*100:.1f}%)")
            
            print(f"\n最佳方法分布:")
            method_distribution = best_methods_df['method'].value_counts()
            for method, count in method_distribution.items():
                print(f"{method}: {count} 个组合 ({count/total_combinations*100:.1f}%)")
        
        else:
            print(f"警告: {dataset_name} 没有成功的方法结果")

if __name__ == "__main__":
    comprehensive_regression_analysis()
# 用最简单方式进行拟合并验算
import pandas as pd
import numpy as np
from getData import getRawData,getGroupData,getNerfRData


# 计算7日收入与3日收入的比值
def r7r3(df):
    # 将参数中的df中的，列：users_count，total_revenue_d1 进行舍弃
    # 将除 total_revenue_d3、total_revenue_d7 的列作为 groupby 的依据
    # groupby之后，每一行计算一个比值
    # 最后将比值汇总求平均
    # 最终输出列：groupby的依据列（排除掉install_day），r7r3
    
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
    
    # 按照groupby列进行分组，并计算每组的r7r3比值
    grouped_results = []
    
    for group_values, group_data in result_df.groupby(groupby_cols):
        # 计算每行的r7r3比值
        group_data = group_data.copy()
        # 避免除零错误，当total_revenue_d3为0时，设置比值为0
        group_data['r7r3_ratio'] = np.where(
            group_data['total_revenue_d3'] > 0,
            group_data['total_revenue_d7'] / group_data['total_revenue_d3'],
            0
        )
        
        # 计算该组的平均比值
        avg_ratio = group_data['r7r3_ratio'].mean()
        
        # 构建结果行
        result_row = {}
        if len(groupby_cols) == 1:
            result_row[groupby_cols[0]] = group_values
        else:
            for i, col in enumerate(groupby_cols):
                result_row[col] = group_values[i]
        result_row['r7r3'] = avg_ratio
        
        grouped_results.append(result_row)
    
    # 转换为DataFrame
    final_result = pd.DataFrame(grouped_results)
    
    return final_result


# 与r7r3类似，区别是不再计算每天的r7r3比值，而是计算汇总后的r7r3比值
def r7r3Avg(df):
    # 将参数中的df中的，列：users_count，total_revenue_d1 进行舍弃
    # 将除 total_revenue_d3、total_revenue_d7 的列作为 groupby 的依据
    # groupby之后，先汇总total_revenue_d3和total_revenue_d7，然后计算比值
    # 最终输出列：groupby的依据列（排除掉install_day），r7r3
    
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
    
    # 按照groupby列进行分组，先汇总收入，再计算比值
    grouped_data = result_df.groupby(groupby_cols).agg({
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    
    # 计算汇总后的r7r3比值
    # 避免除零错误，当total_revenue_d3为0时，设置比值为0
    grouped_data['r7r3'] = np.where(
        grouped_data['total_revenue_d3'] > 0,
        grouped_data['total_revenue_d7'] / grouped_data['total_revenue_d3'],
        0
    )
    
    # 只保留需要的列
    final_cols = groupby_cols + ['r7r3']
    final_result = grouped_data[final_cols]
    
    return final_result

# 预测数据，并计算误差
def predictAndCalculateError(df, r7r3_df):
    # 将r7r3_df 中，除了r7r3列之外的列，作为df的索引
    # 用索引将 df 和 r7r3_df 进行合并
    # df中的total_revenue_d3 * 对应的r7r3值，作为预测的total_revenue_d7
    # 计算误差 = (total_revenue_d7 - 预测的total_revenue_d7) / total_revenue_d7
    # 最后按照索引分组，计算每组的平均误差（MAPE）
    
    # 确定索引列（r7r3_df中除了r7r3列之外的所有列）
    index_cols = [col for col in r7r3_df.columns if col != 'r7r3']
    
    # 将df和r7r3_df进行合并
    merged_df = df.merge(r7r3_df, on=index_cols, how='left')
    
    # 计算预测的total_revenue_d7
    merged_df['predicted_revenue_d7'] = merged_df['total_revenue_d3'] * merged_df['r7r3']
    
    # 将install_day转换为日期格式，并计算周
    merged_df['install_date'] = pd.to_datetime(merged_df['install_day'], format='%Y%m%d')
    merged_df['week'] = merged_df['install_date'].dt.isocalendar().week
    merged_df['year'] = merged_df['install_date'].dt.year
    merged_df['year_week'] = merged_df['year'].astype(str) + '_W' + merged_df['week'].astype(str).str.zfill(2)
    
    # 计算误差（MAPE）
    # 避免除零错误，当total_revenue_d7为0时，设置误差为0
    merged_df['error'] = np.where(
        merged_df['total_revenue_d7'] > 0,
        np.abs(merged_df['total_revenue_d7'] - merged_df['predicted_revenue_d7']) / merged_df['total_revenue_d7'],
        0
    )
    
    # 按照索引列分组，计算每组的平均误差（MAPE）
    grouped_results = []
    
    for group_values, group_data in merged_df.groupby(index_cols):
        # 计算该组的平均绝对百分比误差
        mape = group_data['error'].mean()
        
        # 按周汇总数据，计算按周的误差
        weekly_cols = index_cols + ['year_week']
        weekly_data = group_data.groupby(weekly_cols).agg({
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
        
        grouped_results.append(result_row)
    
    # 转换为DataFrame
    result_df = pd.DataFrame(grouped_results)
    
    return result_df


# 预测数据，并计算误差
# 分组版本，与predictAndCalculateError的区别是，计算预测值的部分完全一致
# 计算误差前，要将分组信息合并掉，即revenue_d3_min，revenue_d3_max两列去掉后重新汇总，再计算误差
def predictAndCalculateErrorGroup(df, r7r3_df):
    # 确定索引列（r7r3_df中除了r7r3列之外的所有列）
    index_cols = [col for col in r7r3_df.columns if col != 'r7r3']
    
    # 将df和r7r3_df进行合并
    merged_df = df.merge(r7r3_df, on=index_cols, how='left')
    
    # 计算预测的total_revenue_d7
    merged_df['predicted_revenue_d7'] = merged_df['total_revenue_d3'] * merged_df['r7r3']
    
    # 将install_day转换为日期格式，并计算周
    merged_df['install_date'] = pd.to_datetime(merged_df['install_day'], format='%Y%m%d')
    merged_df['week'] = merged_df['install_date'].dt.isocalendar().week
    merged_df['year'] = merged_df['install_date'].dt.year
    merged_df['year_week'] = merged_df['year'].astype(str) + '_W' + merged_df['week'].astype(str).str.zfill(2)
    
    # 关键区别：去掉分组信息（revenue_d3_min, revenue_d3_max）后重新汇总
    # 确定需要去掉的分组列
    group_cols_to_remove = ['revenue_d3_min', 'revenue_d3_max']
    
    # 确定重新汇总的分组列（去掉分组信息列）
    regroup_cols = [col for col in index_cols if col not in group_cols_to_remove]
    
    # 按照新的分组列重新汇总数据
    regrouped_df = merged_df.groupby(regroup_cols + ['install_day']).agg({
        'total_revenue_d7': 'sum',
        'predicted_revenue_d7': 'sum'
    }).reset_index()
    
    # 计算误差（MAPE）
    # 避免除零错误，当total_revenue_d7为0时，设置误差为0
    regrouped_df['error'] = np.where(
        regrouped_df['total_revenue_d7'] > 0,
        np.abs(regrouped_df['total_revenue_d7'] - regrouped_df['predicted_revenue_d7']) / regrouped_df['total_revenue_d7'],
        0
    )
    
    # 将install_day转换为日期格式，并计算周（重新汇总后需要重新计算）
    regrouped_df['install_date'] = pd.to_datetime(regrouped_df['install_day'], format='%Y%m%d')
    regrouped_df['week'] = regrouped_df['install_date'].dt.isocalendar().week
    regrouped_df['year'] = regrouped_df['install_date'].dt.year
    regrouped_df['year_week'] = regrouped_df['year'].astype(str) + '_W' + regrouped_df['week'].astype(str).str.zfill(2)
    
    # 按照重新分组的列分组，计算每组的平均误差（MAPE）
    grouped_results = []
    
    for group_values, group_data in regrouped_df.groupby(regroup_cols):
        # 计算该组的平均绝对百分比误差
        mape = group_data['error'].mean()
        
        # 按周汇总数据，计算按周的误差
        weekly_cols = regroup_cols + ['year_week']
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
        if len(regroup_cols) == 1:
            result_row[regroup_cols[0]] = group_values
        else:
            for i, col in enumerate(regroup_cols):
                result_row[col] = group_values[i]
        result_row['mape'] = mape
        result_row['weekly_mape'] = weekly_mape
        
        grouped_results.append(result_row)
    
    # 转换为DataFrame
    result_df = pd.DataFrame(grouped_results)
    
    return result_df

# 用均值效果一般，所以决定用线性拟合
# 还是效仿r7r3思路，先分组获得r3与r7，然后进行线性拟合，获得一个r7r3
def r7r3LinearRegression(df):
    from sklearn.linear_model import LinearRegression
    
    # 将参数中的df中的，列：users_count，total_revenue_d1 进行舍弃
    # 将除 total_revenue_d3、total_revenue_d7 的列作为 groupby 的依据
    # groupby之后，对每组使用线性回归拟合r3与r7的关系
    # 最终输出列：groupby的依据列（排除掉install_day），r7r3（斜率）
    
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
    
    # 按照groupby列进行分组，并对每组进行线性回归
    grouped_results = []
    
    for group_values, group_data in result_df.groupby(groupby_cols):
        # 过滤掉r3为0的数据点，避免影响回归
        valid_data = group_data[group_data['total_revenue_d3'] > 0].copy()
        
        if len(valid_data) >= 2:  # 至少需要2个数据点进行线性回归
            # 准备回归数据
            X = valid_data[['total_revenue_d3']].values  # 自变量：3日收入
            y = valid_data['total_revenue_d7'].values    # 因变量：7日收入
            
            # 创建线性回归模型
            model = LinearRegression()
            model.fit(X, y)
            
            # 获取斜率作为r7r3比值
            slope = model.coef_[0]
            
            # 如果斜率为负数或异常大，使用备用方法（简单比值的均值）
            if slope <= 0 or slope > 10:  # 设置合理的上限
                ratio_data = valid_data['total_revenue_d7'] / valid_data['total_revenue_d3']
                slope = ratio_data.mean()
        else:
            # 数据点不足，使用简单比值的均值
            if len(valid_data) > 0:
                ratio_data = valid_data['total_revenue_d7'] / valid_data['total_revenue_d3']
                slope = ratio_data.mean()
            else:
                slope = 0.0
        
        # 构建结果行
        result_row = {}
        if len(groupby_cols) == 1:
            result_row[groupby_cols[0]] = group_values
        else:
            for i, col in enumerate(groupby_cols):
                result_row[col] = group_values[i]
        result_row['r7r3'] = slope
        
        grouped_results.append(result_row)
    
    # 转换为DataFrame
    final_result = pd.DataFrame(grouped_results)
    
    return final_result


# 线性回归版本 - 包含斜率和截距
def linearRegressionFit(df):
    from sklearn.linear_model import LinearRegression
    
    # 将参数中的df中的，列：users_count，total_revenue_d1 进行舍弃
    # 将除 total_revenue_d3、total_revenue_d7 的列作为 groupby 的依据
    # groupby之后，对每组使用线性回归拟合r3与r7的关系
    # 最终输出列：groupby的依据列（排除掉install_day），slope（斜率），intercept（截距）
    
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
    
    # 按照groupby列进行分组，并对每组进行线性回归
    grouped_results = []
    
    for group_values, group_data in result_df.groupby(groupby_cols):
        # 过滤掉r3为0的数据点，避免影响回归
        valid_data = group_data[group_data['total_revenue_d3'] > 0].copy()
        
        if len(valid_data) >= 2:  # 至少需要2个数据点进行线性回归
            # 准备回归数据
            X = valid_data[['total_revenue_d3']].values  # 自变量：3日收入
            y = valid_data['total_revenue_d7'].values    # 因变量：7日收入
            
            # 创建线性回归模型
            model = LinearRegression()
            model.fit(X, y)
            
            # 获取斜率和截距
            slope = model.coef_[0]
            intercept = model.intercept_
            
            # 如果斜率为负数或异常大，使用备用方法
            if slope <= 0 or slope > 10:
                ratio_data = valid_data['total_revenue_d7'] / valid_data['total_revenue_d3']
                slope = ratio_data.mean()
                intercept = 0.0
        else:
            # 数据点不足，使用简单比值的均值
            if len(valid_data) > 0:
                ratio_data = valid_data['total_revenue_d7'] / valid_data['total_revenue_d3']
                slope = ratio_data.mean()
                intercept = 0.0
            else:
                slope = 0.0
                intercept = 0.0
        
        # 构建结果行
        result_row = {}
        if len(groupby_cols) == 1:
            result_row[groupby_cols[0]] = group_values
        else:
            for i, col in enumerate(groupby_cols):
                result_row[col] = group_values[i]
        result_row['slope'] = slope
        result_row['intercept'] = intercept
        
        grouped_results.append(result_row)
    
    # 转换为DataFrame
    final_result = pd.DataFrame(grouped_results)
    
    return final_result


# 使用线性回归模型进行预测并计算误差
def predictWithLinearRegression(df, lr_df):
    # 确定索引列（lr_df中除了slope和intercept列之外的所有列）
    index_cols = [col for col in lr_df.columns if col not in ['slope', 'intercept']]
    
    # 将df和lr_df进行合并
    merged_df = df.merge(lr_df, on=index_cols, how='left')
    
    # 使用线性回归公式计算预测的total_revenue_d7
    # predicted_r7 = slope * r3 + intercept
    merged_df['predicted_revenue_d7'] = merged_df['total_revenue_d3'] * merged_df['slope'] + merged_df['intercept']
    
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
    
    # 按照索引列分组，计算每组的平均误差（MAPE）
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
        
        grouped_results.append(result_row)
    
    # 转换为DataFrame
    result_df = pd.DataFrame(grouped_results)
    
    return result_df


def mainRawLR():
    rawDf0, rawDf1, rawDf2 = getRawData()
    
    print("=== 线性回归拟合测试（包含斜率和截距） ===")
    
    # 线性回归拟合
    print("rawDf0线性回归拟合:")
    lrResult0 = linearRegressionFit(rawDf0)
    print(lrResult0)
    print()
    
    print("rawDf1线性回归拟合:")
    lrResult1 = linearRegressionFit(rawDf1)
    print(lrResult1.head(10))
    print()
    
    print("rawDf2线性回归拟合:")
    lrResult2 = linearRegressionFit(rawDf2)
    print(lrResult2.head(10))
    print()
    
    # 使用线性回归进行预测并计算误差
    print("=== 线性回归预测误差计算 ===")
    
    print("rawDf0线性回归预测误差:")
    lrError0 = predictWithLinearRegression(rawDf0, lrResult0)
    print(lrError0)
    print()
    
    print("rawDf1线性回归预测误差:")
    lrError1 = predictWithLinearRegression(rawDf1, lrResult1)
    print(lrError1.head(10))
    print()
    
    print("rawDf2线性回归预测误差:")
    lrError2 = predictWithLinearRegression(rawDf2, lrResult2)
    print(lrError2.head(10))
    print()
    
    # 保存线性回归结果到CSV
    print("保存线性回归误差结果到CSV文件:")
    
    lrError0_sorted = lrError0.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    lrError0_sorted.to_csv('/src/data/20250820_errorLRFull0.csv', index=False)
    print("线性回归误差0已保存到: /src/data/20250820_errorLRFull0.csv")
    
    lrError1_sorted = lrError1.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    lrError1_sorted.to_csv('/src/data/20250820_errorLRFull1.csv', index=False)
    print("线性回归误差1已保存到: /src/data/20250820_errorLRFull1.csv")
    
    lrError2_sorted = lrError2.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    lrError2_sorted.to_csv('/src/data/20250820_errorLRFull2.csv', index=False)
    print("线性回归误差2已保存到: /src/data/20250820_errorLRFull2.csv")


def mainRaw():
    rawDf0, rawDf1, rawDf2 = getRawData()
    
    # 测试r7r3函数
    print("测试rawDf0的r7r3比值:")
    result0 = r7r3(rawDf0)
    print(result0)
    print()
    
    print("测试rawDf1的r7r3比值:")
    result1 = r7r3(rawDf1)
    print(result1.head(10))
    print()
    
    print("测试rawDf2的r7r3比值:")
    result2 = r7r3(rawDf2)
    print(result2.head(10))
    print()
    
    # 测试r7r3Avg函数
    print("测试r7r3Avg函数:")
    print("rawDf0的r7r3Avg比值:")
    resultAvg0 = r7r3Avg(rawDf0)
    print(resultAvg0)
    print()
    
    print("rawDf1的r7r3Avg比值:")
    resultAvg1 = r7r3Avg(rawDf1)
    print(resultAvg1.head(10))
    print()
    
    print("rawDf2的r7r3Avg比值:")
    resultAvg2 = r7r3Avg(rawDf2)
    print(resultAvg2.head(10))
    print()
    
    # 测试r7r3LinearRegression函数
    print("测试r7r3LinearRegression函数:")
    print("rawDf0的r7r3LinearRegression比值:")
    resultLR0 = r7r3LinearRegression(rawDf0)
    print(resultLR0)
    print()
    
    print("rawDf1的r7r3LinearRegression比值:")
    resultLR1 = r7r3LinearRegression(rawDf1)
    print(resultLR1.head(10))
    print()
    
    print("rawDf2的r7r3LinearRegression比值:")
    resultLR2 = r7r3LinearRegression(rawDf2)
    print(resultLR2.head(10))
    print()
    
    # 测试predictAndCalculateError函数 - 使用r7r3结果
    print("测试predictAndCalculateError函数 - 使用r7r3结果:")
    print("rawDf0预测误差:")
    error0 = predictAndCalculateError(rawDf0, result0)
    print(error0)
    print()
    
    print("rawDf1预测误差:")
    error1 = predictAndCalculateError(rawDf1, result1)
    print(error1.head(10))
    print()
    
    print("rawDf2预测误差:")
    error2 = predictAndCalculateError(rawDf2, result2)
    print(error2.head(10))
    print()
    
    # 测试predictAndCalculateError函数 - 使用r7r3Avg结果
    print("测试predictAndCalculateError函数 - 使用r7r3Avg结果:")
    print("rawDf0预测误差(Avg):")
    errorAvg0 = predictAndCalculateError(rawDf0, resultAvg0)
    print(errorAvg0)
    print()
    
    print("rawDf1预测误差(Avg):")
    errorAvg1 = predictAndCalculateError(rawDf1, resultAvg1)
    print(errorAvg1.head(10))
    print()
    
    print("rawDf2预测误差(Avg):")
    errorAvg2 = predictAndCalculateError(rawDf2, resultAvg2)
    print(errorAvg2.head(10))
    print()
    
    # 测试predictAndCalculateError函数 - 使用r7r3LinearRegression结果
    print("测试predictAndCalculateError函数 - 使用r7r3LinearRegression结果:")
    print("rawDf0预测误差(LR):")
    errorLR0 = predictAndCalculateError(rawDf0, resultLR0)
    print(errorLR0)
    print()
    
    print("rawDf1预测误差(LR):")
    errorLR1 = predictAndCalculateError(rawDf1, resultLR1)
    print(errorLR1.head(10))
    print()
    
    print("rawDf2预测误差(LR):")
    errorLR2 = predictAndCalculateError(rawDf2, resultLR2)
    print(errorLR2.head(10))
    print()
    
    # 按周误差降序排列并保存CSV
    print("保存误差结果到CSV文件:")
    
    # r7r3方法的误差结果
    error0_sorted = error0.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    error0_sorted.to_csv('/src/data/20250820_error0.csv', index=False)
    print("error0已保存到: /src/data/20250820_error0.csv")
    
    error1_sorted = error1.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    error1_sorted.to_csv('/src/data/20250820_error1.csv', index=False)
    print("error1已保存到: /src/data/20250820_error1.csv")
    
    error2_sorted = error2.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    error2_sorted.to_csv('/src/data/20250820_error2.csv', index=False)
    print("error2已保存到: /src/data/20250820_error2.csv")
    
    # r7r3Avg方法的误差结果
    errorAvg0_sorted = errorAvg0.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorAvg0_sorted.to_csv('/src/data/20250820_errorAvg0.csv', index=False)
    print("errorAvg0已保存到: /src/data/20250820_errorAvg0.csv")
    
    errorAvg1_sorted = errorAvg1.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorAvg1_sorted.to_csv('/src/data/20250820_errorAvg1.csv', index=False)
    print("errorAvg1已保存到: /src/data/20250820_errorAvg1.csv")
    
    errorAvg2_sorted = errorAvg2.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorAvg2_sorted.to_csv('/src/data/20250820_errorAvg2.csv', index=False)
    print("errorAvg2已保存到: /src/data/20250820_errorAvg2.csv")
    
    # r7r3LinearRegression方法的误差结果
    errorLR0_sorted = errorLR0.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorLR0_sorted.to_csv('/src/data/20250820_errorLR0.csv', index=False)
    print("errorLR0已保存到: /src/data/20250820_errorLR0.csv")
    
    errorLR1_sorted = errorLR1.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorLR1_sorted.to_csv('/src/data/20250820_errorLR1.csv', index=False)
    print("errorLR1已保存到: /src/data/20250820_errorLR1.csv")
    
    errorLR2_sorted = errorLR2.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorLR2_sorted.to_csv('/src/data/20250820_errorLR2.csv', index=False)
    print("errorLR2已保存到: /src/data/20250820_errorLR2.csv")

def mainGroup():
    groupDf0,groupDf1,groupDf2 = getGroupData()

    print("测试groupDf0的r7r3比值:")
    result0 = r7r3(groupDf0)
    print(result0)
    print()
    
    print("测试groupDf1的r7r3比值:")
    result1 = r7r3(groupDf1)
    print(result1.head(10))
    print()
    
    print("测试groupDf2的r7r3比值:")
    result2 = r7r3(groupDf2)
    print(result2.head(10))
    print()
    
    # 测试r7r3Avg函数
    print("测试r7r3Avg函数:")
    print("groupDf0的r7r3Avg比值:")
    resultAvg0 = r7r3Avg(groupDf0)
    print(resultAvg0)
    print()
    
    print("groupDf1的r7r3Avg比值:")
    resultAvg1 = r7r3Avg(groupDf1)
    print(resultAvg1.head(10))
    print()
    
    print("groupDf2的r7r3Avg比值:")
    resultAvg2 = r7r3Avg(groupDf2)
    print(resultAvg2.head(10))
    print()
    
    # 测试predictAndCalculateErrorGroup函数 - 使用r7r3结果
    print("测试predictAndCalculateErrorGroup函数 - 使用r7r3结果:")
    print("groupDf0预测误差(Group):")
    errorGroup0 = predictAndCalculateErrorGroup(groupDf0, result0)
    print(errorGroup0)
    print()
    
    print("groupDf1预测误差(Group):")
    errorGroup1 = predictAndCalculateErrorGroup(groupDf1, result1)
    print(errorGroup1.head(10))
    print()
    
    print("groupDf2预测误差(Group):")
    errorGroup2 = predictAndCalculateErrorGroup(groupDf2, result2)
    print(errorGroup2.head(10))
    print()
    
    # 测试predictAndCalculateErrorGroup函数 - 使用r7r3Avg结果
    print("测试predictAndCalculateErrorGroup函数 - 使用r7r3Avg结果:")
    print("groupDf0预测误差(GroupAvg):")
    errorGroupAvg0 = predictAndCalculateErrorGroup(groupDf0, resultAvg0)
    print(errorGroupAvg0)
    print()
    
    print("groupDf1预测误差(GroupAvg):")
    errorGroupAvg1 = predictAndCalculateErrorGroup(groupDf1, resultAvg1)
    print(errorGroupAvg1.head(10))
    print()
    
    print("groupDf2预测误差(GroupAvg):")
    errorGroupAvg2 = predictAndCalculateErrorGroup(groupDf2, resultAvg2)
    print(errorGroupAvg2.head(10))
    print()
    
    # 保存Group误差结果到CSV
    print("保存Group误差结果到CSV文件:")
    
    # r7r3方法的Group误差结果
    errorGroup0_sorted = errorGroup0.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorGroup0_sorted.to_csv('/src/data/20250820_errorGroup0.csv', index=False)
    print("errorGroup0已保存到: /src/data/20250820_errorGroup0.csv")
    
    errorGroup1_sorted = errorGroup1.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorGroup1_sorted.to_csv('/src/data/20250820_errorGroup1.csv', index=False)
    print("errorGroup1已保存到: /src/data/20250820_errorGroup1.csv")
    
    errorGroup2_sorted = errorGroup2.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorGroup2_sorted.to_csv('/src/data/20250820_errorGroup2.csv', index=False)
    print("errorGroup2已保存到: /src/data/20250820_errorGroup2.csv")
    
    # r7r3Avg方法的Group误差结果
    errorGroupAvg0_sorted = errorGroupAvg0.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorGroupAvg0_sorted.to_csv('/src/data/20250820_errorGroupAvg0.csv', index=False)
    print("errorGroupAvg0已保存到: /src/data/20250820_errorGroupAvg0.csv")
    
    errorGroupAvg1_sorted = errorGroupAvg1.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorGroupAvg1_sorted.to_csv('/src/data/20250820_errorGroupAvg1.csv', index=False)
    print("errorGroupAvg1已保存到: /src/data/20250820_errorGroupAvg1.csv")
    
    errorGroupAvg2_sorted = errorGroupAvg2.sort_values('weekly_mape', ascending=False).reset_index(drop=True)
    errorGroupAvg2_sorted.to_csv('/src/data/20250820_errorGroupAvg2.csv', index=False)
    print("errorGroupAvg2已保存到: /src/data/20250820_errorGroupAvg2.csv")


if __name__ == "__main__":
    # mainRaw()
    # mainGroup()
    mainRawLR()

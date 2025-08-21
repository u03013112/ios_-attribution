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
    pass

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


if __name__ == "__main__":
    mainRaw()

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

# 预测数据，并计算误差
def predictAndCalculateError(df,r7r3_df):
    # 将r7r3_df 中，除了r7r3列之外的列，作为df的索引
    # 用索引将 df 和 r7r3_df 进行合并
    # df中的total_revenue_d3 * 对应的r7r3值，作为预测的total_revenue_d7
    # 计算误差 = (total_revenue_d7 - 预测的total_revenue_d7) / total_revenue_d7
    # 最后按照索引分组，计算每组的平均误差（MAPE）
    pass



def main():
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


if __name__ == "__main__":
    main()

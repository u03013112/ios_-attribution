# 元忠 20250611需求
from unittest import result
import pandas as pd
import os
from scipy.stats import pearsonr, spearmanr


def getDataStep1():
    filename = '/src/data/a_processed.csv'
    if os.path.exists(filename):
        print('File exists, reading data from', filename)
        df = pd.read_csv(filename)
    else:
        df = pd.read_csv('a.csv')
        df = df[[
            'material_md5',
            'os',
            'mediasource',
            'inventory',
            'cost_value_usd',
            'click',
            'impressions',
            'install',
            'ctr',
            'cvr',
            'cpm',
            'result',
        ]]

        df['ctr'] = df['click'] / df['impressions']
        
        # df['cvr'] = df['install'] / df['click']
        # 如果click为0，则cvr为0，否则计算cvr
        df['cvr'] = df.apply(lambda row: row['install'] / row['click'] if row['click'] > 0 else 0, axis=1)

        df['cpm'] = df['cost_value_usd'] / df['impressions'] * 1000

        # print(df.head())

        # 拆分result，result 是类似"Overall: 62.694 Color: 61.682 Noise: 56.338 Artifact: 58.803 Blur: 60.137 Temporal: 62.39" 的字符串
        # 将其拆分为多个列 Overall, Color, Noise, Artifact, Blur, Temporal
        def split_result(result):
            # Remove spaces around the colon
            result_cleaned = result.replace(' :', ':').replace(': ', ':')
            # Split the cleaned result string by spaces
            parts = result_cleaned.split(' ')
            
            result_dict = {}
            for part in parts:
                if ':' in part:
                    key, value = part.split(':')
                    result_dict[key] = float(value)
            return pd.Series(result_dict)

        df_result = df['result'].astype(str).apply(split_result)
        # print(df_result.head())

        # 将拆分后的结果与原始 DataFrame 合并
        df = pd.concat([df, df_result], axis=1)
        # 删除原始的 result 列
        df.drop(columns=['result'], inplace=True)
        # 保存处理后的 DataFrame
        df.to_csv(filename, index=False)

    return df

def getDataStep2(getDataStep1Return):
    filename = '/src/data/b_processed.csv'
    if os.path.exists(filename):
        print('File exists, reading data from', filename)
        df = pd.read_csv(filename)
    else:
        df = pd.read_csv('b.csv')
        df = df.rename(columns={
            'md5':'material_md5',
            '一级标签':'tag_level_1',
            '二级标签':'tag_level_2'
        })

        df = pd.merge(df,getDataStep1Return,on=['material_md5'],how = 'right')

        df.to_csv(filename, index=False)

    return df

# 最小花费过滤，将花费过少的视频排除掉
def getDataStep3(getDataStep2Return,minCost=10000):
    # 整合一下标签
    # 叫做tag_type,按照tag_level_2分组
    # '营销类' -> '代言人';'测试类' -> '玩法类'; 其他 -> '其他'
    getDataStep2Return['tag_type'] = getDataStep2Return['tag_level_2'].apply(
        lambda x: '代言人' if x == '营销类' else ('玩法类' if x == '测试类' else '其他')
    )

    df = getDataStep2Return[getDataStep2Return['cost_value_usd'] >= minCost].copy()
    print(f'最小花费：{minCost}过滤后剩余：{len(df)},原有占比{(len(df)/len(getDataStep2Return)):.2%}')
    return df

def getData(minCost):
    df = getDataStep1()
    df2 = getDataStep2(df)
    # 找到df2中没有tag_level_1或tag_level_2的行
    missing_tags_df = df2[df2['tag_level_1'].isnull() | df2['tag_level_2'].isnull()]
    # 打印这些行的前10行
    print(missing_tags_df.head(10))
    # 并计算这些行的行数占比
    missing_ratio = len(missing_tags_df) / len(df2)
    print(f"Missing tags ratio: {missing_ratio:.2%}")

    df3 = getDataStep3(df2,minCost=minCost)
    return df3


import pandas as pd
from scipy.stats import pearsonr, spearmanr

def getCorr():
    df = getData(0)
    
    # 定义花费范围
    spend_ranges = [100000, 10000, 3000, 1000]
    
    # 定义要计算相关性的列
    score_columns = ['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']
    index_columns = ['ctr', 'cvr', 'cpm']
    
    # 存储所有结果的列表
    all_results = []
    
    # 分组并计算相关性
    for spend_range in spend_ranges:
        filtered_df = df[df['cost_value_usd'] >= spend_range]
        for group_values, group_df in filtered_df.groupby(['mediasource', 'os', 'inventory']):
            mediasource, os, inventory = group_values
            
            # 初始化结果字典
            result_dict_t = {
                'Spend Range': spend_range,
                'Mediasource': mediasource,
                'Os': os,
                'Inventory': inventory
            }
            
            # 计算每个 index_column 与 score_columns 的相关性
            for index_col in index_columns:
                result_dict = result_dict_t.copy()
                result_dict[f'Index'] = index_col
                result_dict_corr = result_dict.copy()
                result_dict_corr[f'corrOrP'] = 'Correlation'
                result_dict_p = result_dict.copy()
                result_dict_p[f'corrOrP'] = 'P Value'

                for score_col in score_columns:
                    # 检查数据有效性
                    valid_data = group_df[[index_col, score_col]].dropna()
                    x = valid_data[index_col]
                    y = valid_data[score_col]
                    
                    if len(x) > 1 and len(y) > 1 and not (x.nunique() == 1 or y.nunique() == 1):
                        # 确保数据不含 NaN/Inf 且不是常数
                        pearson_corr, pearson_p = pearsonr(x, y)
                        spearman_corr, spearman_p = spearmanr(x, y)
                    else:
                        pearson_corr, pearson_p = float('nan'), float('nan')
                        spearman_corr, spearman_p = float('nan'), float('nan')
                    
                    # 整理结果
                    result_dict_corr[f'{score_col}_pearson'] = pearson_corr
                    result_dict_corr[f'{score_col}_spearman'] = spearman_corr
                    
                    
                    
                    result_dict_p[f'{score_col}_pearson'] = pearson_p
                    result_dict_p[f'{score_col}_spearman'] = spearman_p
                    
                    
            
                result_corr_df = pd.DataFrame([result_dict_corr])
                result_p_df = pd.DataFrame([result_dict_p])

                all_results.append(result_corr_df)
                all_results.append(result_p_df)
    
    # 使用 pd.concat 将所有结果 DataFrame 合并为一个
    final_results = pd.concat(all_results, ignore_index=True)
    
    # 保存结果到 CSV 文件
    final_results.to_csv('/src/data/correlation_results.csv', index=False)

# 添加食品标签
def getCorr2():
    df = getData(0)
    
    # 定义花费范围
    spend_ranges = [100000, 10000, 3000, 1000]
    
    # 定义要计算相关性的列
    score_columns = ['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']
    index_columns = ['ctr', 'cvr', 'cpm']
    
    tag_type = df['tag_type'].unique().tolist()

    # 存储所有结果的列表
    all_results = []
    
    # 分组并计算相关性
    for spend_range in spend_ranges:
        filtered_df = df[df['cost_value_usd'] >= spend_range]
        for group_values, group_df in filtered_df.groupby(['mediasource', 'os', 'inventory']):
            mediasource, os, inventory = group_values
            
            for tag in tag_type:
                df0 = group_df[group_df['tag_type'] == tag]

                # 初始化结果字典
                result_dict_t = {
                    'Spend Range': spend_range,
                    'Mediasource': mediasource,
                    'Os': os,
                    'Inventory': inventory,
                    'Tag Type': tag,
                    'Video Count': len(df0)
                }
                
                # 计算每个 index_column 与 score_columns 的相关性
                for index_col in index_columns:
                    result_dict = result_dict_t.copy()
                    result_dict[f'Index'] = index_col
                    result_dict_corr = result_dict.copy()
                    result_dict_corr[f'corrOrP'] = 'Correlation'
                    result_dict_p = result_dict.copy()
                    result_dict_p[f'corrOrP'] = 'P Value'

                    for score_col in score_columns:
                        # 检查数据有效性
                        valid_data = df0[[index_col, score_col]].dropna()
                        x = valid_data[index_col]
                        y = valid_data[score_col]
                        
                        if len(x) > 1 and len(y) > 1 and not (x.nunique() == 1 or y.nunique() == 1):
                            # 确保数据不含 NaN/Inf 且不是常数
                            pearson_corr, pearson_p = pearsonr(x, y)
                            spearman_corr, spearman_p = spearmanr(x, y)
                        else:
                            pearson_corr, pearson_p = float('nan'), float('nan')
                            spearman_corr, spearman_p = float('nan'), float('nan')
                        
                        # 整理结果
                        result_dict_corr[f'{score_col}_pearson'] = pearson_corr
                        result_dict_corr[f'{score_col}_spearman'] = spearman_corr
                        
                        
                        
                        result_dict_p[f'{score_col}_pearson'] = pearson_p
                        result_dict_p[f'{score_col}_spearman'] = spearman_p
                        
                        
                
                    result_corr_df = pd.DataFrame([result_dict_corr])
                    result_p_df = pd.DataFrame([result_dict_p])

                    all_results.append(result_corr_df)
                    all_results.append(result_p_df)
    
    # 使用 pd.concat 将所有结果 DataFrame 合并为一个
    final_results = pd.concat(all_results, ignore_index=True)

    # 简单易读处理
    # 将 Spend Range 列转换为字符串格式，并将100000转换为 '100K+', 10000转换为 '10K+', 3000转换为 '3K+', 1000转换为 '1K+'
    final_results['Spend Range'] = final_results['Spend Range'].replace({
        100000: '100K+',
        10000: '10K+',
        3000: '3K+',
        1000: '1K+'
    })

    # 保存结果到 CSV 文件
    final_results.to_csv('/src/data/correlation_results2.csv', index=False)


if __name__ == '__main__':
    # getCorr()
    getCorr2()
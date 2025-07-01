# 元忠 20250630 需求
# 提供了一套旧的评分，vqa_score(1).csv，完成之前的分析
from unittest import result
import pandas as pd
import os
from scipy.stats import pearsonr, spearmanr

def getDataStep1():
    df = pd.read_csv('vqa_score(1).csv')
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
        'vqa_score',
    ]]
    df['ctr'] = df['click'] / df['impressions']
    df['cvr'] = df.apply(lambda row: row['install'] / row['click'] if row['click'] > 0 else 0, axis=1)
    df['cpm'] = df['cost_value_usd'] / df['impressions'] * 1000

    return df

def getDataStep2(getDataStep1Return):
    
    df = pd.read_csv('b.csv')
    df = df.rename(columns={
        'md5':'material_md5',
        '一级标签':'tag_level_1',
        '二级标签':'tag_level_2'
    })

    df = pd.merge(df,getDataStep1Return,on=['material_md5'],how = 'right')

    return df

# 整合一下标签
def getDataStep3(getDataStep2Return):
    # 叫做tag_type,按照tag_level_2分组
    # '营销类' -> '代言人';'测试类' -> '玩法类'; 其他 -> '其他'
    getDataStep2Return['tag_type'] = getDataStep2Return['tag_level_2'].apply(
        lambda x: '代言人' if x == '营销类' else ('玩法类' if x == '测试类' else '其他')
    )

    return getDataStep2Return


def main():
    # 数据加载与预处理
    df = getDataStep1()
    df2 = getDataStep2(df)
    
    # 检查标签缺失情况
    missing_tags_df = df2[df2['tag_level_1'].isnull() | df2['tag_level_2'].isnull()]
    missing_ratio = len(missing_tags_df) / len(df2)
    print(f"Missing tags ratio: {missing_ratio:.2%}")
    
    # 标签分类整合
    df = getDataStep3(df2)
    print(df.head())

    # 保存预处理后的数据
    df.to_csv('/src/data/vqa_score_processed.csv', index=False)

    # 定义花费范围
    spend_ranges = [100000, 10000, 3000, 1000]
    
    # 定义要计算相关性的列
    score_columns = ['vqa_score']
    index_columns = ['ctr', 'cvr', 'cpm']

    # 存储所有结果的列表
    all_results = []
    
    # 分组并计算相关性
    for spend_range in spend_ranges:
        filtered_df = df[df['cost_value_usd'] >= spend_range]
        
        # 直接将tag_type合并到groupby中，避免二次筛选
        grouped = filtered_df.groupby(['mediasource', 'os', 'inventory', 'tag_type'])
        
        for group_values, df0 in grouped:
            mediasource, os, inventory, tag = group_values

            # 初始化结果字典
            result_dict_base = {
                'Spend Range': spend_range,
                'Mediasource': mediasource,
                'Os': os,
                'Inventory': inventory,
                'Tag Type': tag,
                'Video Count': len(df0)
            }
            
            # 计算每个指标与评分的相关性
            for index_col in index_columns:
                result_dict_corr = result_dict_base.copy()
                result_dict_corr['Index'] = index_col
                result_dict_corr['corrOrP'] = 'Correlation'

                result_dict_p = result_dict_base.copy()
                result_dict_p['Index'] = index_col
                result_dict_p['corrOrP'] = 'P Value'

                for score_col in score_columns:
                    # 检查数据有效性
                    valid_data = df0[[index_col, score_col]].dropna()
                    x = valid_data[index_col]
                    y = valid_data[score_col]
                    
                    if len(x) > 1 and len(y) > 1 and not (x.nunique() == 1 or y.nunique() == 1):
                        # 数据有效，计算相关性
                        pearson_corr, pearson_p = pearsonr(x, y)
                        spearman_corr, spearman_p = spearmanr(x, y)
                    else:
                        # 数据无效或不足，设为NaN
                        pearson_corr, pearson_p = float('nan'), float('nan')
                        spearman_corr, spearman_p = float('nan'), float('nan')

                    # 存储相关性系数
                    result_dict_corr[f'{score_col}_pearson'] = pearson_corr
                    result_dict_corr[f'{score_col}_spearman'] = spearman_corr
                    
                    # 存储p值
                    result_dict_p[f'{score_col}_pearson'] = pearson_p
                    result_dict_p[f'{score_col}_spearman'] = spearman_p
                
                # 将结果加入到列表中
                all_results.append(pd.DataFrame([result_dict_corr]))
                all_results.append(pd.DataFrame([result_dict_p]))
    
    # 合并所有结果
    final_results = pd.concat(all_results, ignore_index=True)

    # 调整花费范围显示格式
    final_results['Spend Range'] = final_results['Spend Range'].replace({
        100000: '100K+',
        10000: '10K+',
        3000: '3K+',
        1000: '1K+'
    })

    # 过滤掉没有视频的数据
    final_results = final_results[final_results['Video Count'] > 0]

    # 保存结果到CSV文件
    final_results.to_csv('/src/data/correlation_results2.csv', index=False)
    print("Correlation results saved to /src/data/correlation_results2.csv")


def debug():
    df = pd.read_csv('/src/data/vqa_score_processed.csv')

    # 只看100K+	Facebook	IOS	Facebook	玩法类 ，ctr 和 vqa_score 的相关性
    filtered_df = df[
        (df['cost_value_usd'] >= 100000 ) &
        (df['mediasource'] == 'Facebook') &
        (df['os'] == 'IOS') &
        (df['inventory'] == 'Instagram') &
        (df['tag_type'] == '玩法类')
    ]

    print(len(filtered_df))

    print('CTR 和 VQA Score 的相关性：')
    # valid_data = filtered_df[['ctr', 'vqa_score']].dropna()
    valid_data = filtered_df[['ctr', 'vqa_score']]
    print(valid_data.corr(method='pearson'))
    print(valid_data.corr(method='spearman'))

if __name__ == '__main__':
    main()
    # debug()
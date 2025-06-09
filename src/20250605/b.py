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

def corr(minCost):
    df = getData(minCost)
    totalDf = df[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm','Overall','Color','Noise','Artifact','Blur','Temporal']]

    
    # 计算'Overall','Color','Noise','Artifact','Blur','Temporal' vs 'cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm' 的相关性
    correlation = totalDf.corr(method='pearson')[['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].loc[
        ['ctr', 'cvr', 'cpm']
    ]

    print("Correlation matrix:")
    print(correlation)

    groupDf = df.groupby(['tag_level_1','tag_level_2'])
    for name, group in groupDf:
        print(f"\nGroup: {name}")
        group = group[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm', 'Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']]
        group_correlation = group.corr(method='pearson')[['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].loc[
            ['ctr', 'cvr', 'cpm']
        ]
        print("Correlation matrix for group:")
        print(group_correlation)


    groupDf = df.groupby(['os', 'mediasource', 'inventory','tag_level_1','tag_level_2'])
    for name, group in groupDf:
        print(f"\nGroup: {name}")
        group = group[['cost_value_usd', 'click', 'impressions', 'install', 'ctr', 'cvr', 'cpm', 'Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']]
        group_correlation = group.corr(method='pearson')[['Overall', 'Color', 'Noise', 'Artifact', 'Blur', 'Temporal']].loc[
            ['ctr', 'cvr', 'cpm']
        ]
        print("Correlation matrix for group:")
        print(group_correlation)

if __name__ == "__main__":
    corr(1000)
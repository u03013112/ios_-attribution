# 用户活跃度调查 & 预测

# 1. 用户活跃度重要程度占比
# 先认为目前的战斗力统计
# 然后按照用户在队伍中的战斗力占比作为 期望贡献占比
# 再统计实际在比赛中的得分占比，作为实际贡献占比
# 找到主要贡献差异
# 比如：活跃度为0的用户数占比，战斗力占比
# 要再按照目前队伍匹配分

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import os
import re

def getData():
    filename = '沙漠风暴 匹配 详细数据 20250117_20250117.csv'
    # filename = '沙漠风暴 匹配 详细数据 20250117_20250117_for_test.csv'
    df = pd.read_csv(filename)

    # 将 'wk' 列转换为日期格式
    df['wk'] = pd.to_datetime(df['wk'])

    def parse_and_sort_strengthinfo_column(df, column_name):
        # 初始化存储解析后数据的列表
        parsed_data = []

        # 解析所有行的 JSON 数据
        all_data = df[column_name].apply(lambda x: json.loads(x) if pd.notna(x) else {})

        for data in all_data:
            row_data = {}
            sorted_data = sorted(data.items(), key=lambda item: float(item[1].split('|')[0]), reverse=True)
            for i in range(1, 31):
                if i <= len(sorted_data):
                    uid, info = sorted_data[i-1]
                    parts = info.split('|')
                    match_score = parts[0]
                    attributes = parts[1].split(';')
                    live_rate = parts[2]
                    live_rate2 = parts[3] if len(parts) > 3 else 0
                    row_data[f'{column_name}_uid_{i}'] = uid
                    row_data[f'{column_name}_match_score_{i}'] = match_score
                    row_data[f'{column_name}_attribute1_{i}'] = attributes[0] if len(attributes) > 0 else 0
                    row_data[f'{column_name}_attribute2_{i}'] = attributes[1] if len(attributes) > 1 else 0
                    row_data[f'{column_name}_attribute3_{i}'] = attributes[2] if len(attributes) > 2 else 0
                    row_data[f'{column_name}_attribute4_{i}'] = attributes[3] if len(attributes) > 3 else 0
                    row_data[f'{column_name}_live_rate_{i}'] = live_rate
                    row_data[f'{column_name}_live_rate2_{i}'] = live_rate2
                else:
                    row_data[f'{column_name}_uid_{i}'] = 0
                    row_data[f'{column_name}_match_score_{i}'] = 0
                    row_data[f'{column_name}_attribute1_{i}'] = 0
                    row_data[f'{column_name}_attribute2_{i}'] = 0
                    row_data[f'{column_name}_attribute3_{i}'] = 0
                    row_data[f'{column_name}_attribute4_{i}'] = 0
                    row_data[f'{column_name}_live_rate_{i}'] = 0
                    row_data[f'{column_name}_live_rate2_{i}'] = 0
            parsed_data.append(row_data)

        # 将解析后的数据转换为DataFrame
        parsed_df = pd.DataFrame(parsed_data)
        return parsed_df

    # 需要解析的列
    columns_to_parse = ['strengthinfo_a', 'strengthinfo2_a', 'strengthinfo_b', 'strengthinfo2_b']

    for col in columns_to_parse:
        parsed_df = parse_and_sort_strengthinfo_column(df, col)
        df = pd.concat([df, parsed_df], axis=1)

    return df

def prepareData(df, N):
    # 目标变量
    y = df['is_quality']

    # 特征变量，排除指定的列
    columns_to_exclude = [
        'alliance_a_id', 'group_a', 'strengthinfo_a', 'strengthinfo2_a', 'score_a',
        'alliance_b_id', 'group_b', 'strengthinfo_b', 'strengthinfo2_b', 'score_b', 'is_win', 'is_quality',
    ]

    # 只保留前 N 名的数据
    columns_to_include = ['wk', 'strength_a','strength_b','score_a','score_b']
    for col in df.columns:
        if col not in columns_to_exclude:
            match = re.search(r'_(\d+)$', col)
            if match:
                index = int(match.group(1))
                if index <= N:
                    columns_to_include.append(col)

    x = df[columns_to_include]

    return x, y

def getAddScoreData():
    filename = '沙漠风暴 add_score_20250120.csv'
    df = pd.read_csv(filename)

    df.rename(columns={'#account_id':'uid'}, inplace=True)
    df['wk'] = pd.to_datetime(df['wk'])

    return df


def main():
    filename = '/src/data/20250117a2_data_for_test.csv'
    if os.path.exists(filename):
        print('已存在%s' % filename)
        df = pd.read_csv(filename)
    else:
        df = getData()
        df.to_csv(filename, index=False)

    df['wk'] = pd.to_datetime(df['wk'])
    N = 10  # 只使用前 N 名的数据
    x, y = prepareData(df, N)

    # x.to_csv('/src/data/x.csv', index=False)
    columns_to_save = ['wk','strength_a', 'strength_b', 'score_a', 'score_b']
    for i in range(1, N + 1):
        columns_to_save += [f'strengthinfo_a_uid_{i}', f'strengthinfo_a_match_score_{i}']
        columns_to_save += [f'strengthinfo_b_uid_{i}', f'strengthinfo_b_match_score_{i}']

    x = x[columns_to_save]
    # 每一行是一场战斗，添加一列战斗编号
    x['battle_number'] = range(len(x))

    # 拆分
    strengthADf0 = x[['wk','battle_number', 'strength_a', 'score_a']]
    strengthADf1 = x[['wk','battle_number']]
    for i in range(1, N + 1):
        strengthADf1 = pd.concat([strengthADf1, x[[f'strengthinfo_a_uid_{i}', f'strengthinfo_a_match_score_{i}']]], axis=1)
    
    print('before melt')
    print(strengthADf1.head())
    
    # 将uid和match_score分别melt成两张表
    uid_df = pd.melt(strengthADf1, id_vars=['wk','battle_number'], value_vars=[f'strengthinfo_a_uid_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='uid')
    score_df = pd.melt(strengthADf1, id_vars=['wk','battle_number'], value_vars=[f'strengthinfo_a_match_score_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='match_score')
    
    # 提取number
    uid_df['number'] = uid_df['variable'].str.extract(r'_(\d+)$').astype(int)
    score_df['number'] = score_df['variable'].str.extract(r'_(\d+)$').astype(int)
    
    # 删除无用列
    uid_df = uid_df.drop(columns=['variable'])
    score_df = score_df.drop(columns=['variable'])
    
    # 合并
    result_df = pd.merge(uid_df, score_df, on=['wk','battle_number', 'number'])
    
    # print('final result')
    # print(result_df[result_df['battle_number']==0])

    addScoreDf = getAddScoreData()
    addScoreDf = addScoreDf.groupby(['wk','uid']).agg({'add_score_sum':'sum'}).reset_index()
    
    result_df = pd.merge(result_df, addScoreDf, on=['wk','uid'], how='left')
    result_df['add_score_sum'] = result_df['add_score_sum'].fillna(0)

    print('result_df')
    print(result_df[result_df['battle_number']==0])

    # 重新将数据汇总成每个 wk + battle_number 一行：
    # 按照wk + battle_number 分组，计算每个分组的 add_score_sum == 0 的 人数占比，记作 add_score_zero_rate
    # 计算每个分组中 add_score_sum == 0 的 match_score 占比，记作 add_score_zero_match_score_rate
    grouped = result_df.groupby(['wk', 'battle_number'])
    summary_df = grouped.apply(lambda g: pd.Series({
        'add_score_zero_rate': (g['add_score_sum'] == 0).mean(),
        'add_score_zero_match_score_rate': g.loc[g['add_score_sum'] == 0, 'match_score'].sum() / g['match_score'].sum() if g['match_score'].sum() != 0 else 0
    })).reset_index()

    summary_df = summary_df.merge(strengthADf0, on=['wk', 'battle_number'], how='left')
    print('summary result')
    print(summary_df[summary_df['battle_number']==0])



if __name__ == '__main__':
    main()
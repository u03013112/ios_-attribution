# 与activity.py 区别
# 逻辑整理，取出不必要的逻辑

# 用重新计算的个人匹配分替代之前方案中算好的个人匹配分
# 额外的，计算按照新算出来的用户活性，重新计算个人匹配分，然后再按照简单公式计算队伍匹配分。再给队伍匹配分进行评分。

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

import matplotlib.pyplot as plt
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

def getAddScoreData():
    # filename = '沙漠风暴 add_score_20250120.csv'
    filename = '/src/data/add_score_2024-11-25_2025-01-20.csv'
    df = pd.read_csv(filename)

    df.rename(columns={'#account_id':'uid'}, inplace=True)
    df['wk'] = pd.to_datetime(df['wk'])

    return df


def main():
    # filename = '/src/data/20250117a2_data_for_test.csv'
    filename = '/src/data/20250117a2_data.csv'
    if os.path.exists(filename):
        print('已存在%s' % filename)
        df = pd.read_csv(filename)
    else:
        df = getData()
        df.to_csv(filename, index=False)

    df['wk'] = pd.to_datetime(df['wk'])
    N = 30  # 只使用前 N 名的数据
    x = df.copy()

    # x.to_csv('/src/data/x.csv', index=False)
    columns_to_save = ['wk','strength_a', 'strength_b', 'score_a', 'score_b']
    for i in range(1, N + 1):
        columns_to_save += [f'strengthinfo_a_uid_{i}', f'strengthinfo_a_match_score_{i}', f'strengthinfo_a_live_rate_{i}', f'strengthinfo_a_attribute1_{i}',f'strengthinfo_a_attribute2_{i}',f'strengthinfo_a_attribute3_{i}',f'strengthinfo_a_attribute4_{i}']
        columns_to_save += [f'strengthinfo_b_uid_{i}', f'strengthinfo_b_match_score_{i}', f'strengthinfo_b_live_rate_{i}', f'strengthinfo_b_attribute1_{i}',f'strengthinfo_b_attribute2_{i}',f'strengthinfo_b_attribute3_{i}',f'strengthinfo_b_attribute4_{i}']

    x = x[columns_to_save]
    # 每一行是一场战斗，添加一列战斗编号
    x['battle_number'] = range(len(x))
    x[['wk', 'battle_number','strength_a', 'strength_b', 'score_a', 'score_b']].to_csv('/src/data/20250121final_summary2.csv', index=False)
    print('save to /src/data/20250121final_summary2.csv')

    # 拆分 a 队部分
    strengthADf0 = x[['wk', 'battle_number', 'strength_a', 'score_a']]
    strengthADf1 = x[['wk', 'battle_number']]
    for i in range(1, N + 1):
        strengthADf1 = pd.concat([strengthADf1, x[[f'strengthinfo_a_uid_{i}', f'strengthinfo_a_match_score_{i}', f'strengthinfo_a_live_rate_{i}', f'strengthinfo_a_attribute1_{i}',f'strengthinfo_a_attribute2_{i}',f'strengthinfo_a_attribute3_{i}',f'strengthinfo_a_attribute4_{i}']]], axis=1)
    
    # print('before melt (a 队)')
    # print(strengthADf1.head())
    
    # 将 uid 和 match_score 分别 melt 成两张表
    uid_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_uid_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='uid')
    # match_score_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_match_score_{i}' for i in range(1, N + 1)],
    #                 var_name='variable', value_name='match_score_old')
    # live_rate_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_live_rate_{i}' for i in range(1, N + 1)],
    #                 var_name='variable', value_name='live_rate')
    att1_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_attribute1_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='attribute1')
    att2_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_attribute2_{i}' for i in range(1, N + 1)],
                    var_name='variable', value_name='attribute2')
    att3_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_attribute3_{i}' for i in range(1, N + 1)],
                    var_name='variable', value_name='attribute3')
    att4_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_attribute4_{i}' for i in range(1, N + 1)],
                    var_name='variable', value_name='attribute4')
    
    # 提取 number
    uid_df_a['number'] = uid_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    # match_score_df_a['number'] = match_score_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    # live_rate_df_a['number'] = live_rate_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    att1_df_a['number'] = att1_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    att2_df_a['number'] = att2_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    att3_df_a['number'] = att3_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    att4_df_a['number'] = att4_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    
    # 删除无用列
    uid_df_a = uid_df_a.drop(columns=['variable'])
    # match_score_df_a = match_score_df_a.drop(columns=['variable'])
    # live_rate_df_a = live_rate_df_a.drop(columns=['variable'])
    att1_df_a = att1_df_a.drop(columns=['variable'])
    att2_df_a = att2_df_a.drop(columns=['variable'])
    att3_df_a = att3_df_a.drop(columns=['variable'])
    att4_df_a = att4_df_a.drop(columns=['variable'])
    
    # 合并
    result_df_a = pd.merge(uid_df_a, att1_df_a, on=['wk', 'battle_number', 'number'])
    # result_df_a = pd.merge(result_df_a, match_score_df_a, on=['wk', 'battle_number', 'number'])
    # result_df_a = pd.merge(result_df_a, live_rate_df_a, on=['wk', 'battle_number', 'number'])
    result_df_a = pd.merge(result_df_a, att2_df_a, on=['wk', 'battle_number', 'number'])
    result_df_a = pd.merge(result_df_a, att3_df_a, on=['wk', 'battle_number', 'number'])
    result_df_a = pd.merge(result_df_a, att4_df_a, on=['wk', 'battle_number', 'number'])
    
    addScoreDf = getAddScoreData()
    addScoreDf = addScoreDf.groupby(['wk', 'uid']).agg({'add_score_sum': 'sum'}).reset_index()
    
    result_df_a = pd.merge(result_df_a, addScoreDf, on=['wk', 'uid'], how='left')
    result_df_a['add_score_sum'] = result_df_a['add_score_sum'].fillna(0)

    # 计算新的 match_score
    result_df_a['match_score'] = (result_df_a['attribute1'] * 1 + result_df_a['attribute2'] * 0.6 + result_df_a['attribute3'] * 0.3 + result_df_a['attribute4'] * 0.1) / 1000
    # result_df_a['match_score2'] = result_df_a['match_score_old'] / result_df_a['live_rate']

    # print('重排序前 result_df_a:')
    # print(result_df_a[result_df_a['battle_number'] == 0])
    # print(result_df_a[result_df_a['battle_number'] == 1])
    # 按照wk，battle_number 分组，组内按照 match_score 降序排序，重排 number，并将uid == 0 的行删除
    result_df_a = result_df_a.sort_values(by=['wk', 'battle_number', 'match_score'], ascending=[True, True, False])
    result_df_a['number'] = result_df_a.groupby(['wk', 'battle_number']).cumcount() + 1
    result_df_a = result_df_a[result_df_a['uid'] != 0]

    result_df_a.to_csv('/src/data/20250121result_df_a2.csv', index=False)
    # print('result_df_a:')
    # print(result_df_a[result_df_a['battle_number'] == 0])
    # print(result_df_a[result_df_a['battle_number'] == 1])
    
    # # 重新将数据汇总成每个 wk + battle_number 一行：
    # # 按照 wk + battle_number 分组，计算每个分组的 add_score_sum == 0 的人数占比，记作 add_score_zero_rate
    # # 计算每个分组中 add_score_sum == 0 的 match_score 占比，记作 add_score_zero_match_score_rate
    # grouped_a = result_df_a.groupby(['wk', 'battle_number'])
    # summary_df_a = grouped_a.apply(lambda g: pd.Series({
    #     'add_score_zero_rate_a': (g['add_score_sum'] == 0).mean(),
    #     'add_score_zero_match_score_rate_a': g.loc[g['add_score_sum'] == 0, 'match_score'].sum() / g['match_score'].sum() if g['match_score'].sum() != 0 else 0
    # })).reset_index()

    # summary_df_a = summary_df_a.merge(strengthADf0, on=['wk', 'battle_number'], how='left')
    # print('summary result (a 队)')
    # print(summary_df_a[summary_df_a['battle_number'] == 0])

    # 拆分 b 队部分
    strengthBDf0 = x[['wk', 'battle_number', 'strength_b', 'score_b']]
    strengthBDf1 = x[['wk', 'battle_number']]
    for i in range(1, N + 1):
        strengthBDf1 = pd.concat([strengthBDf1, x[[f'strengthinfo_b_uid_{i}', f'strengthinfo_b_match_score_{i}', f'strengthinfo_b_live_rate_{i}', f'strengthinfo_b_attribute1_{i}',f'strengthinfo_b_attribute2_{i}',f'strengthinfo_b_attribute3_{i}',f'strengthinfo_b_attribute4_{i}']]], axis=1)

    # 将 uid 和 match_score 分别 melt 成两张表
    uid_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_uid_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='uid')
    # match_score_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_match_score_{i}' for i in range(1, N + 1)],
    #                 var_name='variable', value_name='match_score_old')
    # live_rate_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_live_rate_{i}' for i in range(1, N + 1)],
    #                 var_name='variable', value_name='live_rate')
    att1_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_attribute1_{i}' for i in range(1, N + 1)], 
                        var_name='variable', value_name='attribute1')
    att2_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_attribute2_{i}' for i in range(1, N + 1)],
                        var_name='variable', value_name='attribute2')
    att3_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_attribute3_{i}' for i in range(1, N + 1)],
                        var_name='variable', value_name='attribute3')
    att4_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_attribute4_{i}' for i in range(1, N + 1)],
                        var_name='variable', value_name='attribute4')

    # 提取 number
    uid_df_b['number'] = uid_df_b['variable'].str.extract(r'_(\d+)').astype(int)
    # match_score_df_b['number'] = match_score_df_b['variable'].str.extract(r'_(\d+)').astype(int)
    # live_rate_df_b['number'] = live_rate_df_b['variable'].str.extract(r'_(\d+)').astype(int)
    att1_df_b['number'] = att1_df_b['variable'].str.extract(r'_(\d+)').astype(int)
    att2_df_b['number'] = att2_df_b['variable'].str.extract(r'_(\d+)').astype(int)
    att3_df_b['number'] = att3_df_b['variable'].str.extract(r'_(\d+)').astype(int)
    att4_df_b['number'] = att4_df_b['variable'].str.extract(r'_(\d+)').astype(int)

    # 删除无用列
    uid_df_b = uid_df_b.drop(columns=['variable'])
    # match_score_df_b = match_score_df_b.drop(columns=['variable'])
    # live_rate_df_b = live_rate_df_b.drop(columns=['variable'])
    att1_df_b = att1_df_b.drop(columns=['variable'])
    att2_df_b = att2_df_b.drop(columns=['variable'])
    att3_df_b = att3_df_b.drop(columns=['variable'])
    att4_df_b = att4_df_b.drop(columns=['variable'])

    # 合并
    result_df_b = pd.merge(uid_df_b, att1_df_b, on=['wk', 'battle_number', 'number'])
    # result_df_b = pd.merge(result_df_b, match_score_df_b, on=['wk', 'battle_number', 'number'])
    # result_df_b = pd.merge(result_df_b, live_rate_df_b, on=['wk', 'battle_number', 'number'])
    result_df_b = pd.merge(result_df_b, att2_df_b, on=['wk', 'battle_number', 'number'])
    result_df_b = pd.merge(result_df_b, att3_df_b, on=['wk', 'battle_number', 'number'])
    result_df_b = pd.merge(result_df_b, att4_df_b, on=['wk', 'battle_number', 'number'])

    result_df_b = pd.merge(result_df_b, addScoreDf, on=['wk', 'uid'], how='left')
    result_df_b['add_score_sum'] = result_df_b['add_score_sum'].fillna(0)

    # 计算新的 match_score
    result_df_b['match_score'] = (result_df_b['attribute1'] * 1 + result_df_b['attribute2'] * 0.6 + result_df_b['attribute3'] * 0.3 + result_df_b['attribute4'] * 0.1) / 1000
    # result_df_b['match_score2'] = result_df_b['match_score_old'] / result_df_b['live_rate']

    # print('重排序前 result_df_b:')
    # print(result_df_b[result_df_b['battle_number'] == 0])


    # 按照wk，battle_number 分组，组内按照 match_score 降序排序，重排 number，并将uid == 0 的行删除
    result_df_b = result_df_b.sort_values(by=['wk', 'battle_number', 'match_score'], ascending=[True, True, False])
    result_df_b['number'] = result_df_b.groupby(['wk', 'battle_number']).cumcount() + 1
    result_df_b = result_df_b[result_df_b['uid'] != 0]

    result_df_b.to_csv('/src/data/20250121result_df_b2.csv', index=False)

    # print('result_df_b')
    # print(result_df_b[result_df_b['battle_number'] == 0])
    
    # # 重新将数据汇总成每个 wk + battle_number 一行：
    # # 按照 wk + battle_number 分组，计算每个分组的 add_score_sum == 0 的人数占比，记作 add_score_zero_rate
    # # 计算每个分组中 add_score_sum == 0 的 match_score 占比，记作 add_score_zero_match_score_rate
    # grouped_b = result_df_b.groupby(['wk', 'battle_number'])
    # summary_df_b = grouped_b.apply(lambda g: pd.Series({
    #     'add_score_zero_rate_b': (g['add_score_sum'] == 0).mean(),
    #     'add_score_zero_match_score_rate_b': g.loc[g['add_score_sum'] == 0, 'match_score'].sum() / g['match_score'].sum() if g['match_score'].sum() != 0 else 0
    # })).reset_index()

    # summary_df_b = summary_df_b.merge(strengthBDf0, on=['wk', 'battle_number'], how='left')
    # print('summary result (b 队)')
    # print(summary_df_b[summary_df_b['battle_number'] == 0])

    #     # 合并 a 队和 b 队的结果
    # final_summary_df = pd.merge(summary_df_a, summary_df_b, on=['wk', 'battle_number'], how='left')

    # # 选择最终需要的列
    # final_columns = [
    #     'wk', 'battle_number', 
    #     'add_score_zero_rate_a', 'add_score_zero_match_score_rate_a', 'strength_a', 'score_a',
    #     'add_score_zero_rate_b', 'add_score_zero_match_score_rate_b', 'strength_b', 'score_b'
    # ]
    # final_summary_df = final_summary_df[final_columns]

    # print('final summary result')
    # print(final_summary_df[final_summary_df['battle_number'] == 0])

    # # 将结果保存到文件
    # final_summary_df.to_csv('/src/data/20250121final_summary.csv', index=False)

# 数据整理
def prepareDataForTrain(recalculate=False):
    result_a_path = '/src/data/20250121result_df_a2.csv'
    result_b_path = '/src/data/20250121result_df_b2.csv'
    summary_path = '/src/data/20250121final_summary2.csv'

    filename = '/src/data/20250121_combined_result2.csv'

    if recalculate or not (os.path.exists(filename)):
        # 读取数据
        aDf = pd.read_csv(result_a_path)
        bDf = pd.read_csv(result_b_path)
        summaryDf = pd.read_csv(summary_path)

        # 添加 team 列
        aDf['team'] = 'a'
        bDf['team'] = 'b'

        # 合并 a 和 b 队的数据
        combinedDf = pd.concat([aDf, bDf], axis=0)

        # 计算 match_score_rate
        combinedDf['match_score_rate'] = combinedDf.groupby(['wk', 'battle_number', 'team'])['match_score'].transform(lambda x: x / x.sum())

        # 计算 strength_percentile
        # 先将 a 队和 b 队的 strength 数据合并在一起
        strengthDf = summaryDf[['wk', 'battle_number', 'strength_a', 'strength_b']]
        strengthDf = strengthDf.melt(id_vars=['wk', 'battle_number'], value_vars=['strength_a', 'strength_b'], 
                                     var_name='team', value_name='strength')
        strengthDf['team'] = strengthDf['team'].apply(lambda x: 'a' if x == 'strength_a' else 'b')

        # 计算每个 wk 的 strength 分位数
        strengthDf['strength_percentile'] = strengthDf.groupby('wk')['strength'].rank(pct=True)

        # 将 strength_percentile 合并到 combinedDf
        combinedDf = combinedDf.merge(strengthDf[['wk', 'battle_number', 'team', 'strength_percentile', 'strength']], 
                                      on=['wk', 'battle_number', 'team'], 
                                      how='left')

        # 计算 add_score_sum 和出战状态
        combinedDf['is_active'] = (combinedDf['add_score_sum'] > 0).astype(int)

        combinedDf.to_csv('/src/data/20250121_combined_result_all2.csv', index=False)

        # 选择需要的列
        columns_needed = ['uid', 'wk', 'match_score', 'match_score_rate', 'strength_percentile', 'add_score_sum', 'is_active', 'team', 'strength']
        result_df = combinedDf[columns_needed]

        # 保存结果
        result_df.to_csv(filename, index=False)
    else:
        # 直接从记录结果中获取结果
        result_df = pd.read_csv(filename)

    # 
    print('去除 uid 为 0 的数据')
    print('去除前：',len(result_df))
    result_df = result_df[result_df['uid'] != 0]
    print('去除后：',len(result_df))

    # 拆分成 x 和 y
    x = result_df[['match_score_rate', 'strength_percentile']]
    y = result_df['is_active']

    # 添加 debug 信息
    print('result_df with strength column:')
    print(result_df.head())

    return x, y

def decisionTreeClassification(recalculate=False):
    x, y = prepareDataForTrain(recalculate)

    # 检查并处理 NaN 值
    if x.isnull().values.any():
        print("NaN values found in x, filling with 0")
        x = x.fillna(0)
    if y.isnull().values.any():
        print("NaN values found in y, filling with 0")
        y = y.fillna(0)

    # 拆分数据集为训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # 创建决策树分类器
    clf = DecisionTreeClassifier(random_state=0, max_depth=2, min_samples_split=10, min_samples_leaf=5, criterion='gini')

    # 训练模型
    clf.fit(x_train, y_train)

    # 预测
    y_pred = clf.predict(x_test)

    # 计算准确率、精确率、召回率和 F1 分数
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    print(f'Accuracy: {accuracy:.2f}')
    print(f'Precision: {precision:.2f}')
    print(f'Recall: {recall:.2f}')
    print(f'F1 Score: {f1:.2f}')

    # 将测试集结果与原始数据合并
    x_test['y_true'] = y_test
    x_test['y_pred'] = y_pred

    # 按照 strength_percentile 分组计算每组的准确率、精确率、召回率和 F1 分数
    x_test['strength_group'] = pd.cut(x_test['strength_percentile'], bins=np.arange(0, 1.05, 0.05), include_lowest=True)
    grouped = x_test.groupby('strength_group')
    
    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['y_true'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['y_true'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['y_true'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['y_true'], g['y_pred'], zero_division=0))

    # 绘制图表
    x0 = [interval.mid for interval in accuracy_by_group.index]
    y_accuracy = accuracy_by_group.values
    y_precision = precision_by_group.values
    y_recall = recall_by_group.values
    y_f1 = f1_by_group.values

    plt.figure(figsize=(15, 6))  # 设置图的尺寸

    plt.plot(x0, y_accuracy, marker='o', linestyle='-', color='b', label='Accuracy')
    plt.plot(x0, y_precision, marker='o', linestyle='-', color='g', label='Precision')
    plt.plot(x0, y_recall, marker='o', linestyle='-', color='r', label='Recall')
    plt.plot(x0, y_f1, marker='o', linestyle='-', color='c', label='F1 Score')

    plt.scatter(x0, y_accuracy, color='b')
    plt.scatter(x0, y_precision, color='g')
    plt.scatter(x0, y_recall, color='r')
    plt.scatter(x0, y_f1, color='c')

    plt.xlabel('Strength Percentile')
    plt.ylabel('Score')
    plt.title('Model Performance by Strength Percentile')
    plt.legend()
    plt.grid(True)  # 网格线
    plt.savefig('/src/data/20250121dt2.png')  # 保存图像

    # 可视化决策树
    plt.figure(figsize=(20, 20))
    plot_tree(clf, filled=True, feature_names=x.columns, class_names=['Class 0', 'Class 1'])
    plt.title('Decision Tree Visualization')
    plt.savefig('/src/data/20250121dt_tree2.png')  # 保存决策树图像

    # 保存模型
    import joblib
    joblib.dump(clf, '/src/data/20250121dt_model2.pkl')

def prepareDataForTest(recalculate=False):
    result_a_path = '/src/data/20250121result_df_a2.csv'
    result_b_path = '/src/data/20250121result_df_b2.csv'
    summary_path = '/src/data/20250121final_summary2.csv'

    filename = '/src/data/20250121_combined_result2.csv'

    if recalculate or not (os.path.exists(filename)):
        # 读取数据
        aDf = pd.read_csv(result_a_path)
        bDf = pd.read_csv(result_b_path)
        summaryDf = pd.read_csv(summary_path)

        # 添加 team 列
        aDf['team'] = 'a'
        bDf['team'] = 'b'

        # 合并 a 和 b 队的数据
        combinedDf = pd.concat([aDf, bDf], axis=0)

        # 计算 match_score_rate
        combinedDf['match_score_rate'] = combinedDf.groupby(['wk', 'battle_number', 'team'])['match_score'].transform(lambda x: x / x.sum())

        # 计算 strength_percentile
        # 先将 a 队和 b 队的 strength 数据合并在一起
        strengthDf = summaryDf[['wk', 'battle_number', 'strength_a', 'strength_b']]
        strengthDf = strengthDf.melt(id_vars=['wk', 'battle_number'], value_vars=['strength_a', 'strength_b'], 
                                    var_name='team', value_name='strength')
        strengthDf['team'] = strengthDf['team'].apply(lambda x: 'a' if x == 'strength_a' else 'b')

        # 计算每个 wk 的 strength 分位数
        strengthDf['strength_percentile'] = strengthDf.groupby('wk')['strength'].rank(pct=True)

        # 将 strength_percentile 合并到 combinedDf
        combinedDf = combinedDf.merge(strengthDf[['wk', 'battle_number', 'team', 'strength_percentile', 'strength']], 
                                    on=['wk', 'battle_number', 'team'], 
                                    how='left')

        # 计算 add_score_sum 和出战状态
        combinedDf['is_active'] = (combinedDf['add_score_sum'] > 0).astype(int)

        combinedDf.to_csv('/src/data/20250121_combined_result_all2.csv', index=False)

        # 选择需要的列
        columns_needed = ['uid', 'wk', 'match_score', 'match_score_rate', 'strength_percentile', 'add_score_sum', 'is_active', 'team', 'strength']
        result_df = combinedDf[columns_needed]

        # 保存结果
        result_df.to_csv(filename, index=False)
    else:
        # 直接从记录结果中获取结果
        result_df = pd.read_csv(filename)

    # 
    # print('去除 uid 为 0 的数据')
    # print('去除前：',len(result_df))
    # result_df = result_df[result_df['uid'] != 0]
    # print('去除后：',len(result_df))

    # print('result_df 中包含的wk:')
    # print(result_df.sort_values(['wk']).groupby('wk').size())

    # 将 wk >= '2024-12-30' 的数据作为测试数据
    train_df = result_df[result_df['wk'] < '2024-12-30']
    test_df = result_df[(result_df['wk'] >= '2024-12-30') & (result_df['wk'] <= '2025-01-06')]
    print('train_df:', len(train_df))
    print('test_df:', len(test_df))

    # x 中要保留更多信息，以便后续分析

    trainX = train_df[['wk', 'uid', 'match_score_rate', 'strength_percentile']]
    trainY = train_df[['wk', 'uid','is_active']]

    testX = test_df[['wk', 'uid', 'match_score_rate', 'strength_percentile']]
    testY = test_df[['wk', 'uid','is_active']]

    return trainX, trainY, testX, testY


def test():
    # 需要数据
    # wk，battle_number，score_a，score_b
    # 用score_a，score_b 计算是否质量局，即 (大的队伍的score - 小的队伍的score) / (小的队伍的score) < 1
    # is_quality 是质量局的标志，1 为质量局，0 为非质量局
    filename = '/src/data/20250121final_summary2.csv'
    df = pd.read_csv(filename)
    df['bigger_score'] = df[['score_a', 'score_b']].max(axis=1)
    df['smaller_score'] = df[['score_a', 'score_b']].min(axis=1)
    df['is_quality'] = ((df['bigger_score'] - df['smaller_score']) / df['smaller_score']) < 1
    df['is_quality'] = df['is_quality'].astype(int)
    # print(df.head())

    trainX, trainY, testX, testY = prepareDataForTest()

    trainX = trainX.fillna(0)
    testX = testX.fillna(0)

    # 创建决策树分类器
    clf = DecisionTreeClassifier(random_state=0, max_depth=2, min_samples_split=10, min_samples_leaf=5, criterion='gini')

    # 训练模型
    clf.fit(trainX[['match_score_rate', 'strength_percentile']], trainY[['is_active']])

    # 预测
    y_pred = clf.predict(testX[['match_score_rate', 'strength_percentile']])
    # 计算准确率、精确率、召回率和 F1 分数
    accuracy = accuracy_score(testY[['is_active']], y_pred)
    precision = precision_score(testY[['is_active']], y_pred)
    recall = recall_score(testY[['is_active']], y_pred)
    f1 = f1_score(testY[['is_active']], y_pred)

    print(f'Accuracy: {accuracy:.2f}')
    print(f'Precision: {precision:.2f}')
    print(f'Recall: {recall:.2f}')
    print(f'F1 Score: {f1:.2f}')


    # 将测试集结果与原始数据合并
    testX['is_active'] = testY['is_active']
    testX['y_true'] = testY['is_active']
    testX['y_pred'] = y_pred

    # print('testX:')
    # print(testX.head())
    testX.to_csv('/src/data/20250121testX2.csv', index=False)


    # 按照 strength_percentile 分组计算每组的准确率、精确率、召回率和 F1 分数
    testX['strength_group'] = pd.cut(testX['strength_percentile'], bins=np.arange(0, 1.05, 0.05), include_lowest=True)
    grouped = testX.groupby('strength_group')
    
    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['y_true'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['y_true'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['y_true'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['y_true'], g['y_pred'], zero_division=0))

    # 绘制图表
    x0 = [interval.mid for interval in accuracy_by_group.index]
    y_accuracy = accuracy_by_group.values
    y_precision = precision_by_group.values
    y_recall = recall_by_group.values
    y_f1 = f1_by_group.values

    plt.figure(figsize=(15, 6))  # 设置图的尺寸

    plt.plot(x0, y_accuracy, marker='o', linestyle='-', color='b', label='Accuracy')
    plt.plot(x0, y_precision, marker='o', linestyle='-', color='g', label='Precision')
    plt.plot(x0, y_recall, marker='o', linestyle='-', color='r', label='Recall')
    plt.plot(x0, y_f1, marker='o', linestyle='-', color='c', label='F1 Score')

    plt.scatter(x0, y_accuracy, color='b')
    plt.scatter(x0, y_precision, color='g')
    plt.scatter(x0, y_recall, color='r')
    plt.scatter(x0, y_f1, color='c')

    plt.xlabel('Strength Percentile')
    plt.ylabel('Score')
    plt.title('Model Performance by Strength Percentile')
    plt.legend()
    plt.grid(True)  # 网格线
    plt.savefig('/src/data/20250121dt2.png')  # 保存图像

    # 可视化决策树
    plt.figure(figsize=(20, 20))
    plot_tree(clf, filled=True, feature_names=['match_score_rate', 'strength_percentile'], class_names=['No', 'Yes'])
    plt.title('Decision Tree Visualization')
    plt.savefig('/src/data/20250121dt_tree2.png')  # 保存决策树图像


    # 过滤，只要匹配分数前 10 的数据
    N = 10

    result_a_path = '/src/data/20250121result_df_a2.csv'
    aDf = pd.read_csv(result_a_path)

    aDf = aDf[aDf['wk'] >= '2024-12-30']
    aDf = aDf[aDf['number'] <= N]

    aDf = aDf.merge(testX[['wk','uid','y_pred', 'is_active']], on=['wk', 'uid'], how='left')
    aDf = aDf[['wk', 'battle_number', 'uid','match_score','y_pred', 'is_active']]
    
    aDf['match_score2'] = aDf['match_score'] * aDf['y_pred']
    # aDf['match_score2'] = aDf['match_score'] * aDf['is_active']

    print(aDf[aDf['battle_number'] == 6])
    aDfSummary = aDf.groupby(['wk', 'battle_number']).agg({'match_score2': 'sum'}).reset_index()
    print('aDfSummary:')
    print(aDfSummary.head())

    result_b_path = '/src/data/20250121result_df_b2.csv'
    bDf = pd.read_csv(result_b_path)
    bDf = bDf[bDf['wk'] >= '2024-12-30']
    bDf = bDf[bDf['number'] <= N]

    bDf = bDf.merge(testX[['wk','uid','y_pred', 'is_active']], on=['wk', 'uid'], how='left')
    bDf = bDf[['wk', 'battle_number', 'uid','match_score','y_pred', 'is_active']]
    
    bDf['match_score2'] = bDf['match_score'] * bDf['y_pred']
    # bDf['match_score2'] = bDf['match_score'] * bDf['is_active']

    print(bDf[bDf['battle_number'] == 6])
    bDfSummary = bDf.groupby(['wk', 'battle_number']).agg({'match_score2': 'sum'}).reset_index()
    print('bDfSummary:')
    print(bDfSummary.head())
    
    df = df[df['wk'] >= '2024-12-30']
    df = df[['wk', 'battle_number', 'is_quality']]
    aDfSummary = aDfSummary[['wk', 'battle_number', 'match_score2']]
    aDfSummary.rename(columns={'match_score2': 'strength_a'}, inplace=True)
    df = df.merge(aDfSummary, on=['wk', 'battle_number'], how='left')
    bDfSummary = bDfSummary[['wk', 'battle_number', 'match_score2']]
    bDfSummary.rename(columns={'match_score2': 'strength_b'}, inplace=True)
    df = df.merge(bDfSummary, on=['wk', 'battle_number'], how='left')

    print('final result:')
    print(df.head())
    
    # 按照decisionTreeClassification的模型，重新计算strength_a，strength_b
    # 计算 两队的strength_diff_ratio 即 (大的队伍的strength - 小的队伍的strength) / (小的队伍的strength)
    # 然后计算 每场比赛的 strength_diff_ratio 小于 N 的情况下，预测为质量局，预测的precision，recall，f1
    # 其中N为 5% 到 25% 之间的值

    df['bigger_strength'] = df[['strength_a', 'strength_b']].max(axis=1)
    df['smaller_strength'] = df[['strength_a', 'strength_b']].min(axis=1)
    df['strength_diff_ratio'] = (df['bigger_strength'] - df['smaller_strength']) / df['smaller_strength']

    # 定义阈值范围
    
    thresholds = np.linspace(0, 1.5, 151)
    result = []

    for threshold in thresholds:
        df['is_quality_pred'] = (df['strength_diff_ratio'] < threshold).astype(int)
        precision = precision_score(df['is_quality'], df['is_quality_pred'])
        recall = recall_score(df['is_quality'], df['is_quality_pred'])
        f1 = f1_score(df['is_quality'], df['is_quality_pred'])
        result.append([threshold, precision, recall, f1])

    result = pd.DataFrame(result, columns=['threshold', 'precision', 'recall', 'f1'])
    print('result:')
    print(result)
    result.to_csv('/src/data/20250121result2.csv', index=False)


# test2 往数据中加入更多特征
def prepareDataForTest2(recalculate=False):
    result_a_path = '/src/data/20250121result_df_a2.csv'
    result_b_path = '/src/data/20250121result_df_b2.csv'
    summary_path = '/src/data/20250121final_summary2.csv'

    filename = '/src/data/20250121_combined_result2.csv'

    if recalculate or not (os.path.exists(filename)):
        # 读取数据
        aDf = pd.read_csv(result_a_path)
        bDf = pd.read_csv(result_b_path)
        summaryDf = pd.read_csv(summary_path)

        # 添加 team 列
        aDf['team'] = 'a'
        bDf['team'] = 'b'

        # 合并 a 和 b 队的数据
        combinedDf = pd.concat([aDf, bDf], axis=0)

        # 计算 match_score_rate
        combinedDf['match_score_rate'] = combinedDf.groupby(['wk', 'battle_number', 'team'])['match_score'].transform(lambda x: x / x.sum())

        # 计算 strength_percentile
        # 先将 a 队和 b 队的 strength 数据合并在一起
        strengthDf = summaryDf[['wk', 'battle_number', 'strength_a', 'strength_b']]
        strengthDf = strengthDf.melt(id_vars=['wk', 'battle_number'], value_vars=['strength_a', 'strength_b'], 
                                    var_name='team', value_name='strength')
        strengthDf['team'] = strengthDf['team'].apply(lambda x: 'a' if x == 'strength_a' else 'b')

        # 计算每个 wk 的 strength 分位数
        strengthDf['strength_percentile'] = strengthDf.groupby('wk')['strength'].rank(pct=True)

        # 将 strength_percentile 合并到 combinedDf
        combinedDf = combinedDf.merge(strengthDf[['wk', 'battle_number', 'team', 'strength_percentile', 'strength']], 
                                    on=['wk', 'battle_number', 'team'], 
                                    how='left')

        # 计算 add_score_sum 和出战状态
        combinedDf['is_active'] = (combinedDf['add_score_sum'] > 0).astype(int)

        combinedDf.to_csv('/src/data/20250121_combined_result_all2.csv', index=False)

        # 选择需要的列
        columns_needed = ['uid', 'wk', 'match_score', 'match_score_rate', 'strength_percentile', 'add_score_sum', 'is_active', 'team', 'strength']
        result_df = combinedDf[columns_needed]

        # 保存结果
        result_df.to_csv(filename, index=False)
    else:
        # 直接从记录结果中获取结果
        result_df = pd.read_csv(filename)

    result_df['wk'] = pd.to_datetime(result_df['wk'])
    # print('result_df 中包含的wk:')
    # print(result_df.sort_values(['wk']).groupby('wk').size())

    newFilename = '/src/data/20250121smfb_data_20241125_20250120.csv'
    newDf = pd.read_csv(newFilename)
    newDf['wk'] = pd.to_datetime(newDf['wk'])
    newDf.rename(columns={'#account_id': 'uid'}, inplace=True)
    # print('newDf 中包含的wk:')
    # print(newDf.sort_values(['wk']).groupby('wk').size())

    result_df = result_df.merge(newDf[['wk', 'uid', 'individual_score_total_mean', '3day_login_count','7day_login_count']], on=['wk', 'uid'], how='left')

    # 将 wk >= '2024-12-30' 的数据作为测试数据
    train_df = result_df[result_df['wk'] < '2024-12-30']
    test_df = result_df[(result_df['wk'] >= '2024-12-30') & (result_df['wk'] <= '2025-01-06')]
    print('train_df:', len(train_df))
    print('test_df:', len(test_df))

    # x 中要保留更多信息，以便后续分析

    trainX = train_df[['wk', 'uid', 'match_score_rate', 'strength_percentile', 'individual_score_total_mean', '3day_login_count','7day_login_count']]
    trainY = train_df[['wk', 'uid','is_active']]

    testX = test_df[['wk', 'uid', 'match_score_rate', 'strength_percentile', 'individual_score_total_mean', '3day_login_count','7day_login_count']]
    testY = test_df[['wk', 'uid','is_active']]

    return trainX, trainY, testX, testY



def test2():
    trainX, trainY, testX, testY = prepareDataForTest2()

    trainX = trainX.fillna(0)
    testX = testX.fillna(0)

    # for test 简单验算一下现有规则的准确率
    testX['y_true'] = testY['is_active']
    testX['y_pred'] = 1
    testX.loc[(testX['individual_score_total_mean'] > 0)
            & (testX['individual_score_total_mean'] < 5000), 'y_pred'] = 0
    testX.loc[(testX['individual_score_total_mean'] >= 5000) &
            (testX['7day_login_count'] == 0), 'y_pred'] = 0
    testX.loc[(testX['individual_score_total_mean'] == 0) &
            (testX['3day_login_count'] == 0), 'y_pred'] = 0
    
    accuracy = accuracy_score(testX['y_true'], testX['y_pred'])
    precision = precision_score(testX['y_true'], testX['y_pred'])
    recall = recall_score(testX['y_true'], testX['y_pred'])
    f1 = f1_score(testX['y_true'], testX['y_pred'])
    print('test2:')
    print(f'Accuracy: {accuracy:.2f}')
    print(f'Precision: {precision:.2f}')
    print(f'Recall: {recall:.2f}')
    print(f'F1 Score: {f1:.2f}')
    return

    # 创建决策树分类器
    clf = DecisionTreeClassifier(random_state=0, max_depth=2, min_samples_split=10, min_samples_leaf=5, criterion='gini')

    # 训练模型
    clf.fit(trainX[['match_score_rate', 'strength_percentile','individual_score_total_mean', '3day_login_count','7day_login_count']], trainY[['is_active']])

    # 预测
    y_pred = clf.predict(testX[['match_score_rate', 'strength_percentile','individual_score_total_mean', '3day_login_count','7day_login_count']])
    # 计算准确率、精确率、召回率和 F1 分数
    accuracy = accuracy_score(testY[['is_active']], y_pred)
    precision = precision_score(testY[['is_active']], y_pred)
    recall = recall_score(testY[['is_active']], y_pred)
    f1 = f1_score(testY[['is_active']], y_pred)

    print(f'Accuracy: {accuracy:.2f}')
    print(f'Precision: {precision:.2f}')
    print(f'Recall: {recall:.2f}')
    print(f'F1 Score: {f1:.2f}')


def test3():
    filename = '/src/data/20250121smfb_data_20241125_20250120.csv'
    df = pd.read_csv(filename)
    df['wk'] = pd.to_datetime(df['wk'])
    df.rename(columns={'#account_id': 'uid'}, inplace=True)
    
    trainDf = df[df['wk'] < '2024-12-30']
    testDf = df[(df['wk'] >= '2024-12-30') & (df['wk'] <= '2025-01-06')]

    clf = DecisionTreeClassifier(random_state=0, max_depth=2, min_samples_split=10, min_samples_leaf=5, criterion='gini')

    # 训练模型
    clf.fit(trainDf[['individual_score_total_mean', '3day_login_count','7day_login_count']], trainDf[['activity']])
    # 预测

    y_pred = clf.predict(testDf[['individual_score_total_mean', '3day_login_count','7day_login_count']])

    # 计算准确率、精确率、召回率和 F1 分数
    accuracy = accuracy_score(testDf[['activity']], y_pred)
    precision = precision_score(testDf[['activity']], y_pred)
    recall = recall_score(testDf[['activity']], y_pred)
    f1 = f1_score(testDf[['activity']], y_pred)

    print(f'Accuracy: {accuracy:.2f}')
    print(f'Precision: {precision:.2f}')
    print(f'Recall: {recall:.2f}')
    print(f'F1 Score: {f1:.2f}')


    # 将测试集结果与原始数据合并
    testX = testDf[['wk', 'uid', 'strength', 'activity']].copy()
    testX.rename(columns={'activity': 'y_true'}, inplace=True)
    testX['y_pred'] = y_pred

    testX.to_csv('/src/data/20250121testX3.csv', index=False)

    # 按照 strength 的分位数分组计算每组的准确率、精确率、召回率和 F1 分数
    testX['strength_group'] = pd.qcut(testX['strength'], q=20, duplicates='drop')
    grouped = testX.groupby('strength_group')
    
    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['y_true'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['y_true'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['y_true'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['y_true'], g['y_pred'], zero_division=0))

    # 绘制图表
    x0 = [interval.mid for interval in accuracy_by_group.index]
    y_accuracy = accuracy_by_group.values
    y_precision = precision_by_group.values
    y_recall = recall_by_group.values
    y_f1 = f1_by_group.values

    plt.figure(figsize=(15, 6))  # 设置图的尺寸

    plt.plot(x0, y_accuracy, marker='o', linestyle='-', color='b', label='Accuracy')
    plt.plot(x0, y_precision, marker='o', linestyle='-', color='g', label='Precision')
    plt.plot(x0, y_recall, marker='o', linestyle='-', color='r', label='Recall')
    plt.plot(x0, y_f1, marker='o', linestyle='-', color='c', label='F1 Score')

    plt.scatter(x0, y_accuracy, color='b')
    plt.scatter(x0, y_precision, color='g')
    plt.scatter(x0, y_recall, color='r')
    plt.scatter(x0, y_f1, color='c')

    plt.xlabel('Strength Percentile')
    plt.ylabel('Score')
    plt.title('Model Performance by Strength Percentile')
    plt.legend()
    plt.grid(True)  # 网格线
    plt.savefig('/src/data/20250121dt3.png')  # 保存图像

    # 可视化决策树
    plt.figure(figsize=(20, 20))
    plot_tree(clf, filled=True, feature_names=['individual_score_total_mean', '3day_login_count','7day_login_count'], class_names=['No', 'Yes'])
    plt.title('Decision Tree Visualization')
    plt.savefig('/src/data/20250121dt_tree3.png')  # 保存决策树图像


def simplified_decision_tree(individual_score_total_mean):
    if individual_score_total_mean <= 3.0:
        return 0  # No
    else:
        return 1  # Yes

def debug3():
    filename = '/src/data/20250121smfb_data_20241125_20250120.csv'
    df = pd.read_csv(filename)
    df['wk'] = pd.to_datetime(df['wk'])
    df.rename(columns={'#account_id': 'uid'}, inplace=True)
    
    trainDf = df[df['wk'] < '2024-12-30']
    testDf = df[(df['wk'] >= '2024-12-30') & (df['wk'] <= '2025-01-06')]

    # 使用简化的决策规则进行预测
    y_pred = testDf['individual_score_total_mean'].apply(simplified_decision_tree)

    # 计算准确率、精确率、召回率和 F1 分数
    accuracy = accuracy_score(testDf['activity'], y_pred)
    precision = precision_score(testDf['activity'], y_pred)
    recall = recall_score(testDf['activity'], y_pred)
    f1 = f1_score(testDf['activity'], y_pred)

    print(f'Accuracy: {accuracy:.2f}')
    print(f'Precision: {precision:.2f}')
    print(f'Recall: {recall:.2f}')
    print(f'F1 Score: {f1:.2f}')

    # 将测试集结果与原始数据合并
    testX = testDf[['wk', 'uid', 'strength', 'activity']].copy()
    testX.rename(columns={'activity': 'y_true'}, inplace=True)
    testX['y_pred'] = y_pred

    testX.to_csv('/src/data/20250121testX3_debug.csv', index=False)

    # 按照 strength 的分位数分组计算每组的准确率、精确率、召回率和 F1 分数
    testX['strength_group'] = pd.qcut(testX['strength'], q=20, duplicates='drop')
    grouped = testX.groupby('strength_group')
    
    accuracy_by_group = grouped.apply(lambda g: accuracy_score(g['y_true'], g['y_pred']))
    precision_by_group = grouped.apply(lambda g: precision_score(g['y_true'], g['y_pred'], zero_division=0))
    recall_by_group = grouped.apply(lambda g: recall_score(g['y_true'], g['y_pred'], zero_division=0))
    f1_by_group = grouped.apply(lambda g: f1_score(g['y_true'], g['y_pred'], zero_division=0))

    # 绘制图表
    x0 = [interval.mid for interval in accuracy_by_group.index]
    y_accuracy = accuracy_by_group.values
    y_precision = precision_by_group.values
    y_recall = recall_by_group.values
    y_f1 = f1_by_group.values

    plt.figure(figsize=(15, 6))  # 设置图的尺寸

    plt.plot(x0, y_accuracy, marker='o', linestyle='-', color='b', label='Accuracy')
    plt.plot(x0, y_precision, marker='o', linestyle='-', color='g', label='Precision')
    plt.plot(x0, y_recall, marker='o', linestyle='-', color='r', label='Recall')
    plt.plot(x0, y_f1, marker='o', linestyle='-', color='c', label='F1 Score')

    plt.scatter(x0, y_accuracy, color='b')
    plt.scatter(x0, y_precision, color='g')
    plt.scatter(x0, y_recall, color='r')
    plt.scatter(x0, y_f1, color='c')

    plt.xlabel('Strength Percentile')
    plt.ylabel('Score')
    plt.title('Model Performance by Strength Percentile')
    plt.legend()
    plt.grid(True)  # 网格线

    plt.savefig('/src/data/20250121debug3.png')  # 保存图像

def debug3_1():
    filename = '/src/data/20250121smfb_data_20241125_20250120.csv'
    df = pd.read_csv(filename)
    df['wk'] = pd.to_datetime(df['wk'])
    df.rename(columns={'#account_id': 'uid'}, inplace=True)
    
    # 选择测试集数据
    testDf = df[(df['wk'] >= '2024-12-30') & (df['wk'] <= '2025-01-06')].copy()

    # 抽样1000条数据
    sampleDf = testDf.sample(n=1000, random_state=0)
    sampleDf.to_csv('/src/data/20250121testX3_debug3_1_sample.csv', index=False)

    # 按照 strength 的分位数分组
    testDf['strength_group'] = pd.qcut(testDf['strength'], q=20, duplicates='drop')
    grouped = testDf.groupby('strength_group')

    # 计算每组的 activity 均值
    activity_mean_by_group = grouped['activity'].mean()

    # 绘制图表
    x0 = [interval.mid for interval in activity_mean_by_group.index]
    y_activity_mean = activity_mean_by_group.values

    plt.figure(figsize=(15, 6))  # 设置图的尺寸

    plt.plot(x0, y_activity_mean, marker='o', linestyle='-', color='b', label='Activity Mean')

    plt.scatter(x0, y_activity_mean, color='b')

    plt.xlabel('Strength Percentile')
    plt.ylabel('Activity Mean')
    plt.title('Activity Mean by Strength Percentile')
    plt.legend()
    plt.grid(True)  # 网格线
    plt.savefig('/src/data/20250121activity_mean_by_strength.png')  # 保存图像

    # 保存分组后的数据
    testDf.to_csv('/src/data/20250121testX3_debug3_1.csv', index=False)

if __name__ == "__main__":
    # main()
    # debug()
    # debug2()
    # analyze()
    # prepareData()
    # decisionTreeClassification()

    # test()

    debug3()
    # debug3_1()

    # test2()

    # test3()
    
    

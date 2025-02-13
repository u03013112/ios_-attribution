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
    x, y = prepareData(df, N)

    # x.to_csv('/src/data/x.csv', index=False)
    columns_to_save = ['wk','strength_a', 'strength_b', 'score_a', 'score_b']
    for i in range(1, N + 1):
        columns_to_save += [f'strengthinfo_a_uid_{i}', f'strengthinfo_a_match_score_{i}']
        columns_to_save += [f'strengthinfo_b_uid_{i}', f'strengthinfo_b_match_score_{i}']

    x = x[columns_to_save]
    # 每一行是一场战斗，添加一列战斗编号
    x['battle_number'] = range(len(x))

        # 拆分 a 队部分
    strengthADf0 = x[['wk', 'battle_number', 'strength_a', 'score_a']]
    strengthADf1 = x[['wk', 'battle_number']]
    for i in range(1, N + 1):
        strengthADf1 = pd.concat([strengthADf1, x[[f'strengthinfo_a_uid_{i}', f'strengthinfo_a_match_score_{i}']]], axis=1)
    
    # print('before melt (a 队)')
    # print(strengthADf1.head())
    
    # 将 uid 和 match_score 分别 melt 成两张表
    uid_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_uid_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='uid')
    score_df_a = pd.melt(strengthADf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_a_match_score_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='match_score')
    
    # 提取 number
    uid_df_a['number'] = uid_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    score_df_a['number'] = score_df_a['variable'].str.extract(r'_(\d+)$').astype(int)
    
    # 删除无用列
    uid_df_a = uid_df_a.drop(columns=['variable'])
    score_df_a = score_df_a.drop(columns=['variable'])
    
    # 合并
    result_df_a = pd.merge(uid_df_a, score_df_a, on=['wk', 'battle_number', 'number'])
    
    addScoreDf = getAddScoreData()
    addScoreDf = addScoreDf.groupby(['wk', 'uid']).agg({'add_score_sum': 'sum'}).reset_index()
    
    result_df_a = pd.merge(result_df_a, addScoreDf, on=['wk', 'uid'], how='left')
    result_df_a['add_score_sum'] = result_df_a['add_score_sum'].fillna(0)

    result_df_a.to_csv('/src/data/20250121result_df_a.csv', index=False)

    # print('result_df_a')
    # print(result_df_a[result_df_a['battle_number'] == 0])

    # 重新将数据汇总成每个 wk + battle_number 一行：
    # 按照 wk + battle_number 分组，计算每个分组的 add_score_sum == 0 的人数占比，记作 add_score_zero_rate
    # 计算每个分组中 add_score_sum == 0 的 match_score 占比，记作 add_score_zero_match_score_rate
    grouped_a = result_df_a.groupby(['wk', 'battle_number'])
    summary_df_a = grouped_a.apply(lambda g: pd.Series({
        'add_score_zero_rate_a': (g['add_score_sum'] == 0).mean(),
        'add_score_zero_match_score_rate_a': g.loc[g['add_score_sum'] == 0, 'match_score'].sum() / g['match_score'].sum() if g['match_score'].sum() != 0 else 0
    })).reset_index()

    summary_df_a = summary_df_a.merge(strengthADf0, on=['wk', 'battle_number'], how='left')
    print('summary result (a 队)')
    print(summary_df_a[summary_df_a['battle_number'] == 0])

        # 拆分 b 队部分
    strengthBDf0 = x[['wk', 'battle_number', 'strength_b', 'score_b']]
    strengthBDf1 = x[['wk', 'battle_number']]
    for i in range(1, N + 1):
        strengthBDf1 = pd.concat([strengthBDf1, x[[f'strengthinfo_b_uid_{i}', f'strengthinfo_b_match_score_{i}']]], axis=1)
    
    # print('before melt (b 队)')
    # print(strengthBDf1.head())
    
    # 将 uid 和 match_score 分别 melt 成两张表
    uid_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_uid_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='uid')
    score_df_b = pd.melt(strengthBDf1, id_vars=['wk', 'battle_number'], value_vars=[f'strengthinfo_b_match_score_{i}' for i in range(1, N + 1)], 
                    var_name='variable', value_name='match_score')
    
    # 提取 number
    uid_df_b['number'] = uid_df_b['variable'].str.extract(r'_(\d+)$').astype(int)
    score_df_b['number'] = score_df_b['variable'].str.extract(r'_(\d+)$').astype(int)
    
    # 删除无用列
    uid_df_b = uid_df_b.drop(columns=['variable'])
    score_df_b = score_df_b.drop(columns=['variable'])
    
    # 合并
    result_df_b = pd.merge(uid_df_b, score_df_b, on=['wk', 'battle_number', 'number'])
    
    result_df_b = pd.merge(result_df_b, addScoreDf, on=['wk', 'uid'], how='left')
    result_df_b['add_score_sum'] = result_df_b['add_score_sum'].fillna(0)

    result_df_b.to_csv('/src/data/20250121result_df_b.csv', index=False)
    # print('result_df_b')
    # print(result_df_b[result_df_b['battle_number'] == 0])

    # 重新将数据汇总成每个 wk + battle_number 一行：
    # 按照 wk + battle_number 分组，计算每个分组的 add_score_sum == 0 的人数占比，记作 add_score_zero_rate
    # 计算每个分组中 add_score_sum == 0 的 match_score 占比，记作 add_score_zero_match_score_rate
    grouped_b = result_df_b.groupby(['wk', 'battle_number'])
    summary_df_b = grouped_b.apply(lambda g: pd.Series({
        'add_score_zero_rate_b': (g['add_score_sum'] == 0).mean(),
        'add_score_zero_match_score_rate_b': g.loc[g['add_score_sum'] == 0, 'match_score'].sum() / g['match_score'].sum() if g['match_score'].sum() != 0 else 0
    })).reset_index()

    summary_df_b = summary_df_b.merge(strengthBDf0, on=['wk', 'battle_number'], how='left')
    print('summary result (b 队)')
    print(summary_df_b[summary_df_b['battle_number'] == 0])

        # 合并 a 队和 b 队的结果
    final_summary_df = pd.merge(summary_df_a, summary_df_b, on=['wk', 'battle_number'], how='left')

    # 选择最终需要的列
    final_columns = [
        'wk', 'battle_number', 
        'add_score_zero_rate_a', 'add_score_zero_match_score_rate_a', 'strength_a', 'score_a',
        'add_score_zero_rate_b', 'add_score_zero_match_score_rate_b', 'strength_b', 'score_b'
    ]
    final_summary_df = final_summary_df[final_columns]

    print('final summary result')
    print(final_summary_df[final_summary_df['battle_number'] == 0])

    # 将结果保存到文件
    final_summary_df.to_csv('/src/data/20250121final_summary.csv', index=False)

def debug():
    # filename = '/src/data/20250121result_df_a.csv'
    filename = '/src/data/20250121result_df_b.csv'
    df = pd.read_csv(filename)

    print(df[df['battle_number'] == 262])

def debug2():
    filename = '/src/data/20250121_combined_result_all.csv'
    df = pd.read_csv(filename)

    print(df[df['battle_number'] == 262])

def analyze():
    filename = '/src/data/20250121final_summary.csv'
    df = pd.read_csv(filename)

    # 将a队和b队的数据合并
    aDf = df[['wk', 'battle_number', 'add_score_zero_match_score_rate_a', 'strength_a']]
    aDf.rename(columns={'add_score_zero_match_score_rate_a': 'add_score_zero_match_score_rate', 'strength_a': 'strength'}, inplace=True)
    bDf = df[['wk', 'battle_number', 'add_score_zero_match_score_rate_b', 'strength_b']]
    bDf.rename(columns={'add_score_zero_match_score_rate_b': 'add_score_zero_match_score_rate', 'strength_b': 'strength'}, inplace=True)

    df = pd.concat([aDf, bDf], axis=0)
    # 按照 strength 的分位数每分组，计算每5%分一组，个分组的 add_score_zero_match_score_rate 的均值
    df['strength_group'] = pd.qcut(df['strength'], 20, duplicates='drop')
    grouped = df.groupby(['strength_group'])
    result = grouped['add_score_zero_match_score_rate'].mean()
    print(result)

    # 画图，横轴为 strength，纵轴为 add_score_zero_match_score_rate 的均值
    
    # 将 Interval 的中点作为 x 轴的值
    x = [interval.mid for interval in result.index]
    y = result.values


    # 设置图的尺寸
    plt.figure(figsize=(15, 6))  # 宽15英寸，高6英寸

    # 绘制折线图
    plt.plot(x, y, marker='o', linestyle='-', color='b', label='Add Score Zero Match Score Rate')

    # 添加散点图以突出显示每个点
    plt.scatter(x, y, color='r')

    # 添加标签和标题
    plt.xlabel('Strength')
    plt.ylabel('Add Score Zero Match Score Rate')
    plt.title('Add Score Zero Match Score Rate by Strength')
    plt.legend()

    # 网格线
    plt.grid(True)

    # 保存图像
    plt.savefig('/src/data/20250121result.png')

# 数据整理
def prepareDataForTrain(recalculate=False):
    result_a_path = '/src/data/20250121result_df_a.csv'
    result_b_path = '/src/data/20250121result_df_b.csv'
    summary_path = '/src/data/20250121final_summary.csv'

    filename = '/src/data/20250121_combined_result.csv'

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

        combinedDf.to_csv('/src/data/20250121_combined_result_all.csv', index=False)

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
    plt.savefig('/src/data/20250121dt.png')  # 保存图像

    # 可视化决策树
    plt.figure(figsize=(20, 20))
    plot_tree(clf, filled=True, feature_names=x.columns, class_names=['Class 0', 'Class 1'])
    plt.title('Decision Tree Visualization')
    plt.savefig('/src/data/20250121dt_tree.png')  # 保存决策树图像


if __name__ == "__main__":
    # main()
    # debug()
    # debug2()
    # analyze()
    # prepareData()
    decisionTreeClassification()
    

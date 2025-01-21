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
        'wk', 'alliance_a_id', 'group_a', 'strengthinfo_a', 'strengthinfo2_a', 'score_a',
        'alliance_b_id', 'group_b', 'strengthinfo_b', 'strengthinfo2_b', 'score_b', 'is_win', 'is_quality',
        'strength_a','strength_b'
    ]

    # 只保留前 N 名的数据
    columns_to_include = []
    for col in df.columns:
        if col not in columns_to_exclude:
            match = re.search(r'_(\d+)$', col)
            if match:
                index = int(match.group(1))
                if index <= N:
                    columns_to_include.append(col)

    x = df[columns_to_include]

    return x, y


def getK(x_train, x_test):
    # 提取训练集中的相关特征和目标变量
    X_train = x_train[['strengthinfo_a_attribute1_1', 'strengthinfo_a_attribute2_1', 'strengthinfo_a_attribute3_1', 'strengthinfo_a_attribute4_1']]
    y_train = x_train['strengthinfo_a_match_score_1'].astype(float) / x_train['strengthinfo_a_live_rate_1'].astype(float)

    # 去除 y_train 中为 0 的样本
    non_zero_indices = y_train != 0
    X_train = X_train[non_zero_indices]
    y_train = y_train[non_zero_indices]

    # 去除 NaN 和无穷大值
    valid_indices = np.isfinite(y_train) & np.all(np.isfinite(X_train), axis=1)
    X_train = X_train[valid_indices]
    y_train = y_train[valid_indices]

    # 提取测试集中的相关特征和目标变量
    X_test = x_test[['strengthinfo_a_attribute1_1', 'strengthinfo_a_attribute2_1', 'strengthinfo_a_attribute3_1', 'strengthinfo_a_attribute4_1']]
    y_test = x_test['strengthinfo_a_match_score_1'].astype(float) / x_test['strengthinfo_a_live_rate_1'].astype(float)

    # 去除 y_test 中为 0 的样本
    non_zero_indices_test = y_test != 0
    X_test = X_test[non_zero_indices_test]
    y_test = y_test[non_zero_indices_test]

    # 去除 NaN 和无穷大值
    valid_indices_test = np.isfinite(y_test) & np.all(np.isfinite(X_test), axis=1)
    X_test = X_test[valid_indices_test]
    y_test = y_test[valid_indices_test]

    # 训练线性回归模型
    model = LinearRegression()
    model.fit(X_train, y_train)

    # 获取系数
    coefficients = model.coef_

    # 将系数分解为 k1, k2, k3, k4
    k1, k2, k3, k4 = coefficients

    # 打印系数
    print(f'k1: {k1}, k2: {k2}, k3: {k3}, k4: {k4}')

    # 在测试集上进行预测
    y_pred = model.predict(X_test)

    # 计算MAPE
    mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
    print(f'MAPE: {mape:.2f}%')

    return k1, k2, k3, k4, mape

def main():
    filename = '/src/data/20250117a2_data.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        df = pd.read_csv(filename)
    else:
        df = getData()
        df.to_csv(filename, index=False)

    N = 10  # 只使用前 N 名的数据
    x, y = prepareData(df, N)
    
    # xForSave = x.head(10)
    # xForSave.to_csv('/src/data/20250117a2_x.csv',index=False)

    # 划分训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=0)

    # 计算 k1, k2, k3, k4 并验证
    k1, k2, k3, k4, mape = getK(x_train, x_test)
    print(f'k1: {k1}, k2: {k2}, k3: {k3}, k4: {k4}')
    print(f'MAPE: {mape:.2f}%')

if __name__ == '__main__':
    main()

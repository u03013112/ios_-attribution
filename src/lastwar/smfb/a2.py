import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping

import re
import json
import os

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

def func1(x_train, x_test, y_train, y_test):
    # 创建决策树分类器
    # clf = DecisionTreeClassifier(random_state=0)
    clf = DecisionTreeClassifier(random_state=0, max_depth=3, min_samples_split=10, min_samples_leaf=5, criterion='gini')

    # 训练模型
    clf.fit(x_train, y_train)

    # 获取特征重要性
    feature_importances = clf.feature_importances_

    # 创建包含特征重要性的 DataFrame
    feature_importance_df = pd.DataFrame({
        'feature': x_train.columns,
        'importance': feature_importances
    })

    # 按重要性排序
    feature_importance_df = feature_importance_df.sort_values(by='importance', ascending=False)

    # 保存特征重要性到文件
    feature_importance_df.to_csv('/src/data/input1.csv', index=False)

    # 预测
    y_pred = clf.predict(x_test)

    # 计算准确率、召回率和F1分数
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    # 打印结果
    print(f'Accuracy: {accuracy:.4f}')
    print(f'Precision: {precision:.4f}')
    print(f'Recall: {recall:.4f}')
    print(f'F1 Score: {f1:.4f}')

def func1_p1(x_train, x_test, y_train, y_test, N):
    # 创建决策树分类器
    # clf = DecisionTreeClassifier(random_state=0, max_depth=3, min_samples_split=10, min_samples_leaf=5, criterion='gini')
    clf = DecisionTreeClassifier(random_state=0, max_depth=10, min_samples_split=5, min_samples_leaf=2, criterion='gini')

    # 训练模型
    clf.fit(x_train, y_train)

    # 获取特征重要性
    feature_importances = clf.feature_importances_

    # 创建包含特征重要性的 DataFrame
    feature_importance_df = pd.DataFrame({
        'feature': x_train.columns,
        'importance': feature_importances
    })

    # 按重要性排序
    feature_importance_df = feature_importance_df.sort_values(by='importance', ascending=False)

    # 保存特征重要性到文件
    feature_importance_df.to_csv('/src/data/input2.csv', index=False)

    # 选择前 N 个重要特征
    top_features = feature_importance_df.head(N)['feature']

    # 使用前 N 个重要特征重新训练模型
    x_train_top = x_train[top_features]
    x_test_top = x_test[top_features]

    # 重新训练决策树模型
    clf.fit(x_train_top, y_train)

    # 预测
    y_pred = clf.predict(x_test_top)
    yPredDf = pd.DataFrame(y_pred)
    yPredDf.to_csv('/src/data/y_pred.csv', index=False)

    # 计算准确率、召回率和F1分数
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    # 打印结果
    print(f'Accuracy: {accuracy:.4f}')
    print(f'Precision: {precision:.4f}')
    print(f'Recall: {recall:.4f}')
    print(f'F1 Score: {f1:.4f}')

    return feature_importance_df

def func2(x_train, x_test, y_train, y_test, validation_split=0.2, epochs=100, batch_size=64, patience=5):
    # 数据标准化
    scaler = StandardScaler()
    x_train = scaler.fit_transform(x_train)
    x_test = scaler.transform(x_test)

    # 构建模型
    model = Sequential()
    model.add(Dense(1024, input_dim=x_train.shape[1], activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(256, activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(256, activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(256, activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(1, activation='sigmoid'))

    # 编译模型
    model.compile(optimizer=Adam(learning_rate=0.001), loss='binary_crossentropy', metrics=['accuracy'])

    # 早停回调
    early_stopping = EarlyStopping(monitor='val_loss', patience=patience, restore_best_weights=True)

    # 训练模型，使用部分训练数据作为验证集
    history = model.fit(x_train, y_train, validation_split=validation_split, epochs=epochs, batch_size=batch_size, verbose=1, callbacks=[early_stopping])

    # 预测
    y_prob = model.predict(x_test).flatten()
    y_pred = (y_prob >= 0.3).astype(int)

    y_probDf = pd.DataFrame(y_prob)
    y_probDf.to_csv('/src/data/y_prob2.csv', index=False)
    y_predDf = pd.DataFrame(y_pred)
    y_predDf.to_csv('/src/data/y_pred2.csv', index=False)

    # 计算准确率、召回率和F1分数
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    # 打印结果
    print(f'Accuracy: {accuracy:.4f}')
    print(f'Precision: {precision:.4f}')
    print(f'Recall: {recall:.4f}')
    print(f'F1 Score: {f1:.4f}')

    return model, history

def main():
    filename = '/src/data/20250117a2_data.csv'
    # filename = '/src/data/20250120a2_data.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        df = pd.read_csv(filename)
    else:
        df = getData()
        df.to_csv(filename, index=False)

    x,y = prepareData(df,5)
    
    # xForSave = x.head(10)
    # xForSave.to_csv('/src/data/20250117a2_x.csv',index=False)

    # 划分训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=0)

    # func1(x_train, x_test, y_train, y_test)
    # func1_p1(x_train, x_test, y_train, y_test, 20)
    model, history = func2(x_train, x_test, y_train, y_test)


if __name__ == '__main__':
    main()
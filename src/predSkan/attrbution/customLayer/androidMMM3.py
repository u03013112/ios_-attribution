import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

def train1L(rNUsd='r1usd_mmm'):
    # 读取数据
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = df[['install_date', 'media', 'r7usd_raw', rNUsd]]

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values=rNUsd).reset_index()
    media_df = media_df.fillna(0)

    # 将y，即r7usd_raw的按天汇总做成df
    y_df = df.groupby('install_date')['r7usd_raw'].sum().reset_index()

    # 将两个df进行merge
    merged_df = pd.merge(media_df, y_df, on='install_date')

    unique_install_dates = merged_df['install_date'].unique()
    train_dates, test_dates = train_test_split(unique_install_dates, test_size=0.3, random_state=42)

    # 根据分割的安装日期划分训练集和测试集
    train_df = merged_df[merged_df['install_date'].isin(train_dates)].sort_values('install_date')
    test_df = merged_df[merged_df['install_date'].isin(test_dates)].sort_values('install_date')

    # 准备训练和测试数据
    input_columns = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    X_train = train_df[input_columns].values
    y_train = train_df['r7usd_raw'].values

    X_test = test_df[input_columns].values
    y_test = test_df['r7usd_raw'].values

    # 创建模型
    model = LinearRegression()
    model.fit(X_train, y_train)

    # 获取并打印权重
    w_pred = model.coef_
    print("Predicted weights (w1, w2, w3, w4, w5):", w_pred)

    # 验证模型预测结果（训练集和测试集）
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)

    mse_train = np.mean((y_train_pred - y_train) ** 2)
    mse_test = np.mean((y_test_pred - y_test) ** 2)

    print("Mean squared error (train):", mse_train)
    print("Mean squared error (test):", mse_test)

    # 保存模型
    import pickle
    with open('/src/data/zk2/model1L.pkl', 'wb') as f:
        pickle.dump(model, f)

def check1L(rNUsd='r1usd_mmm'):
    # 读取数据
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = df[['install_date', 'media', 'r7usd_raw', rNUsd]]

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values=rNUsd).reset_index()
    media_df = media_df.fillna(0)

    # 将y，即r7usd_raw的按天汇总做成df
    y_df = df.groupby('install_date')['r7usd_raw'].sum().reset_index()

    # 将两个df进行merge
    merged_df = pd.merge(media_df, y_df, on='install_date')

    # 准备训练和测试数据
    input_columns = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    X = merged_df[input_columns].values
    y = merged_df['r7usd_raw'].values

    # 加载模型
    import pickle
    with open('/src/data/zk2/model1L.pkl', 'rb') as f:
        model = pickle.load(f)

    # 用此模型预测r7usd_pred，并计算与r7usd_raw的mape
    y_pred = model.predict(X)

    merged_df['r7usd_pred'] = y_pred
    merged_df['mape'] = abs(merged_df['r7usd_raw'] - merged_df['r7usd_pred']) / merged_df['r7usd_raw']
    print("Global MAPE:", merged_df['mape'].mean())

    # 计算并打印每个媒体的MAPE
    for media in input_columns:
        media_df = df[df['media'] == media]
        media_df = media_df.pivot_table(index='install_date', columns='media', values=rNUsd).reset_index()
        media_df = media_df.fillna(0)

        X_media = media_df[[media]].values
        y_media = media_df[media].values

        # 将输入矩阵的形状修改为与模型匹配
        X_media_full = np.zeros((X_media.shape[0], len(input_columns)))
        X_media_full[:, input_columns.index(media)] = X_media[:, 0]

        y_pred_media = model.predict(X_media_full)

        # 计算媒体的r7usd_raw
        media_raw_df = df[df['media'] == media].groupby('install_date')['r7usd_raw'].sum().reset_index()
        y_media_raw = media_raw_df['r7usd_raw'].values

        mape_media = np.mean(np.abs((y_pred_media - y_media_raw) / y_media_raw))
        print(f"{media} MAPE: {mape_media}")


if __name__ == '__main__':
    train1L(rNUsd = 'r3usd_mmm')
    check1L(rNUsd = 'r3usd_mmm')
import pandas as pd
import numpy as np
import json
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_percentage_error, r2_score
from sklearn.preprocessing import StandardScaler, MinMaxScaler, PowerTransformer
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import itertools
import os

# 计算 MAPE 和 R2 的函数
def compute_metrics(y_true, y_pred, groupby_col):
    df = pd.DataFrame({'y_true': y_true, 'y_pred': y_pred, 'groupby_col': groupby_col})
    df_agg = df.groupby('groupby_col').sum()
    mape = mean_absolute_percentage_error(df_agg['y_true'], df_agg['y_pred'])
    r2 = r2_score(df_agg['y_true'], df_agg['y_pred'])
    return mape, r2

# 将预测的 cv7 值转换为 r7usd 预测值
# def convert_cv7_to_r7usd(cv7_pred, cv_map_df7):
#     r7usd_pred = []
#     for cv in cv7_pred:
#         min_revenue = cv_map_df7.loc[cv_map_df7['cv'] == cv, 'min_event_revenue'].values[0]
#         max_revenue = cv_map_df7.loc[cv_map_df7['cv'] == cv, 'max_event_revenue'].values[0]
#         r7usd_pred.append((min_revenue + max_revenue) / 2)
#     return np.array(r7usd_pred)
def convert_cv7_to_r7usd(cv7_pred, cv_map_df7):
    r7usd_pred = []
    for cv in cv7_pred:
        try:
            min_revenue = cv_map_df7.loc[cv_map_df7['cv'] == cv, 'min_event_revenue'].values[0]
        except Exception:
            min_revenue = 0

        try:
            max_revenue = cv_map_df7.loc[cv_map_df7['cv'] == cv, 'max_event_revenue'].values[0]
        except Exception:
            max_revenue = 0

        r7usd_pred.append((min_revenue + max_revenue) / 2)
    return np.array(r7usd_pred)


# 定义超参数网格
param_grid = {
    'epochs': [10, 20],
    'batch_size': [32, 64],
    'hidden_layers': [1, 2],
    'hidden_units': [32, 64]
}

# 创建日志文件
log_file = '/src/data/dnnGpt.csv'
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

# 初始化日志文件
with open(log_file, 'w') as f:
    f.write('cv1,train_mape,train_r2,test_mape,test_r2,params,message\n')

# 加载数据
data_file = '/src/data/dnn1Step2.csv'
df = pd.read_csv(data_file)

# 特征列
feature_columns = ['count', 'countMergeBuilding', 'countMergeArmy', 'countHeroLevelUp', 'countHeroStarUp',
                   'countPayCount', 'countUserLevelMax', 'ENERGY', 'FREE_GOLD', 'MILITARY', 'OILA', 'PAID_GOLD', 'SOIL']

# 特征预处理方法
preprocessing_methods = [
    ('StandardScaler', StandardScaler()),
    ('MinMaxScaler', MinMaxScaler()),
    ('PowerTransformer', PowerTransformer())
]

# 初始化全局预测和真实值
global_y_true_train = []
global_y_pred_train = []
global_y_true_test = []
global_y_pred_test = []

# 对每个 cv1 分类训练和预测模型
unique_cv1 = df['cv1'].unique()
for cv1 in unique_cv1:
    print(f"Processing cv1: {cv1}")

    # 提取当前 cv1 的数据
    df_cv1 = df[df['cv1'] == cv1]

    # 对 cv1 == 0 中的 cv7 == 0 的样本进行采样
    if cv1 == 0:
        df_cv1 = df_cv1.groupby('cv7').apply(lambda x: x.sample(frac=0.1) if x.name == 0 else x).reset_index(drop=True)

    # 划分训练集和测试集
    X = df_cv1[feature_columns]
    y = df_cv1['cv7']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 对特征进行预处理
    for preprocess_name, preprocess_method in preprocessing_methods:
        X_train_preprocessed = preprocess_method.fit_transform(X_train)
        X_test_preprocessed = preprocess_method.transform(X_test)

        # 修改 message 字段以描述当前代码修改和预处理方法
        message = f"Version with feature preprocessing ({preprocess_name}) and sampling for cv1 == 0 and cv7 == 0."

        best_params = None
        best_metrics = None
        best_model = None

        # 超参数网格搜索
        for params in itertools.product(*param_grid.values()):
            param_dict = dict(zip(param_grid.keys(), params))

            # 创建神经网络模型
            model = Sequential()
            model.add(Dense(param_dict['hidden_units'], activation='relu', input_shape=(len(feature_columns),)))
            for _ in range(param_dict['hidden_layers'] - 1):
                model.add(Dense(param_dict['hidden_units'], activation='relu'))
            model.add(Dense(9, activation='softmax'))  # 9 个分类

            # 编译模型
            model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

            # 训练模型
            model.fit(X_train_preprocessed, y_train, epochs=param_dict['epochs'], batch_size=param_dict['batch_size'], verbose=2)

            # 预测训练集和测试集
            y_train_pred = model.predict(X_train_preprocessed)
            y_test_pred = model.predict(X_test_preprocessed)
            y_train_pred_classes = np.argmax(y_train_pred, axis=1)
            y_test_pred_classes = np.argmax(y_test_pred, axis=1)

            # 读取 /src/data/cvMapDf7_%d.csv 文件
            cv_map_df7 = pd.read_csv(f"/src/data/cvMapDf7_{cv1}.csv")

            # 将预测的 cv7 值转换为 r7usd 预测值
            y_train_pred_r7usd = convert_cv7_to_r7usd(y_train_pred_classes, cv_map_df7)
            y_test_pred_r7usd = convert_cv7_to_r7usd(y_test_pred_classes, cv_map_df7)

            # 计算训练集和测试集的 MAPE 和 R2
            train_mape, train_r2 = compute_metrics(y_train, y_train_pred_r7usd, df_cv1.loc[X_train.index, 'installDate'])
            test_mape, test_r2 = compute_metrics(y_test, y_test_pred_r7usd, df_cv1.loc[X_test.index, 'installDate'])

            # 更新全局预测和真实值
            global_y_true_train.extend(y_train.values)
            global_y_pred_train.extend(y_train_pred_r7usd)
            global_y_true_test.extend(y_test.values)
            global_y_pred_test.extend(y_test_pred_r7usd)

            # 记录日志
            log_message = f"{cv1},{train_mape},{train_r2},{test_mape},{test_r2},{json.dumps(param_dict)},{message}\n"
            with open(log_file, 'a') as f:
                f.write(log_message)

            # 更新最优参数和指标
            if best_metrics is None or test_mape < best_metrics['test_mape']:
                best_params = param_dict
                best_metrics = {'train_mape': train_mape, 'train_r2': train_r2, 'test_mape': test_mape, 'test_r2': test_r2}
                best_model = model

        # 输出最优参数和指标
        print(f"Best params for cv1 {cv1} with preprocessing {preprocess_name}: {best_params}")
        print(f"Best metrics for cv1 {cv1} with preprocessing {preprocess_name}: {best_metrics}")

# 计算训练集和测试集的整体 MAPE 和 R2
global_train_mape, global_train_r2 = compute_metrics(global_y_true_train, global_y_pred_train, df.loc[X_train.index, 'installDate'])
global_test_mape, global_test_r2 = compute_metrics(global_y_true_test, global_y_pred_test, df.loc[X_test.index, 'installDate'])

# 输出整体 MAPE 和 R2
print(f"Global train MAPE: {global_train_mape}")
print(f"Global train R2: {global_train_r2}")
print(f"Global test MAPE: {global_test_mape}")
print(f"Global test R2: {global_test_r2}")

# 记录整体训练集和测试集的 MAPE 和 R2 到日志文件
log_message = f"total,{global_train_mape},{global_train_r2},{global_test_mape},{global_test_r2},NA,Global results\n"
with open(log_file, 'a') as f:
    f.write(log_message)
# 用神经网络进行多分类
# 目前有数据Df,每个用户一行
# 列名为：uid,count,r1usd,r7usd,countMergeBuilding,countMergeArmy,countHeroLevelUp,countHeroStarUp,countPayCount,countUserLevelMax,ENERGY,FREE_GOLD,MILITARY,OILA,PAID_GOLD,SOIL,cv1,cv7,installDate
# 其中uid是用户唯一索引，r1usd是用户首日充值金额，r7usd是用户7日充值金额
# installDate是用户安装日期。
# count,countMergeBuilding,countMergeArmy,countHeroLevelUp,countHeroStarUp,countPayCount,countUserLevelMax,ENERGY,FREE_GOLD,MILITARY,OILA,PAID_GOLD,SOIL都是用户行为，即特征。
# cv1是用户的cv1标签，是根据用户r1usd分布进行分类的。有0~7,8个整数分类。
# cv7是用户的cv7标签，是根据用户所在的cv1中的r7usd分布进行分类的。即cv1==0中的cv7==0的用户和cv1==1中的cv7==0的用户是不类似的。cv7有0~8，9个整数分类。
# 这里需要分类的就是cv7标签，要求在每个不同的cv1中使用不同的模型（可以是相同的模型，但是参数不同，需要单独训练）进行分类。分类结果记作cv7p列。
# 文件'/src/data/cvMapDf7_%d.csv'%cv1 中有3列，cv,min_event_revenue,max_event_revenue。在对应的cv1中，用户的cv7是根据r7usd在cvMapDf7中的区间进行分类的。
# 反之在预测分类后，需要将cv7的预测值cv7p，转换为r7usd的预测值r7usdp，即cv1==0 & cv7p==0的用户的r7usdp是cvMapDf7_0.csv 中cv==0的(min_event_revenue+max_event_revenue)/2。
# 最后计算测试集数据按照installDate进行汇总后的r7usd与r7usdp的MAPE与R2作为验证指标。

# 代码要求，适度的注释，代码简洁，可读性强，可复用性强，可扩展性强，可维护性强。

# 对代码提出新需求：
# 1、需要分别计算测试集和训练集的MAPE与R2，注意要先将数据groupby installDate，r7usd & r7usdp sum后进行计算
# 2、对可能需要调整的超参数进行枚举，并在超参数实践过程中记录日志，记录在'/src/data/dnnGpt.log'中,格式可读性好
# 3、对每个超参数的枚举过程中，需要记录每个超参数的最优值，最优值对应的MAPE与R2，最优值对应的模型参数，最终进行输出

# 可能会随着结果，我会不断的修改这套代码，为了区分不同的代码训练过程，请给每次代码修改预留一个message字段，并在里面对这次代码修改进行摘要。然后将这个message字段记录在'/src/data/dnnGpt.log'中，格式可读性好也记录到日志中。
# 上面日志格式也进行修改，改为csv,列分别为：训练集MAPE1,R2,测试集MAPE，R2，超参数（记录成json string），message

import pandas as pd
import numpy as np
import json
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_percentage_error, r2_score
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

# 定义超参数网格
param_grid = {
    'epochs': [10, 20],
    'batch_size': [32, 64],
    'hidden_layers': [1, 2],
    'hidden_units': [32, 64]
}

# 创建日志文件
log_file = '/src/data/dnnGpt.log'
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

# 初始化日志文件
with open(log_file, 'w') as f:
    f.write('train_mape,train_r2,test_mape,test_r2,params,message\n')

# 加载数据
data_file = '/src/data/dnn1Step2.csv'
df = pd.read_csv(data_file)

# 特征列
feature_columns = ['count', 'countMergeBuilding', 'countMergeArmy', 'countHeroLevelUp', 'countHeroStarUp',
                   'countPayCount', 'countUserLevelMax', 'ENERGY', 'FREE_GOLD', 'MILITARY', 'OILA', 'PAID_GOLD', 'SOIL']

# 修改 message 字段以描述当前代码修改
message = "Initial version with hyperparameter grid search and CSV log format."

# 对每个 cv1 分类训练和预测模型
unique_cv1 = df['cv1'].unique()
for cv1 in unique_cv1:
    print(f"Processing cv1: {cv1}")
    best_params = None
    best_metrics = None
    best_model = None

    # 提取当前 cv1 的数据
    df_cv1 = df[df['cv1'] == cv1]

    # 划分训练集和测试集
    X = df_cv1[feature_columns]
    y = df_cv1['cv7']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

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
        model.fit(X_train, y_train, epochs=param_dict['epochs'], batch_size=param_dict['batch_size'], verbose=2)

        # 预测训练集和测试集
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        y_train_pred_classes = np.argmax(y_train_pred, axis=1)
        y_test_pred_classes = np.argmax(y_test_pred, axis=1)

        # 计算训练集和测试集的 MAPE 和 R2
        train_mape, train_r2 = compute_metrics(y_train, y_train_pred_classes, df_cv1.loc[X_train.index, 'installDate'])
        test_mape, test_r2 = compute_metrics(y_test, y_test_pred_classes, df_cv1.loc[X_test.index, 'installDate'])

        # 记录日志
        log_message = f"{train_mape},{train_r2},{test_mape},{test_r2},{json.dumps(param_dict)},{message}\n"
        with open(log_file, 'a') as f:
            f.write(log_message)

        # 更新最优参数和指标
        if best_metrics is None or test_mape < best_metrics['test_mape']:
            best_params = param_dict
            best_metrics = {'train_mape': train_mape, 'train_r2': train_r2, 'test_mape': test_mape, 'test_r2': test_r2}
            best_model = model

    # 输出最优参数和指标
    print(f"Best params for cv1 {cv1}: {best_params}")
    print(f"Best metrics for cv1 {cv1}: {best_metrics}")

# 为了记录会话信息，下面是GPT的复述
# 1. 使用神经网络进行多分类。数据包含用户行为特征，以及 `cv1` 和 `cv7` 标签。根据用户的 `r1usd` 和 `r7usd` 分布进行分类。
# 2. 对于每个不同的 `cv1` 分类，使用不同的模型（可以是相同的模型，但参数不同，需要单独训练）进行 `cv7` 分类。将分类结果记作 `cv7p` 列。
# 3. 在预测分类后，需要将预测的 `cv7` 值（`cv7p`）转换为预测的 `r7usd` 值（`r7usdp`）。
# 4. 最后，计算测试集数据按照 `installDate` 进行汇总后的 `r7usd` 与 `r7usdp` 的 MAPE 和 R2 作为验证指标。
# 5. 使用超参数网格搜索寻找最佳模型，并记录日志。日志记录在 `/src/data/dnnGpt.log` 中，格式为 CSV，包括训练集 MAPE、R2、测试集 MAPE、R2、超参数（JSON 格式）以及 `message` 字段。
# 6. 对每个超参数的枚举过程中，记录每个超参数的最优值，最优值对应的 MAPE 和 R2，以及最优值对应的模型参数。在搜索结束后，输出这些信息。
# 7. 为每次代码修改预留一个 `message` 字段，并在其中对这次代码修改进行摘要。将这个 `message` 字段记录在日志文件中，以便在不同的会话中区分不同的代码训练过程。
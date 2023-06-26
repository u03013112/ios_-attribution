# 主要改进
# 不在放入任何r1usd或者r3usd作为输入
# 仅用广告信息作为输入
# 约束部分，考虑用cost作为约束

import pandas as pd
import numpy as np
from tensorflow import keras
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow.keras import Input, Model
from tensorflow.keras.layers import Dense, Add, Layer, Concatenate
from tensorflow.keras.callbacks import EarlyStopping

def getXAndY(media_list):
    df = pd.read_csv('/src/data/zk/df_zk2.csv')
    df = df.fillna(0)

    # train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)
    train_df, test_df = train_test_split(df, test_size=0.3)
    if __debug__:
        print('debug 模式下，不做随机，只取前1000行，方便对数')
        train_df = df.head(1000)
        test_df = df.head(1000)

    X_train = train_df.drop(columns=['install_date', 'r7usd'])
    X_train_filled = X_train.fillna(0)
    y_train = train_df['r7usd']

    X_val = test_df.drop(columns=['install_date', 'r7usd'])
    X_val_filled = X_val.fillna(0)
    y_val = test_df['r7usd']

    # 对特征进行标准化
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_filled)
    X_val_scaled = scaler.transform(X_val_filled)

    if __debug__:
        print('debug 模式下，不做标准化，方便对数')
        X_train_scaled = X_train_filled.to_numpy()
        X_val_scaled = X_val_filled.to_numpy()

    feature_list = ['impressions', 'clicks', 'installs', 'cost']

    def get_media_features(X):
        media_inputs = []
        for media in media_list:
            indexList = [X_train.columns.get_loc(f"{media} {feature}") for feature in feature_list]
            media_features = X[:, indexList]
            media_inputs.append(media_features)
        return media_inputs

    X_train_media = get_media_features(X_train_scaled)
    X_val_media = get_media_features(X_val_scaled)

    X_train_filled_cost = train_df[['googleadwords_int cost', 'Facebook Ads cost', 'bytedanceglobal_int cost', 'snapchat_int cost', 'other cost']].values
    X_train_media = np.array(X_train_media)
    X_train_filled_cost = np.array(X_train_filled_cost)
    X_train = [X_train_media[i] for i in range(5)] + [X_train_filled_cost.reshape(-1, 5)]

    X_val_filled_cost = test_df[['googleadwords_int cost', 'Facebook Ads cost', 'bytedanceglobal_int cost', 'snapchat_int cost', 'other cost']].values
    X_val_media = np.array(X_val_media)
    X_val_filled_cost = np.array(X_val_filled_cost)
    X_val = [X_val_media[i] for i in range(5)] + [X_val_filled_cost.reshape(-1, 5)]

    if __debug__:
        for i in range(len(X_train)):
            print(X_train[i][0])

    return X_train, y_train, X_val, y_val

    
from keras import backend as K

class ConstraintPenalty(Layer):
    def call(self, inputs):
        cost = inputs[:, 0]
        output = inputs[:, 1]

        # 认为ROI的范围在2%到15%之间
        lower_bound = 0.02 * cost
        upper_bound = 0.15 * cost

        lower_violation = tf.maximum(0.0, lower_bound - output)
        upper_violation = tf.maximum(0.0, output - upper_bound)

        penalty = 10000 * (lower_violation + upper_violation)

        if penalty is np.nan:
            tf.print('inputs:', inputs)
            tf.print('cost:', cost)
            tf.print('output:', output)
            tf.print('lower_bound:', lower_bound)
            tf.print('upper_bound:', upper_bound)
            tf.print('lower_violation:', lower_violation)
            tf.print('upper_violation:', upper_violation)
            tf.print('penalty:', penalty)

        self.add_loss(tf.reduce_mean(penalty))
        return output

def train():
    media_list = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    X_train, y_train, X_val, y_val = getXAndY(media_list)

    input_layers = [Input(shape=(4,), name=f'{media.replace(" ", "_")}_input') for media in media_list]
    dense_layers = []

    # 添加一个额外的输入层用于r3usd数据
    cost_input = Input(shape=(5,), name='cost_input')
    input_layers.append(cost_input)

    for i, input_layer in enumerate(input_layers[:-1]):  # cost_input
        media_name = media_list[i].replace(" ", "_")
        hidden_layer1 = Dense(8, activation='relu', name=f'{media_name}_hidden1')(input_layer)
        hidden_layer2 = Dense(8, activation='relu', name=f'{media_name}_hidden2')(hidden_layer1)
        hidden_layer3 = Dense(8, activation='relu', name=f'{media_name}_hidden3')(hidden_layer2)
        output = Dense(1, activation='relu', name=f'{media_name}_output')(hidden_layer3)
        dense_layers.append(output)

    # 为每个媒体创建ConstraintPenalty层
    penalty_layers = []
    for i in range(len(media_list)):
        media_name = media_list[i].replace(" ", "_")
        penalty_layer = ConstraintPenalty(name=f'{media_name}_penalty')(Concatenate(name=f'{media_name}_concatenate')([cost_input[:, i:i+1], dense_layers[i]]))
        penalty_layers.append(penalty_layer)

    # 将所有媒体输出添加到一起
    sum_outputs = Add(name='sum_outputs')(penalty_layers)

    model = Model(inputs=input_layers, outputs=sum_outputs)

    # 使用标准MSE损失函数编译模型
    model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])

    model.summary()
    keras.utils.plot_model(model, '/src/data/zk2/model0620.png', show_shapes=True)

    early_stopping = EarlyStopping(patience=10, restore_best_weights=True)

    model.fit(
        X_train,y_train,
        validation_data=(X_val, y_val),
        epochs=3000, batch_size=1, callbacks=[early_stopping]
    )

    model.save('/src/data/zk/model4.h5')

mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    'snapchat_int',
    # 'other'
]

def prepareDataY(mediaList = mediaList):
    df = pd.read_csv('/src/data/zk/androidFp03.csv')
    df = df.rename(columns={'media_source':'media'})
    # 将df中media列中，不在mediaList中的值，替换为'other'
    df.loc[~df['media'].isin(mediaList), 'media'] = 'other'
    rawDf = df.groupby(['install_date', 'media']).agg({'r1usd':'sum','r3usd':'sum','r7usd':'sum'}).reset_index()
    return rawDf

def check():
    df = pd.read_csv('/src/data/zk/df_zk2.csv')
    df = df.fillna(0)
    X = df.drop(columns=['install_date', 'r7usd'])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    media_list = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    feature_list = ['impressions', 'clicks', 'installs', 'cost']
    media_inputs = []

    for media in media_list:
        media_features = X_scaled[:, [X.columns.get_loc(f"{media} {feature}") for feature in feature_list]]
        media_inputs.append(media_features)

    # 加载模型
    model = tf.keras.models.load_model('/src/data/zk/model4.h5', compile=False, custom_objects={'ConstraintPenalty': ConstraintPenalty})

    X_train_filled_cost = df[['googleadwords_int cost', 'Facebook Ads cost', 'bytedanceglobal_int cost', 'snapchat_int cost', 'other cost']].values
    x = [media_inputs[i] for i in range(5)] + [X_train_filled_cost.reshape(-1, 5)]
    

    # 获取每个媒体的预测结果
    media_outputs = []
    for media in media_list:
        media_name = media.replace(" ", "_")
        media_output_layer = model.get_layer(f'{media_name}_output')
        media_model = Model(inputs=model.inputs, outputs=media_output_layer.output)
        media_pred = media_model.predict(x)
        media_outputs.append(media_pred)

    # 将预测结果保存到CSV文件
    output_dfs = []
    for i, media in enumerate(media_list):
        media_df = pd.DataFrame()
        media_df['install_date'] = df['install_date']
        media_df['media'] = media
        media_df['r7usdPred'] = media_outputs[i].flatten()
        output_dfs.append(media_df)

    output_df = pd.concat(output_dfs, ignore_index=True)
    output_df.to_csv('/src/data/zk2/check4.csv', index=False)

    rawDf = prepareDataY()
    rawDf = rawDf[['install_date', 'media', 'r7usd']]
    mergeDf = rawDf.merge(output_df, how='right', on=['install_date', 'media'])

    mergeDf['mape'] = abs(mergeDf['r7usd'] - mergeDf['r7usdPred']) / mergeDf['r7usd']
    mergeDf.to_csv('/src/data/zk2/check4m.csv', index=False)

    mapeDf = mergeDf.groupby('media')['mape'].mean().reset_index()
    print(mapeDf)


import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def draw(df, output_path):
    # 将install_date列转换为日期类型
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 对每个媒体进行绘图
    for media in df['media'].unique():
        media_df = df[df['media'] == media]

        # 创建一个新的图形和轴，设置图形大小为宽 12 英寸，高 6 英寸
        fig, ax1 = plt.subplots(figsize=(16, 6))

        # 绘制r7usd_raw和r7usd_pred曲线
        ax1.plot(media_df['install_date'], media_df['r7usd'], label='r7usd', color='blue')
        ax1.plot(media_df['install_date'], media_df['r7usdPred'], label='r7usdPred', color='green')

        # 设置x轴的刻度
        ax1.xaxis.set_major_locator(mdates.MonthLocator())
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        # 设置第一个y轴的标签
        ax1.set_ylabel('r7usd')

        # 添加图例
        ax1.legend(loc='upper left')

        # 创建一个共享x轴的第二个y轴
        ax2 = ax1.twinx()

        # 绘制mape曲线
        ax2.plot(media_df['install_date'], media_df['mape'], label='mape', linestyle='dashed', color='red')

        # 设置第二个y轴的标签
        ax2.set_ylabel('mape')

        # 添加图例
        ax2.legend(loc='upper right')

        # 设置标题
        plt.title(media)

        # 保存图形到文件
        plt.savefig(f"{output_path}{media}.jpg")

        # 关闭图形，以便在下一个迭代中创建新的图形
        plt.close(fig)
    
def debug1():
    # 尝试找到r7usd/r3usd的有效范围，然后对模型约束进行调整
    df = pd.read_csv('/src/data/zk/df_zk.csv')
    df = df.fillna(0)
    df['r7/cost'] = df['r7usd'] / df['cost']
    mediaList = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    print('真实值的分布')
    for media in mediaList:
        mediaDf = df[df['media'] == media]
        # 打印媒体的r7/r3的 分布
        print(media)
        print('1%:',mediaDf['r7/cost'].quantile(0.01),'99%', mediaDf['r7/cost'].quantile(0.99))
        print('5%:',mediaDf['r7/cost'].quantile(0.05),'95%', mediaDf['r7/cost'].quantile(0.95))
        print('10%:',mediaDf['r7/cost'].quantile(0.1),'90%', mediaDf['r7/cost'].quantile(0.9))
        print('mean:',mediaDf['r7/cost'].mean())

    # # 预测结果的r7/cost 的分布
    # dfP = pd.read_csv('/src/data/zk2/check4m.csv')
    # dfP = dfP[['install_date','media','r7usdPred']]
    # dfRaw = df[['install_date','media','cost']]
    # df = dfRaw.merge(dfP, how='right', on=['install_date','media'])
    # df['r7/cost'] = df['r7usdPred'] / df['cost']
    # print('预测结果的r7/cost 的分布')
    # for media in mediaList:
    #     mediaDf = df[df['media'] == media]
    #     # 打印媒体的r7/r3的 分布
    #     print(media)
    #     print('1%:',mediaDf['r7/cost'].quantile(0.01),'99%', mediaDf['r7/cost'].quantile(0.99))
    #     print('5%:',mediaDf['r7/cost'].quantile(0.05),'95%', mediaDf['r7/cost'].quantile(0.95))
    #     print('10%:',mediaDf['r7/cost'].quantile(0.1),'90%', mediaDf['r7/cost'].quantile(0.9))
    #     print('mean:',mediaDf['r7/cost'].mean())

def draw2(output_path):
    df = pd.read_csv('/src/data/zk/df_zk.csv')
    df = df.fillna(0)
    df['r7/r3'] = df['r7usd'] / df['r3usd']
    df0 = df[['install_date','media','r7/r3']]

    df = pd.read_csv('/src/data/zk/df_zk.csv')
    dfP = pd.read_csv('/src/data/zk2/check4m.csv')
    dfP = dfP[['install_date','media','r7usdPred']]
    dfRaw = df[['install_date','media','r3usd']]
    df2 = dfRaw.merge(dfP, how='right', on=['install_date','media'])
    df2['r7/r3'] = df2['r7usdPred'] / df2['r3usd']
    df2 = df2[['install_date','media','r7/r3']]

    df = df0.merge(df2, how='right', on=['install_date','media'],suffixes=('_raw', '_pred'))

    # 画图有关r7/r3的
    # 将install_date列转换为日期类型
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 对每个媒体进行绘图
    for media in df['media'].unique():
        media_df = df[df['media'] == media]

        # 创建一个新的图形和轴，设置图形大小为宽 12 英寸，高 6 英寸
        fig, ax1 = plt.subplots(figsize=(16, 6))

        # 绘制r7usd_raw和r7usd_pred曲线
        # ax1.plot(media_df['install_date'], media_df['r7usd'], label='r7usd', color='blue')
        # ax1.plot(media_df['install_date'], media_df['r7usdPred'], label='r7usdPred', color='green')
        ax1.plot(media_df['install_date'], media_df['r7/r3_raw'], label='r7/r3_raw', color='blue')
        ax1.plot(media_df['install_date'], media_df['r7/r3_pred'], label='r7/r3_pred', color='green')

        # 设置x轴的刻度
        ax1.xaxis.set_major_locator(mdates.MonthLocator())
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

        # 设置第一个y轴的标签
        ax1.set_ylabel('r7usd')

        # 添加图例
        ax1.legend(loc='upper left')

        # # 创建一个共享x轴的第二个y轴
        # ax2 = ax1.twinx()

        # # 绘制mape曲线
        # ax2.plot(media_df['install_date'], media_df['mape'], label='mape', linestyle='dashed', color='red')

        # # 设置第二个y轴的标签
        # ax2.set_ylabel('mape')

        # # 添加图例
        # ax2.legend(loc='upper right')

        # 设置标题
        plt.title(media)

        # 保存图形到文件
        plt.savefig(f"{output_path}{media}.jpg")

        # 关闭图形，以便在下一个迭代中创建新的图形
        plt.close(fig)
    




if __name__ == '__main__':
    media_list = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    X_train, y_train, X_val, y_val = getXAndY(media_list)
    # # print(X_train.shape, y_train.shape, X_val.shape, y_val.shape)
    # print('len(X_train):',len(X_train))
    # print('X_train[0].shape)',X_train[0].shape)
    # print('X_train[5].shape)',X_train[5].shape)
    # print('len(X_val):',len(X_val))
    # print('X_val[0].shape)',X_val[0].shape)
    # print('X_val[5].shape)',X_val[5].shape)

    # debug1()

    # train()
    # check()
    

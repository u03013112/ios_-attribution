# AndroidMMM2的改进版
# 使用更多的特征
# 之前是只使用融合归因的3日结果，然后直接拟合总和
# 现在尝试加入广告信息
# 然后是考虑加入是否要加入其他的特征，比如星期

import tensorflow as tf
import pandas as pd
import numpy as np
from tensorflow import keras
from keras.constraints import MinMaxNorm, Constraint
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, Add
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from keras.models import load_model

mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    'snapchat_int',
    # 'other'
]

def prepareDataZk(mediaList = mediaList):
    userDf = pd.read_csv('/src/data/zk/attribution1ReStep1.csv')
    # userDf = pd.read_csv('/src/data/zk/attribution1ReStep2.csv')

    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date
    # df列install_timestamp,cv,user_count,r1usd,r7usd,googleadwords_int count,Facebook Ads count,bytedanceglobal_int count,snapchat_int count
    # 新增加一列 'other count'
    userDf['other count'] = 1 - userDf[[media + ' count' for media in mediaList]].sum(axis=1)
    userDf.loc[userDf['other count']<0,'other count'] = 0
    # print(userDf.head(10))
    mediaList.append('other')
    for media in mediaList:
        media_count_col = media + ' count'
        userDf[media + ' r1usd'] = userDf['r1usd'] * userDf[media_count_col]
        userDf[media + ' r3usd'] = userDf['r3usd'] * userDf[media_count_col]
        userDf[media + ' r7usd'] = userDf['r7usd'] * userDf[media_count_col]
    userDf_r1usd = userDf[['install_date'] + [media + ' r1usd' for media in mediaList]]
    userDf_r3usd = userDf[['install_date'] + [media + ' r3usd' for media in mediaList]]
    userDf_r7usd = userDf[['install_date'] + [media + ' r7usd' for media in mediaList]]

    userDf_r1usd = userDf_r1usd.melt(id_vars=['install_date'], var_name='media', value_name='r1usd')
    userDf_r1usd['media'] = userDf_r1usd['media'].str.replace(' r1usd', '')
    userDf_r1usd = userDf_r1usd.groupby(['install_date', 'media']).sum().reset_index()

    userDf_r3usd = userDf_r3usd.melt(id_vars=['install_date'], var_name='media', value_name='r3usd')
    userDf_r3usd['media'] = userDf_r3usd['media'].str.replace(' r3usd', '')
    userDf_r3usd = userDf_r3usd.groupby(['install_date', 'media']).sum().reset_index()

    userDf_r7usd = userDf_r7usd.melt(id_vars=['install_date'], var_name='media', value_name='r7usd')
    userDf_r7usd['media'] = userDf_r7usd['media'].str.replace(' r7usd', '')
    userDf_r7usd = userDf_r7usd.groupby(['install_date', 'media']).sum().reset_index()

    userDf = userDf_r1usd.merge(userDf_r3usd, on=['install_date', 'media'])
    userDf = userDf.merge(userDf_r7usd, on=['install_date', 'media'])
    userDf.to_csv('/src/data/zk/userDf_zk.csv', index=False )
    return userDf


def prepareDataAd(mediaList = mediaList):
    adData = pd.read_csv('/src/data/customLayer/adData20220101_20230331.csv')
    adData.loc[adData['media'] == 'FacebookAds','media'] = 'Facebook Ads'
    return adData

def prepareDataY(mediaList = mediaList):
    df = pd.read_csv('/src/data/zk/androidFp03.csv')
    df = df.rename(columns={'media_source':'media'})
    # 将df中media列中，不在mediaList中的值，替换为'other'
    df.loc[~df['media'].isin(mediaList), 'media'] = 'other'
    rawDf = df.groupby(['install_date', 'media']).agg({'r1usd':'sum','r3usd':'sum','r7usd':'sum'}).reset_index()
    return rawDf

def prepareData(mediaList = mediaList):
    # userDf = prepareDataZk(mediaList)
    # userDf = userDf[['install_date', 'media', 'r3usd']]
    # userDf.to_csv('/src/data/zk/prepareData1.csv', index=False)

    # adData = prepareDataAd(mediaList)
    # adData.to_csv('/src/data/zk/prepareData2.csv', index=False)

    # rawDf = prepareDataY(mediaList)
    # rawDf = rawDf[['install_date', 'media', 'r7usd']]
    # rawDf.to_csv('/src/data/zk/prepareData3.csv', index=False)

    # print(rawDf.dtypes)
    # print(rawDf.head(10))

    userDf = pd.read_csv('/src/data/zk/prepareData1.csv')
    adData = pd.read_csv('/src/data/zk/prepareData2.csv')
    rawDf = pd.read_csv('/src/data/zk/prepareData3.csv')

    df = rawDf.merge(userDf, how='left', on=['install_date', 'media'])
    df = df.merge(adData, how='left', on=['install_date', 'media'])
    df = df.sort_values(by=['media','install_date']).reindex()
    df = df.fillna(0)
    df = df[['install_date', 'media', 'r3usd', 'impressions','clicks','installs','cost','r7usd']]
    df.to_csv('/src/data/zk/df_zk.csv', index=False)
    print(df.corr())
    return df

def dateFilter(df):
    df = df.loc[
        (df['install_date'] >= '2023-02-01') &
        (df['install_date'] <= '2023-03-01')
    ]

    return df

def prepareData2(mediaList = mediaList):
    df = pd.read_csv('/src/data/zk/df_zk.csv')
    df = dateFilter(df)

    # df拥有列：install_date,media,r3usd,impressions,clicks,installs,cost,r7usd
    # 数据整理，按照mediaList中的media顺序，进行按媒体的特征展开
    # 特征列：r3usd,impressions,clicks,installs,cost
    
    mergeDf = df[['install_date']].copy()
    mergeDf = mergeDf.drop_duplicates()
    mergeDf = mergeDf.sort_values(by=['install_date']).reset_index(drop=True)

    mediaList.append('other')
    for media in mediaList:
        mediaDf = df[df['media'] == media].copy()
        mediaDf = mediaDf[['install_date', 'r3usd', 'impressions', 'clicks', 'installs', 'cost']]
        # 除了install_date，其他列改名为 media + 列名
        mediaDf = mediaDf.rename(columns={
            'r3usd': media + ' r3usd',
            'impressions': media + ' impressions',
            'clicks': media + ' clicks',
            'installs': media + ' installs',
            'cost': media + ' cost'
        })
        mergeDf = mergeDf.merge(mediaDf, how='left', on=['install_date'])

    r7usdSumDf = df.groupby('install_date')['r7usd'].sum().reset_index()
    mergeDf = mergeDf.merge(r7usdSumDf, how='left', on=['install_date'])

    print(mergeDf.head(10))
    mergeDf.to_csv('/src/data/zk/df_zk2.csv', index=False)


def train():
    df = pd.read_csv('/src/data/zk/df_zk2.csv')
    # 首先将整个数据集随机划分为训练集和测试集
    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)

    # 分别处理训练集和测试集的特征和标签
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

    # 划分特征为各个媒体
    media_list = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    feature_list = ['r3usd', 'impressions', 'clicks', 'installs', 'cost']

    def get_media_features(X):
        media_inputs = []
        for media in media_list:
            media_features = X[:, [X_train.columns.get_loc(f"{media} {feature}") for feature in feature_list]]
            media_inputs.append(media_features)
        return media_inputs

    X_train_media = get_media_features(X_train_scaled)
    X_val_media = get_media_features(X_val_scaled)

    # 构建神经网络模型
    input_layers = [Input(shape=(5,)) for _ in media_list]

    # 添加3个隐藏层，每层8个神经元，激活函数为ReLU
    dense_layers = []
    for input_layer in input_layers:
        hidden_layer1 = Dense(8, activation='relu')(input_layer)
        hidden_layer2 = Dense(8, activation='relu')(hidden_layer1)
        hidden_layer3 = Dense(8, activation='relu')(hidden_layer2)
        output = Dense(1, activation='linear')(hidden_layer3)
        dense_layers.append(output)

    output_layer = Add()(dense_layers)

    model = Model(inputs=input_layers, outputs=output_layer)

    # 编译模型
    model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])

    keras.utils.plot_model(model, '/src/data/zk2/model4.png', show_shapes=True)

    # 设置Early Stopping
    early_stopping = EarlyStopping(patience=10, restore_best_weights=True)

    # 训练模型
    model.fit(X_train_media, y_train, validation_data=(X_val_media, y_val), epochs=3000, batch_size=32, callbacks=[early_stopping])

    # 保存模型
    model.save('/src/data/zk/model4.h5')
    
def check():
    df = pd.read_csv('/src/data/zk/df_zk2.csv')
    # 数据处理部分与train()函数相同
    # 然后加载模型
    # 模型加载后，使用模型对所有数据进行预测
    # 这里的要点不是预测最终结果，而是对每个媒体的预测，即最终Add之前的5个结论
    # 将结论用存一个csv，列 install_data，media，r7usdPred
    # 保存到'/src/data/zk2/check4.csv'
    X = df.drop(columns=['install_date', 'r7usd'])
    X_filled = X.fillna(0)
    y = df['r7usd']

    # 对特征进行标准化
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_filled)

    # 划分特征为各个媒体
    media_list = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    feature_list = ['r3usd', 'impressions', 'clicks', 'installs', 'cost']
    media_inputs = []

    for media in media_list:
        media_features = X_scaled[:, [X.columns.get_loc(f"{media} {feature}") for feature in feature_list]]
        media_inputs.append(media_features)

    # 加载模型
    model = load_model('/src/data/zk/model4.h5')

    # 获取每个媒体的预测结果
    media_outputs = []
    for i in range(len(media_list)):
        media_model = Model(inputs=model.inputs, outputs=model.layers[-1].input[i])
        media_pred = media_model.predict(media_inputs)
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

    rawDf = prepareDataY(mediaList)
    rawDf = rawDf[['install_date', 'media', 'r7usd']]

    mergeDf = rawDf.merge(output_df, how='right', on=['install_date', 'media'])

    mergeDf['mape'] = abs(mergeDf['r7usd'] - mergeDf['r7usdPred']) / mergeDf['r7usd']
    mergeDf.to_csv('/src/data/zk2/check4m.csv', index=False)

    # 分媒体计算MAPE
    mapeDf = mergeDf.groupby('media')['mape'].mean().reset_index()
    print(mapeDf)




if __name__ == '__main__':
    # prepareData()
    # train()
    check()
# 尝试将原来的数据与模型进行简化
# 原本是5个媒体（算上other），每个媒体有5个特征
# 简化成2个媒体，Google与other，即将Facebook、bytedanceglobal、snapchat合并到other中
# 特征也按照2个媒体进行合并
# 确认如此方案是否可以更加准确的预测Google的r7usd
# 初步得到结论，只分两个媒体，并没有提高预测效果


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

    # 合并Facebook Ads、bytedanceglobal_int、snapchat_int到other中
    df['other r3usd'] += df['Facebook Ads r3usd'] + df['bytedanceglobal_int r3usd'] + df['snapchat_int r3usd']
    df['other impressions'] += df['Facebook Ads impressions'] + df['bytedanceglobal_int impressions'] + df['snapchat_int impressions']
    df['other clicks'] += df['Facebook Ads clicks'] + df['bytedanceglobal_int clicks'] + df['snapchat_int clicks']
    df['other installs'] += df['Facebook Ads installs'] + df['bytedanceglobal_int installs'] + df['snapchat_int installs']
    df['other cost'] += df['Facebook Ads cost'] + df['bytedanceglobal_int cost'] + df['snapchat_int cost']

    # 删除Facebook Ads、bytedanceglobal_int、snapchat_int的列
    df = df.drop(columns=['Facebook Ads r3usd', 'Facebook Ads impressions', 'Facebook Ads clicks', 'Facebook Ads installs', 'Facebook Ads cost',
                           'bytedanceglobal_int r3usd', 'bytedanceglobal_int impressions', 'bytedanceglobal_int clicks', 'bytedanceglobal_int installs', 'bytedanceglobal_int cost',
                           'snapchat_int r3usd', 'snapchat_int impressions', 'snapchat_int clicks', 'snapchat_int installs', 'snapchat_int cost'])

    train_df, test_df = train_test_split(df, test_size=0.3)

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

    feature_list = ['r3usd', 'impressions', 'clicks', 'installs', 'cost']

    def get_media_features(X):
        media_inputs = []
        for media in media_list:
            media_features = X[:, [X_train.columns.get_loc(f"{media} {feature}") for feature in feature_list]]
            media_inputs.append(media_features)
        return media_inputs

    X_train_media = get_media_features(X_train_scaled)
    X_val_media = get_media_features(X_val_scaled)

    return X_train_media, y_train, X_val_media, y_val

def train():
    media_list = ['googleadwords_int', 'other']

    X_train, y_train, X_val, y_val = getXAndY(media_list)

    input_layers = [Input(shape=(5,), name=f'{media.replace(" ", "_")}_input') for media in media_list]
    dense_layers = []

    for i, input_layer in enumerate(input_layers):
        media_name = media_list[i].replace(" ", "_")
        hidden_layer1 = Dense(8, activation='relu', name=f'{media_name}_hidden1')(input_layer)
        hidden_layer2 = Dense(8, activation='relu', name=f'{media_name}_hidden2')(hidden_layer1)
        hidden_layer3 = Dense(8, activation='relu', name=f'{media_name}_hidden3')(hidden_layer2)
        output = Dense(1, activation='relu', name=f'{media_name}_output')(hidden_layer3)
        dense_layers.append(output)

    sum_outputs = Add(name='sum_outputs')(dense_layers)

    model = Model(inputs=input_layers, outputs=sum_outputs)

    model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])

    model.summary()
    keras.utils.plot_model(model, '/src/data/customLayer/model0612.png', show_shapes=True)

    early_stopping = EarlyStopping(patience=10, restore_best_weights=True)

    model.fit(X_train, y_train,
              validation_data=(X_val, y_val),
              epochs=3000, batch_size=1, callbacks=[early_stopping])

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
    media_list = ['googleadwords_int', 'other']

    df = pd.read_csv('/src/data/zk/df_zk2.csv')
    df = df.fillna(0)
    X = df.drop(columns=['install_date', 'r7usd'])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    media_inputs = []
    feature_list = ['r3usd', 'impressions', 'clicks', 'installs', 'cost']

    for media in media_list:
        media_features = X_scaled[:, [X.columns.get_loc(f"{media} {feature}") for feature in feature_list]]
        media_inputs.append(media_features)

    # 加载模型
    model = tf.keras.models.load_model('/src/data/zk/model4.h5', compile=False)

    # 获取每个媒体的预测结果
    media_outputs = []
    for media in media_list:
        media_name = media.replace(" ", "_")
        media_output_layer = model.get_layer(f'{media_name}_output')
        media_model = Model(inputs=model.inputs, outputs=media_output_layer.output)
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

    rawDf = prepareDataY()
    rawDf = rawDf[['install_date', 'media', 'r7usd']]
    mergeDf = rawDf.merge(output_df, how='right', on=['install_date', 'media'])

    mergeDf['mape'] = abs(mergeDf['r7usd'] - mergeDf['r7usdPred']) / mergeDf['r7usd']
    mergeDf.to_csv('/src/data/zk2/check4m.csv', index=False)

    mapeDf = mergeDf.groupby('media')['mape'].mean().reset_index()
    print(mapeDf)

if __name__ == '__main__':
    # train()
    check()

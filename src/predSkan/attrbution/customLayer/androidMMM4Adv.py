# 我决定用自定义loss函数的方法
# 帮我重写一个train和check方法
# 重新建立模型
# 输入不变 input_layers = [Input(shape=(5,)) for _ in media_list]
# 输出不再使用Add层，直接就每个媒体一个输出
# 自定义loss，所有媒体输出的和 与 r7usd 的mse
# 额外的，添加loss惩罚
# 要求loss传入模型为参数，读取模型中的，然后判断各媒体的输出是否在r3usd的0.8倍~3.5倍的范围内
# 如果不在，惩罚loss，惩罚值为 超出部分 * 1000，比如 r3usd = 1，有效范围是0.8~3.5，那么如果输出是0.7，惩罚值就是 0.1 * 1000 = 100

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

    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)

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

    # 从X_train_filled中获取各媒体的r3usd，组成数组
    X_train_filled_r3usd = train_df[['googleadwords_int r3usd', 'Facebook Ads r3usd', 'bytedanceglobal_int r3usd', 'snapchat_int r3usd', 'other r3usd']].values
    X_train_media = np.array(X_train_media)
    X_train_filled_r3usd = np.array(X_train_filled_r3usd)
    # 将两个np.array合并成一个,X_train_media.shape (5, 318, 5), X_train_filled_r3usd.shape (318, 5)，希望合并成 (6, 318, 5)
    X_train = np.concatenate((X_train_media, X_train_filled_r3usd.reshape(1, -1, 5)), axis=0)

    X_val_filled_r3usd = test_df[['googleadwords_int r3usd', 'Facebook Ads r3usd', 'bytedanceglobal_int r3usd', 'snapchat_int r3usd', 'other r3usd']].values
    X_val_media = np.array(X_val_media)
    X_val_filled_r3usd = np.array(X_val_filled_r3usd)
    X_val = np.concatenate((X_val_media, X_val_filled_r3usd.reshape(1, -1, 5)), axis=0)

    return X_train, y_train, X_val, y_val

    
from keras import backend as K

class ConstraintPenalty(Layer):
    def call(self, inputs):
        r3usd = inputs[:, 0]
        output = inputs[:, 1]

        lower_bound = 0.8 * r3usd
        upper_bound = 2.5 * r3usd

        lower_violation = tf.maximum(0.0, lower_bound - output)
        upper_violation = tf.maximum(0.0, output - upper_bound)

        penalty = 1000 * (lower_violation + upper_violation)

        if penalty is np.nan:
            tf.print('inputs:', inputs)
            tf.print('r3usd:', r3usd)
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

    input_layers = [Input(shape=(5,), name=f'{media.replace(" ", "_")}_input') for media in media_list]
    dense_layers = []

    # 添加一个额外的输入层用于r3usd数据
    r3usd_input = Input(shape=(5,), name='r3usd_input')
    input_layers.append(r3usd_input)

    for i, input_layer in enumerate(input_layers[:-1]):  # 不包括r3usd_input
        media_name = media_list[i].replace(" ", "_")
        hidden_layer1 = Dense(8, activation='relu', name=f'{media_name}_hidden1')(input_layer)
        hidden_layer2 = Dense(8, activation='relu', name=f'{media_name}_hidden2')(hidden_layer1)
        hidden_layer3 = Dense(8, activation='relu', name=f'{media_name}_hidden3')(hidden_layer2)
        output = Dense(1, activation='linear', name=f'{media_name}_output')(hidden_layer3)
        dense_layers.append(output)

    # 为每个媒体创建ConstraintPenalty层
    penalty_layers = []
    for i in range(len(media_list)):
        media_name = media_list[i].replace(" ", "_")
        penalty_layer = ConstraintPenalty(name=f'{media_name}_penalty')(Concatenate(name=f'{media_name}_concatenate')([r3usd_input[:, i:i+1], dense_layers[i]]))
        penalty_layers.append(penalty_layer)

    # 将所有媒体输出添加到一起
    sum_outputs = Add(name='sum_outputs')(penalty_layers)

    model = Model(inputs=input_layers, outputs=sum_outputs)

    # 使用标准MSE损失函数编译模型
    model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])

    model.summary()
    keras.utils.plot_model(model, '/src/data/customLayer/model0612.png', show_shapes=True)

    early_stopping = EarlyStopping(patience=10, restore_best_weights=True)

    model.fit([X_train[i] for i in range(6)], y_train,
          validation_data=([X_val[i] for i in range(6)], y_val),
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
    df = pd.read_csv('/src/data/zk/df_zk2.csv')
    df = df.fillna(0)
    X = df.drop(columns=['install_date', 'r7usd'])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    media_list = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    feature_list = ['r3usd', 'impressions', 'clicks', 'installs', 'cost']
    media_inputs = []

    for media in media_list:
        media_features = X_scaled[:, [X.columns.get_loc(f"{media} {feature}") for feature in feature_list]]
        media_inputs.append(media_features)

    # 加载模型
    model = tf.keras.models.load_model('/src/data/zk/model4.h5', compile=False, custom_objects={'ConstraintPenalty': ConstraintPenalty})

    # 添加一个额外的输入层用于r3usd数据
    r3usd_input = df[['googleadwords_int r3usd', 'Facebook Ads r3usd', 'bytedanceglobal_int r3usd', 'snapchat_int r3usd', 'other r3usd']].values
    x = np.concatenate((media_inputs,r3usd_input.reshape(1, -1, 5)), axis=0)
    # print(x.shape)

    # p = model.predict([x[i] for i in range(6)])
    # print('AAA:',p.shape)

    # 获取每个媒体的预测结果
    media_outputs = []
    for media in media_list:
        media_name = media.replace(" ", "_")
        media_output_layer = model.get_layer(f'{media_name}_output')
        media_model = Model(inputs=model.inputs, outputs=media_output_layer.output)
        media_pred = media_model.predict([x[i] for i in range(6)])
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
    # getXAndY()
    train()
    check()

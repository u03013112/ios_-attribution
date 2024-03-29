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

    # train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)
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
        upper_bound = 2.0 * r3usd

        lower_violation = tf.maximum(0.0, lower_bound - output)
        upper_violation = tf.maximum(0.0, output - upper_bound)

        penalty = 10000 * (lower_violation + upper_violation)

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
        output = Dense(1, activation='relu', name=f'{media_name}_output')(hidden_layer3)
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

from sklearn.metrics import mean_absolute_percentage_error,r2_score
def check2():
    # 与check()方法类似
    # 额外计算R2
    # 计算7日均线，7日的移动均线，用于计算MAPE，R2
    df = pd.read_csv('/src/data/zk2/check4m.csv')

    day = 14

    for media in mediaList:
        print(media)
        mediaDf = df[df['media'] == media].copy()
        # 计算7日均线
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(day).mean()
        mediaDf['r7usdPred7'] = mediaDf['r7usdPred'].rolling(day).mean()
        # 计算7日ewm均线
        mediaDf['r7usd7ewm'] = mediaDf['r7usd'].ewm(span=day).mean()
        mediaDf['r7usdPred7ewm'] = mediaDf['r7usdPred'].ewm(span=day).mean()

        # 计算MAPE与R2
        try:
            mape = mean_absolute_percentage_error(mediaDf['r7usd'], mediaDf['r7usdPred'])
            r2 = r2_score(mediaDf['r7usd'], mediaDf['r7usdPred'])
        except:
            pass
        else:
            print('MAPE:', mape)
            print('R2:', r2)

        try:
            mediaDf = mediaDf.loc[mediaDf['r7usd7'] > 0]
            mape7 = mean_absolute_percentage_error(mediaDf['r7usd7'], mediaDf['r7usdPred7'])
            r27 = r2_score(mediaDf['r7usd7'], mediaDf['r7usdPred7'])

            mape7ewm = mean_absolute_percentage_error(mediaDf['r7usd7ewm'], mediaDf['r7usdPred7ewm'])
            r27ewm = r2_score(mediaDf['r7usd7ewm'], mediaDf['r7usdPred7ewm'])
        except:
            pass
        else:
            print('MAPE7:', mape7)
            print('R27:', r27)
            print('MAPE7ewm:', mape7ewm)
            print('R27ewm:', r27ewm)


def draw3():
    df = pd.read_csv('/src/data/zk2/check4m.csv')
    df = df[['install_date', 'media', 'r7usd', 'r7usdPred']]

    df2 = pd.read_csv('/src/data/zk/df_zk2.csv')
    df2 = df2[['install_date','googleadwords_int cost','Facebook Ads cost','bytedanceglobal_int cost','snapchat_int cost','other cost']]

    # 将df2改成列 'install_date','media','cost'
    df2 = df2.melt(id_vars=['install_date'], var_name='media', value_name='cost')
    df2['media'] = df2['media'].str.replace(' cost', '')
    
    df = df.merge(df2, how='left', on=['install_date', 'media'])
    df = df.fillna(0)

    df['roi'] = df['r7usd'] / df['cost']
    df['roiPred'] = df['r7usdPred'] / df['cost']

    # 画图
    # 将install_date列转换为日期类型，每月一个刻度即可
    # 图宽一些
    # 纵向画3张小图，x坐标对其
    # 第1张
    # 画roi，7日roi均线，7日roi ewm均线，用3个颜色
    # 第2张
    # 分媒体画7日roi ewm均线，每个媒体一个颜色，半透明
    # 分媒体画7日预测roi ewm均线，每个媒体一个颜色
    # 第3张
    # 分媒体画cost，画累计图，即要表现每个媒体花费占比
    # 保存图片为'/src/data/zk2/draw3.png'
    df['install_date'] = pd.to_datetime(df['install_date'])

    # Create a new DataFrame for grouped data without changing the index of the original DataFrame
    df_grouped = df.groupby('install_date').sum().reset_index()
    df_grouped['roi'] = df_grouped['r7usd'] / df_grouped['cost']

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    ax1.plot(df_grouped['install_date'], df_grouped['roi'], label='ROI', alpha=0.5)
    ax1.plot(df_grouped['install_date'], df_grouped['roi'].rolling(window=7).mean(), label='7-day ROI Mean')
    ax1.plot(df_grouped['install_date'], df_grouped['roi'].ewm(span=7).mean(), label='7-day ROI Exponential Mean')
    ax1.legend()
    ax1.set_title('ROI, 7-day Mean and Exponential Mean for Grouped Data')

    media_list = df['media'].unique()
    colors = plt.cm.get_cmap('tab10', len(media_list))

    for i, media in enumerate(media_list):
        media_df = df[df['media'] == media]
        ax2.plot(media_df['install_date'], media_df['roi'].ewm(span=7).mean(), label=media + ' ROI EWM', color=colors(i), alpha=0.5)
        ax2.plot(media_df['install_date'], media_df['roiPred'].ewm(span=7).mean(), label=media + ' Pred ROI EWM', color=colors(i))

    ax2.legend()
    ax2.set_title('7-day Exponential Mean ROI and Pred ROI by Media')

    mediaList = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int',
        'snapchat_int',
        'other'
    ]
    
    dfTmp = df.loc[df['media'] == 'googleadwords_int'].copy()
    dfTmp = dfTmp[['install_date', 'cost']]
    dfTmp['cost'] = 0
    for media in mediaList:
        dfTmp['cost'] += df.loc[df['media'] == media, 'cost'].values
        ax3.plot(dfTmp['install_date'], dfTmp['cost'], label=media)

    ax3.legend()
    ax3.set_title('cost')

    fig.tight_layout()
    plt.savefig('/src/data/zk2/draw3.png')

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
    df['r7/r3'] = df['r7usd'] / df['r3usd']
    mediaList = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    print('真实值的分布')
    for media in mediaList:
        mediaDf = df[df['media'] == media]
        # 打印媒体的r7/r3的 分布
        print(media)
        print('1%:',mediaDf['r7/r3'].quantile(0.01),'99%', mediaDf['r7/r3'].quantile(0.99))
        print('5%:',mediaDf['r7/r3'].quantile(0.05),'95%', mediaDf['r7/r3'].quantile(0.95))
        print('10%:',mediaDf['r7/r3'].quantile(0.1),'90%', mediaDf['r7/r3'].quantile(0.9))
        print('mean:',mediaDf['r7/r3'].mean())

    # 预测结果的r7/r3 的分布
    dfP = pd.read_csv('/src/data/zk2/check4m.csv')
    dfP = dfP[['install_date','media','r7usdPred']]
    dfRaw = df[['install_date','media','r3usd']]
    df = dfRaw.merge(dfP, how='right', on=['install_date','media'])
    df['r7/r3'] = df['r7usdPred'] / df['r3usd']
    print('预测结果的r7/r3 的分布')
    for media in mediaList:
        mediaDf = df[df['media'] == media]
        # 打印媒体的r7/r3的 分布
        print(media)
        print('1%:',mediaDf['r7/r3'].quantile(0.01),'99%', mediaDf['r7/r3'].quantile(0.99))
        print('5%:',mediaDf['r7/r3'].quantile(0.05),'95%', mediaDf['r7/r3'].quantile(0.95))
        print('10%:',mediaDf['r7/r3'].quantile(0.1),'90%', mediaDf['r7/r3'].quantile(0.9))
        print('mean:',mediaDf['r7/r3'].mean())

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
    # getXAndY()
    # train()
    # check()

    # check2()
    draw3()

    # draw(pd.read_csv('/src/data/zk2/check4m.csv'), '/src/data/zk2/m4a')

    # debug1()
    # draw2('/src/data/zk2/m4a2')

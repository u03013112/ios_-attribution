# 2023年，安卓数据
# 用融合归因3日收入金额做输入
# 针对3个主要媒体，和其他媒体，自然量（也作为一个媒体），每个媒体拟合一个倍率系数
# 用融合归因3日收入金额乘以倍率系数，得到预测的7日收入金额
# 再用所有媒体的7日收入金额，与真实的7日收入金额做标准，计算loss
# 找到每个媒体的最合适的系数
# 并计算每个媒体的MAPE，与融合归因的直接7日结论做对比，看是否有提升

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from tensorflow import keras
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, Add
from tensorflow.keras.callbacks import EarlyStopping
from keras.models import load_model
from keras.callbacks import ModelCheckpoint


from sklearn.model_selection import train_test_split

import os
import sys
sys.path.append('/src')

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

mediaList = [
    'Facebook Ads',
    'bytedanceglobal_int',
    'googleadwords_int',
    'other'
]

def getData():
    if os.path.exists(getFilename('20230908android01')):
        print('读取融合归因结论')
        df = pd.read_csv(getFilename('20230908android01'))
    else:
        print('处理融合归因结论')
        # 拥有列 ：
        # media,install_date,r1usd,r3usd,r7usd,r14usd,r28usd,user_count,r1usdp,r3usdp,r7usdp,r14usdp,r28usdp,count,payCount,MAPE1,MAPE3,MAPE7,MAPE14,MAPE28
        df = pd.read_csv(getFilename('attributionRetCheckGeo24V2'))
        df = df[['media','install_date','r1usd','r3usd','r7usd','r14usd','r28usd','r1usdp','r3usdp','r7usdp','r14usdp','r28usdp']]
        df = df.loc[df['media'].isin(mediaList)]
        df.to_csv(getFilename('20230908android01'),index=False)
        print('处理融合归因结论 完成')

    if os.path.exists(getFilename('20230908android02')):
        print('读取整体量')
        totalDf = pd.read_csv(getFilename('20230908android02'))
    else:
        print('处理整体量')
        # 拥有列 ：
        # uid,install_date,r1usd,r2usd,r3usd,r7usd,r14usd,r28usd,install_timestamp,last_timestamp,media_source,country_code,campaign_id
        totalDf = pd.read_csv(getFilename('androidFp07_28'))
        totalDf = totalDf[['uid','install_date','r1usd','r3usd','r7usd','r14usd','r28usd','media_source']]
        totalDf = totalDf.rename(columns={'media_source':'media'})
        totalDf = totalDf.groupby(['install_date','media']).agg({
            'r1usd':'sum',
            'r3usd':'sum',
            'r7usd':'sum',
            'r14usd':'sum',
            'r28usd':'sum'
        }).reset_index()
        totalDf.to_csv(getFilename('20230908android02'),index=False)
        print('处理整体量 完成')

    if os.path.exists(getFilename('20230908android03')):
        print('读取自然量')
        otherDf = pd.read_csv(getFilename('20230908android03'))
    else:
        print('计算自然量')
        # 计算自然量真实值
        totalGroupDf = totalDf.groupby(['install_date']).agg({
            'r1usd':'sum',
            'r3usd':'sum',
            'r7usd':'sum',
            'r14usd':'sum',
            'r28usd':'sum'
        }).reset_index()
        print(totalGroupDf.head(5))

        otherRealDf2 = df.groupby(['install_date']).agg({
            'r1usd':'sum',
            'r3usd':'sum',
            'r7usd':'sum',
            'r14usd':'sum',
            'r28usd':'sum'
        }).reset_index()
        # print(otherRealDf2.head(5))
        otherRealDf = totalGroupDf.merge(otherRealDf2,how='left',on=['install_date'],suffixes=('_total','_media')).reset_index()
        
        otherRealDf['r1usd'] = otherRealDf['r1usd_total'] - otherRealDf['r1usd_media']
        otherRealDf['r3usd'] = otherRealDf['r3usd_total'] - otherRealDf['r3usd_media']
        otherRealDf['r7usd'] = otherRealDf['r7usd_total'] - otherRealDf['r7usd_media']
        otherRealDf['r14usd'] = otherRealDf['r14usd_total'] - otherRealDf['r14usd_media']
        otherRealDf['r28usd'] = otherRealDf['r28usd_total'] - otherRealDf['r28usd_media']
        # print(otherRealDf[['install_date','r1usd','r1usd_total','r1usd_media']].head(5))
        otherRealDf['media'] = 'other'
        otherRealDf = otherRealDf[['install_date','media','r1usd','r3usd','r7usd','r14usd','r28usd']]

        # 先将df中的融合归因结论进行汇总
        # 再用totalDf中的自然量减去融合归因结论，得到自然量
        otherPredDf2 = df.groupby(['install_date']).agg({
            'r1usdp':'sum',
            'r3usdp':'sum',
            'r7usdp':'sum',
            'r14usdp':'sum',
            'r28usdp':'sum'
        })

        otherPredDf = totalGroupDf.merge(otherPredDf2,how='left',on=['install_date']).reset_index()
        otherPredDf['r1usdp'] = otherPredDf['r1usd'] - otherPredDf['r1usdp']
        otherPredDf['r3usdp'] = otherPredDf['r3usd'] - otherPredDf['r3usdp']
        otherPredDf['r7usdp'] = otherPredDf['r7usd'] - otherPredDf['r7usdp']
        otherPredDf['r14usdp'] = otherPredDf['r14usd'] - otherPredDf['r14usdp']
        otherPredDf['r28usdp'] = otherPredDf['r28usd'] - otherPredDf['r28usdp']
        otherPredDf = otherPredDf[['install_date','r1usdp','r3usdp','r7usdp','r14usdp','r28usdp']]

        otherDf = otherRealDf.merge(otherPredDf,how='left',on=['install_date']).reset_index()

        otherDf.to_csv(getFilename('20230908android03'),index=False)
        print('计算自然量 完成')

    # 将自然量和融合归因结论合并
    df = df.append(otherDf,ignore_index=True).reset_index(drop=True)
    # index是哪里来了没搞明白，先不考虑
    df.drop(columns=['index'],inplace=True)
    df.to_csv(getFilename('20230908android04'),index=False)
    for media in mediaList:
        mediaDf = df[df['media']==media]
        print(media)
        # print(mediaDf[['install_date','r1usd','r1usdp']].head(5))
        print(mediaDf.corr())
    
def getXY():
    df = pd.read_csv(getFilename('20230908android04'))
    # 拥有列： media,install_date,r1usd,r3usd,r7usd,r14usd,r28usd,r1usdp,r3usdp,r7usdp,r14usdp,r28usdp
    # 先按照install_date，media排序
    # 其中media列中，有4个值：Facebook Ads,bytedanceglobal_int,googleadwords_int,other
    # 就按mediaList的顺序，4个媒体的r3usdp，组成x
    # 用大盘的r7usd做y。即按照install_date汇总，计算r7usd的和，作为y
    # 返回x,y,y0
    # 其中y0是为了最后check用的，y0是大盘的r7usd，y是4个媒体的r7usd
    df = df.sort_values(by=['install_date','media']).reset_index(drop=True)

    # mediaList = ['Facebook Ads', 'bytedanceglobal_int', 'googleadwords_int', 'other']
    x = df[df['media'].isin(mediaList)].groupby(['install_date', 'media'])['r3usdp'].sum().unstack()[mediaList].values
    y0 = df[df['media'].isin(mediaList)].groupby(['install_date', 'media'])['r7usd'].sum().unstack()[mediaList].values
    y = df.groupby('install_date')['r7usd'].sum().values

    return x, y, y0

# 创建模型，模型分为4个，分别对应4个媒体
# 每个模型都是神经网络模型
# 每个模型输入是1个特征，即r3usdp
# 每个模型都只有一个隐藏层，隐藏层的神经元个数为1，并且只有w，没有b
# 每个模型的输出是1个值，即r7usd
# 最后将4个模型的输出相加，得到最终的预测值
from tensorflow.keras.constraints import Constraint
from keras import backend as K
class CustomWeightConstraint(Constraint):
    def __init__(self, min_value, max_value):
        self.min_value = min_value
        self.max_value = max_value

    def __call__(self, w):
        return K.clip(w, self.min_value, self.max_value)

    def get_config(self):
        return {'min_value': self.min_value, 'max_value': self.max_value}

def createModV1():
    # 创建模型
    # 创建模型，模型分为4个，分别对应4个媒体
    input_layers = []
    hidden_layers = []
    for media in mediaList:
        mediaName = media.replace(' ','_')
        input_layer = Input(shape=(1,), name=f'{mediaName}_input')
        hidden_layer = Dense(1, activation='linear', 
                             use_bias=False, 
                            #  kernel_constraint=MinMaxNorm(min_value=1.0, max_value=3.0),
                            kernel_constraint=CustomWeightConstraint(min_value=1, max_value=3),
                             name=f'{mediaName}_hidden'
                            )(input_layer)
        input_layers.append(input_layer)
        hidden_layers.append(hidden_layer)

    # 将4个模型的输出相加，得到最终的预测值
    output_layer = Add(name='sum_outputs')(hidden_layers)

    # 构建模型
    model = Model(inputs=input_layers, outputs=output_layer)

    # 编译模型
    model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])

    keras.utils.plot_model(model, '/src/data/zk2/model20230908v1.png', show_shapes=True)

    return model

def printW(model):
    # 获取每个媒体的权重
    mediaList = ['Facebook Ads', 'bytedanceglobal_int', 'googleadwords_int', 'other']
    for media in mediaList:
        mediaName = media.replace(' ','_')
        layer = model.get_layer(f'{mediaName}_hidden')
        weights = layer.get_weights()
        constraint = layer.kernel_constraint
        constrained_weights = constraint(weights[0])
        print(f'权重 for {media}: {constrained_weights.numpy()}')

def train(x,y):
    # 将x，y划分为训练集和测试集，比例为7:3
    x_train, x_test, y_train, y_test = train_test_split(x,y,test_size=0.3,random_state=42)
    model = createModV1()

    # 设置Early Stopping
    # early_stopping = EarlyStopping(patience=10, restore_best_weights=True)

    model_checkpoint = ModelCheckpoint('/src/data/zk2/model20230908v1.h5', monitor='val_loss', mode='min', save_best_only=True)

    # 训练模型
    model.fit(
        [x_train[:, i] for i in range(4)], y_train
        , validation_data=([x_test[:, i] for i in range(4)], y_test)
        , epochs=3000
        , batch_size=32
        , callbacks=[model_checkpoint]
    )
    
# 融合归因原始结论，作为对照组
def controlGroup():
    print('融合归因原始结论，作为对照组:')
    df = pd.read_csv(getFilename('20230908android04'))
    df = df[['install_date','media','r7usd','r7usdp']]
    df = df.sort_values(by=['install_date','media']).reset_index(drop=True)
    print(df.head(5))


    for media in mediaList:
        mediaDf = df[df['media']==media].copy()
        print(media)
        mediaDf['MAPE7'] = np.abs((mediaDf['r7usd'] - mediaDf['r7usdp']) / mediaDf['r7usd'])

        mediaDf = mediaDf.fillna(0)
        mediaDf.loc[mediaDf['MAPE7']==np.inf,'MAPE7'] = 0
        print('MAPE7: ', mediaDf.loc[mediaDf['MAPE7']>0]['MAPE7'].mean())

        mediaDf['r7usd rolling7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp rolling7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7 rolling7'] = np.abs((mediaDf['r7usd rolling7'] - mediaDf['r7usdp rolling7']) / mediaDf['r7usd rolling7'])
        print('MAPE rolling7: ', mediaDf['MAPE7 rolling7'].mean())

        mediaDf['install_date'] = pd.to_datetime(mediaDf['install_date'])
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(mediaDf['install_date'], mediaDf['r7usd'], label='r7usd',alpha=0.5)
        ax.plot(mediaDf['install_date'], mediaDf['r7usdp'], label='r7usdp',alpha=0.5)
        ax.plot(mediaDf['install_date'], mediaDf['r7usd rolling7'], label='r7usd rolling7')
        ax.plot(mediaDf['install_date'], mediaDf['r7usdp rolling7'], label='r7usdp rolling7')
        # ax.plot(mediaDf['install_date'], mediaDf['r7usd/r3usdp ewm7'], label='r7usd/r3usdp ewm7')
        # ax.plot(mediaDf['install_date'], mediaDf['r14usd/r3usdp ewm7'], label='r14usd/r3usdp ewm7')
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))  # 设置每7天显示一个日期
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45)
        plt.xlabel('Install Date')
        plt.ylabel('Values')
        plt.title(f'{media} - real r7usd vs self attr r7usd')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f'/src/data/zk2/20230908_cg_{media}.jpg')
        plt.close()

def check(model, x, y0):
    # 对每个媒体，将其他媒体的输入设置为0，然后使用模型进行预测
    for i, media in enumerate(mediaList):
        print(media)
        y_pred = model.predict([x[:, j] if j == i else np.zeros_like(x[:, j]) for j in range(4)])

        # installDateList 是从 2023-01-01开始的string类型的日期列表
        # list长度与y_pred长度相同
        installDateList = pd.date_range(start='2023-01-01', periods=len(y_pred)).strftime('%Y-%m-%d').tolist()

        media_result = pd.DataFrame({
            'install_date': installDateList,
            'r7usd': y0[:, i].reshape(-1),
            'r7usdp': y_pred.reshape(-1),
        })
        media_result['media'] = media
        media_result['MAPE7'] = np.abs((media_result['r7usd'] - media_result['r7usdp']) / media_result['r7usd'])

        mediaDf = media_result
        mediaDf = mediaDf.fillna(0)
        mediaDf.loc[mediaDf['MAPE7']==np.inf,'MAPE7'] = 0
        print('MAPE7: ', mediaDf.loc[mediaDf['MAPE7']>0]['MAPE7'].mean())

        mediaDf['r7usd rolling7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp rolling7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7 rolling7'] = np.abs((mediaDf['r7usd rolling7'] - mediaDf['r7usdp rolling7']) / mediaDf['r7usd rolling7'])
        print('MAPE rolling7: ', mediaDf['MAPE7 rolling7'].mean())

        mediaDf['install_date'] = pd.to_datetime(mediaDf['install_date'])
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(mediaDf['install_date'], mediaDf['r7usd'], label='r7usd',alpha=0.5)
        ax.plot(mediaDf['install_date'], mediaDf['r7usdp'], label='r7usdp',alpha=0.5)
        ax.plot(mediaDf['install_date'], mediaDf['r7usd rolling7'], label='r7usd rolling7')
        ax.plot(mediaDf['install_date'], mediaDf['r7usdp rolling7'], label='r7usdp rolling7')
        # ax.plot(mediaDf['install_date'], mediaDf['r7usd/r3usdp ewm7'], label='r7usd/r3usdp ewm7')
        # ax.plot(mediaDf['install_date'], mediaDf['r14usd/r3usdp ewm7'], label='r14usd/r3usdp ewm7')
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))  # 设置每7天显示一个日期
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45)
        plt.xlabel('Install Date')
        plt.ylabel('Values')
        plt.title(f'{media} - real r7usd vs self attr + pred r7usd')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f'/src/data/zk2/20230908_check_{media}.jpg')
        plt.close()

        media_result.to_csv('/src/data/zk2/20230908android05_%s.csv'%media, index=False)


if __name__ == '__main__':
    # getData()

    x,y,y0 = getXY()
    
    # 尝试将x中的后3个媒体加在一起，这样只有Facebook和自然量两个媒体
    # x[:, 3] = x[:, 1:].sum(axis=1)
    # x[:, 1:3] = 0

    train(x,y)

    model = load_model('/src/data/zk2/model20230908v1.h5', custom_objects={'CustomWeightConstraint': CustomWeightConstraint})
    printW(model)
    check(model,x,y0)


    controlGroup()
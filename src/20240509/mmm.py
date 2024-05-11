import os
import pandas as pd
import numpy as np
import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getLwAdCost(startDayStr,endDayStr):
    filename = f'/src/data/lwAdCost_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    install_day,
    mediasource,
    sum(
    cost_value_usd
    ) as cost
from 
    dwd_overseas_cost_allproject
where
    app = '502'
    AND app_package = 'id6448786147'
    AND cost_value_usd > 0
    AND facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    mediasource
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

def getLwRevenue(startDayStr,endDayStr):
    filename = f'/src/data/lwRevenue_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
SELECT
    COALESCE(
    SUM(
        CASE
        WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN revenue_value_usd
        ELSE 0
        END
    ),
    0
    ) as r1usd,
    COALESCE(
    SUM(
        CASE
        WHEN event_timestamp <= install_timestamp + 3 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
        END
    ),
    0
    ) as r3usd,
    COALESCE(
    SUM(
        CASE
        WHEN event_timestamp <= install_timestamp + 7 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
        END
    ),
    0
    ) as r7usd,
    install_day
FROM
    rg_bi.ads_lastwar_ios_purchase_adv
WHERE
    install_day between {startDayStr} and {endDayStr}
GROUP BY
    install_day
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

def getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr):
    filename = f'/src/data/lwRevenueMedia_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str})
    else:
        print('从MC获得数据')


        sql = f'''
    SET
        odps.sql.timezone = Africa / Accra;

    set
        odps.sql.hive.compatible = true;

    set
        odps.sql.executionengine.enable.rand.time.seed = true;

    @rhData :=
    select
        customer_user_id,
        media,
        rate
    from
        lastwar_ios_funplus02_adv_uid_mutidays_media
    where
        day between '{startDayStr}' and '{endDayStr}';

    @biData :=
    SELECT
        game_uid as customer_user_id,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r1usd,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 3 * 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r3usd,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 7 * 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r7usd,
        install_day as install_date,
        country as country_code
    FROM
        rg_bi.ads_lastwar_ios_purchase_adv
    WHERE
        game_uid IS NOT NULL
    GROUP BY
        game_uid,
        install_day,
        country;

    @biData2 :=
    select
        customer_user_id,
        r1usd,
        r3usd,
        r7usd,
        CASE
            WHEN r1usd = 0 THEN 'free'
            WHEN r1usd > 0
            AND r1usd <= 10 THEN 'low'
            WHEN r1usd > 10
            AND r1usd <= 80 THEN 'mid'
            ELSE 'high'
        END as paylevel,
        install_date,
        country_code
    from
        @biData;

    select
        rh.media,
        sum(bi.r1usd * rh.rate) as r1usd,
        sum(bi.r3usd * rh.rate) as r3usd,
        sum(bi.r7usd * rh.rate) as r7usd,
        bi.paylevel,
        bi.install_date,
        bi.country_code,
        sum(rh.rate) as installs
    from
        @rhData as rh
        left join @biData2 as bi on rh.customer_user_id = bi.customer_user_id
    group by
        rh.media,
        bi.install_date,
        bi.country_code,
        bi.paylevel
    ;
        '''
        print(sql)
        df = execSql(sql)

        df.to_csv(filename,index=False)
    
    return df

def getData(startDayStr,endDayStr):
    adCostDf = getLwAdCost(startDayStr,endDayStr)
    totalRevenueDf = getLwRevenue(startDayStr,endDayStr)
    mediaRevenueDf = getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr)

    # 媒体名称统一,adCostDf 的mediasource改为media
    adCostDf.rename(columns={'mediasource':'media'},inplace=True)

    # df中media中的Facebook Ads，改为Facebook
    adCostDf['media'] = adCostDf['media'].replace('Facebook Ads','Facebook')
    adCostDf['media'] = adCostDf['media'].replace('googleadwords_int','Google')
    
    mediaList = ['Facebook','Google','applovin_int']
    adCostDf = adCostDf[adCostDf['media'].isin(mediaList)]
    adCostDf = adCostDf.pivot(index='install_day', columns='media', values='cost').reset_index()
    adCostDf.rename(columns={
        'Facebook':'Facebook cost',
        'Google':'Google cost',
        'applovin_int':'applovin_int cost'
    },inplace=True)

    mediaRevenueDf = mediaRevenueDf[mediaRevenueDf['media'].isin(mediaList)]
    mediaRevenueDf = mediaRevenueDf.groupby(['media','install_date']).agg({'r1usd':'sum'}).reset_index()
    mediaRevenueDf = mediaRevenueDf.pivot(index='install_date', columns='media', values='r1usd').reset_index()
    mediaRevenueDf.rename(columns={
        'Facebook':'Facebook r1usd',
        'Google':'Google r1usd',
        'applovin_int':'applovin_int r1usd',
        'install_date':'install_day'
    },inplace=True)

    totalRevenueDf = totalRevenueDf[['install_day','r1usd']]
    totalRevenueDf.rename(columns={'r1usd':'total r1usd'},inplace=True)

    df = pd.merge(totalRevenueDf,adCostDf,left_on='install_day',right_on='install_day',how='left')
    df = pd.merge(df,mediaRevenueDf,left_on='install_day',right_on='install_day',how='left')

    # print(df)
    return df

from tensorflow import keras
import tensorflow as tf
from tensorflow.keras.layers import Input, Dense, Add
from tensorflow.keras.models import Model
from tensorflow.keras.constraints import MinMaxNorm
from tensorflow.keras.callbacks import Callback
from keras.initializers import Constant
from keras.models import load_model

def create_model():

    # 创建3个输入层
    input1 = Input(shape=(1,), name='facebook revenue')
    input2 = Input(shape=(1,), name='google revenue')
    input3 = Input(shape=(1,), name='applvin revenue')

    # 设置权重初始化方法
    initial_value = 0.05
    weight_initializer = Constant(value=initial_value)

    # 设置权重限制
    # min_value = 0.015
    # max_value = 0.08
    min_value = 0.0
    max_value = 0.1
    weight_constraint = MinMaxNorm(min_value=min_value, max_value=max_value)

    # 为每个输入层创建一个隐藏层（只有权重k，没有偏置b，且权重受到限制）
    hidden1 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer, name='facebookROI'
                )(input1)
    hidden2 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer, name='googleROI'
                )(input2)
    hidden3 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer, name='applovinROI'
                )(input3)

    # 将3个隐藏层的输出相加
    added_output = Add()([hidden1, hidden2, hidden3])

    # 创建并编译模型
    model = Model(inputs=[input1, input2, input3], outputs=added_output)
    model.compile(optimizer='adadelta', loss='mean_absolute_percentage_error',metrics=['mean_absolute_percentage_error'])

    # # 打印模型结构
    # model.summary()
    keras.utils.plot_model(model, '/src/data/20240509_model1.jpg', show_shapes=True)
    return model

class LogCallback(Callback):
    def __init__(self, model, filepath):
        self.model = model
        self.filepath = filepath
        self.log = []

    def on_epoch_end(self, epoch, logs=None):
        if epoch % 1000 == 0:
            
            loss = logs.get('loss')
            valLoss = logs.get('val_loss')
            # mape = logs.get('mean_absolute_percentage_error')

            weights = self.model.get_weights()
            # 获取每个隐藏层的权重k
            k1, k2, k3 = weights[0][0][0], weights[1][0][0], weights[2][0][0]

            # 添加记录
            self.log.append([epoch, loss, valLoss, k1, k2, k3])

            # 将记录保存到CSV文件
            df = pd.DataFrame(self.log, columns=['epochs', 'loss', 'val_loss', 'k1', 'k2', 'k3'])
            df.to_csv(self.filepath, index=False)

            # 打印记录
            print(f'Epochs: {epoch}, loss: {loss},valLoss:{valLoss}, k1: {k1}, k2: {k2}, k3: {k3}')

class PrintLossAtEndCallback(keras.callbacks.Callback):
    def on_train_end(self, epoch, logs=None):
        logs = logs or {}
        loss = logs.get('val_loss')
        mape = logs.get('mean_absolute_percentage_error')
        print(f'Final {epoch} loss: {loss}')


class SaveModelCallback(Callback):
    def __init__(self, model, save_path):
        self.model = model
        self.save_path = save_path

    def on_epoch_end(self, epoch, logs=None):
        if epoch % 1000 == 0:
            self.model.save(self.save_path)
            print(f'Model saved to {self.save_path}')

def getXY(startDayStr,endDayStr,N = 3):
    df = getData('20231201','20240507')
    df = df.fillna(0)
    df = df.sort_values(by='install_day',ascending=True)

    df['Facebook cost rolling'] = df['Facebook cost'].rolling(window=N).mean()
    df['Google cost rolling'] = df['Google cost'].rolling(window=N).mean()
    df['applovin_int cost rolling'] = df['applovin_int cost'].rolling(window=N).mean()
    df['Facebook r1usd rolling'] = df['Facebook r1usd'].rolling(window=N).mean()
    df['Google r1usd rolling'] = df['Google r1usd'].rolling(window=N).mean()
    df['applovin_int r1usd rolling'] = df['applovin_int r1usd'].rolling(window=N).mean()
    df['Facebook ROI'] = df['Facebook r1usd rolling'] / df['Facebook cost rolling']
    df['Google ROI'] = df['Google r1usd rolling'] / df['Google cost rolling']
    df['applovin_int ROI'] = df['applovin_int r1usd rolling'] / df['applovin_int cost rolling']

    df.to_csv('/src/data/20240509_data.csv',index=False)

    df = df[
        (df['install_day'] >= startDayStr) &
        (df['install_day'] <= endDayStr)
    ]

    x1 = df[['Facebook cost rolling']]
    x2 = df[['Google cost rolling']]
    x3 = df[['applovin_int cost rolling']]

    df['y'] = df['Facebook r1usd rolling'] + df['Google r1usd rolling'] + df['applovin_int r1usd rolling']
    y = df['y']

    

    return [x1,x2,x3],y

def train(startDayStr='20240101',endDayStr='20240201'):
    x,y = getXY(startDayStr,endDayStr)

    model = create_model()
    myCallback1 = LogCallback(model, '/src/data/20240509_trainLog.csv')
    myCallback2 = PrintLossAtEndCallback()
    earlyStopCallBack = keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)
    model.fit(x, y, epochs=30000,
            validation_split=0.2,  # 随机抽取20%作为验证集
            callbacks=[
                myCallback1,
                myCallback2,
                # earlyStopCallBack
            ], 
            verbose=0)
    
    return model

def test(model,startDayStr='20240101',endDayStr='20240201'):
    
    # 打印模型参数
    weights = model.get_weights()
    k1, k2, k3 = weights[0][0][0], weights[1][0][0], weights[2][0][0]

    x,y = getXY(startDayStr,endDayStr)
    y_true = y.values
    
    # 使用模型进行预测
    y_pred = model.predict(x)
    
    # 创建一个数据框，包含真实值和预测值
    df = pd.DataFrame({'y_true': y_true.flatten(), 'y_pred': y_pred.flatten()})
    
    # 计算每一行的百分比误差
    df['percentage_error'] = np.abs(df['y_true'] - df['y_pred']) / df['y_true']
    
    # 计算MAPE
    mape = df['percentage_error'].mean() * 100
    print('----------------------')
    print(f'{startDayStr}~{endDayStr}:')
    print(f'facebook ROI: {k1}, google ROI: {k2}, applovin ROI: {k3}')
    print(f'MAPE: {mape}%')
    print('----------------------')


def predict_with_custom_weights(model, weights, input_data):
    # # 设置模型权重
    # for layer, weight in zip(model.layers, weights):
    #     layer.set_weights([np.array([weight])])

    # 设置模型权重
    weight_index = 0
    for layer in model.layers:
        if len(layer.get_weights()) > 0:  # 只设置具有权重的层
            layer.set_weights([np.array([[weights[weight_index]]])])  # 注意权重的形状是(1, 1)
            weight_index += 1


    # 使用模型进行预测
    predictions = model.predict(input_data)

    return predictions

def debug():
    df = pd.read_csv('/src/data/20240509_data.csv')
    cost = df['Facebook cost'].sum()+df['Google cost'].sum()+df['applovin_int cost'].sum()
    revenue = df['Facebook r1usd'].sum()+df['Google r1usd'].sum()+df['applovin_int r1usd'].sum()
    roi = revenue / cost
    print(roi)

import matplotlib.pyplot as plt
def debug2():
    df = pd.read_csv('/src/data/20240509_trainLog.csv')
    # epochs,loss,val_loss,k1,k2,k3
    # 画图，横轴是epochs，纵轴是loss、val_loss
    # 保存到文件 /src/data/20240509_trainLog.jpg
    # 设置图形大小
    plt.figure(figsize=(10, 6))

    # 绘制loss和val_loss曲线
    plt.plot(df['epochs'], df['loss'], label='loss')
    plt.plot(df['epochs'], df['val_loss'], label='val_loss')

    # 设置图形的标题和坐标轴标签
    plt.title('Training Loss and Validation Loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')

    # 添加图例
    plt.legend()

    # 保存图形到文件
    plt.savefig('/src/data/20240509_trainLog.jpg')

import matplotlib.dates as mdates
def debug3():
    model = create_model()
    custom_weights = [0.031084414571523666, 0.026553891599178314, 0.022436069324612617]

    x,y = getXY('20240201','20240430')
    y_true = y.values
    
    # 使用模型进行预测
    y_pred = predict_with_custom_weights(model, custom_weights, x)
    
    # 创建一个数据框，包含真实值和预测值
    df = pd.DataFrame({'y_true': y_true.flatten(), 'y_pred': y_pred.flatten()})
    
    # 计算每一行的百分比误差
    df['percentage_error'] = np.abs(df['y_true'] - df['y_pred']) / df['y_true']
    
    # 计算MAPE
    mape = df['percentage_error'].mean() * 100
    print('----------------------')
    print(f'MAPE: {mape}%')
    print('----------------------')

    df.to_csv('/src/data/20240509_debug3.csv',index=False)

    # 画图，用20240201~20240430转换成日期做x轴，y轴是真实值和预测值
    # 保存到文件 /src/data/20240509_debug3.jpg
    # 设置图形大小
    plt.figure(figsize=(12, 6))
    # 创建日期范围
    date_range = pd.date_range(start='20240201', end='20240430', freq='D')

    # 绘制真实值和预测值曲线
    plt.plot(date_range, df['y_true'], label='True Values')
    plt.plot(date_range, df['y_pred'], label='Predicted Values')

    # 设置图形的标题和坐标轴标签
    plt.title('True Values vs Predicted Values')
    plt.xlabel('Date')
    plt.ylabel('Values')

    # 设置x轴的日期格式
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=10))

    # 添加图例
    plt.legend()

    # 保存图形到文件
    plt.savefig('/src/data/20240509_debug3.jpg')


if __name__ == '__main__':
    debug3()
    
    # model = train('20240201','20240430')
    # for d in [
    #     ['20240201','20240430'],
    #     ['20240201','20240228'],
    #     ['20240301','20240331'],
    #     ['20240401','20240430']
    # ]:
    #     test(model,startDayStr=d[0],endDayStr=d[1])
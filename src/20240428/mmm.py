import os
import pandas as pd
import numpy as np
import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

# 做一个类似MMM的模型
# 其中输入是几个主要媒体的花费金额，
# 输出是24小时收入
# 自然量与小媒体一起归在其他中
# 数据统一使用rolling30天的数据，这样数据比较平稳
# 训练可以只针对一个月的时间，这样不会有太多的意外波动
# 模型做出一定的限制，比如每个媒体的ROI不能太高，也不能太低。防止线性相关性较高的媒体占用太多贡献，而产生过拟合。
# 暂时全部采用线性模型，后续可以考虑加入非线性模型
# 可以按照当月的平均数值给出参数的初始值。其他媒体与自然量可以手动给出，不再参与训练。
# 意在重新非配媒体归因，解答applovin的抢量疑问。

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
    input1 = Input(shape=(1,), name='input1')
    input2 = Input(shape=(1,), name='input2')
    input3 = Input(shape=(1,), name='input3')

    # 设置权重初始化方法
    initial_value = 0.1
    weight_initializer = Constant(value=initial_value)

    # 设置权重限制
    min_value = 0.01
    max_value = 0.1
    weight_constraint = MinMaxNorm(min_value=min_value, max_value=max_value)

    # 为每个输入层创建一个隐藏层（只有权重k，没有偏置b，且权重受到限制）
    hidden1 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer
                )(input1)
    hidden2 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer
                    )(input2)
    hidden3 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer
                    )(input3)

    # 将3个隐藏层的输出相加
    added_output = Add()([hidden1, hidden2, hidden3])

    # 创建并编译模型
    model = Model(inputs=[input1, input2, input3], outputs=added_output)
    model.compile(optimizer='adam', loss='mse',metrics=['mean_absolute_percentage_error'])

    # # 打印模型结构
    # model.summary()
    keras.utils.plot_model(model, '/src/data/20240428_model1.jpg', show_shapes=True)
    return model

class LogCallback(Callback):
    def __init__(self, model, filepath):
        self.model = model
        self.filepath = filepath
        self.log = []

    def on_epoch_end(self, epoch, logs=None):
        if epoch % 1000 == 0:
            
            loss = logs.get('loss')
            mape = logs.get('mean_absolute_percentage_error')

            weights = self.model.get_weights()
            # 获取每个隐藏层的权重k
            k1, k2, k3 = weights[0][0][0], weights[1][0][0], weights[2][0][0]

            # 添加记录
            self.log.append([epoch, loss, k1, k2, k3])

            # 将记录保存到CSV文件
            df = pd.DataFrame(self.log, columns=['epochs', 'loss', 'k1', 'k2', 'k3'])
            df.to_csv(self.filepath, index=False)

            # 打印记录
            print(f'Epochs: {epoch}, MAPE: {mape}, k1: {k1}, k2: {k2}, k3: {k3}')

class SaveModelCallback(Callback):
    def __init__(self, model, save_path):
        self.model = model
        self.save_path = save_path

    def on_epoch_end(self, epoch, logs=None):
        if epoch % 1000 == 0:
            self.model.save(self.save_path)
            print(f'Model saved to {self.save_path}')

def getXY(startDayStr,endDayStr):
    df = getData('20240101','20240430')
    df = df.fillna(0)
    
    df = df[
        (df['install_day'] > startDayStr) &
        (df['install_day'] < endDayStr)
    ]

    x1 = df[['Facebook cost']]
    x2 = df[['Google cost']]
    x3 = df[['applovin_int cost']]

    # print('x异常值统计：')
    # print(x1.isna().sum(),x2.isna().sum(),x3.isna().sum())
    # print(np.isinf(x1).sum(),np.isinf(x2).sum(),np.isinf(x3).sum())

    df['y'] = df['Facebook r1usd'] + df['Google r1usd'] + df['applovin_int r1usd']
    y = df['y']
    # print('y异常值统计：')
    # print(y.isna().sum())
    # print(np.isinf(y).sum())

    return [x1,x2,x3],y

def train(startDayStr='20240101',endDayStr='20240201'):
    x,y = getXY(startDayStr,endDayStr)

    mod = create_model()
    my_callback = LogCallback(mod, '/src/data/20240428_trainLog.csv')
    unixTime = int(datetime.datetime.now().timestamp())
    modFilename = '/src/data/20240428_model.h5'
    save_model_callback = SaveModelCallback(mod, modFilename)
    mod.fit(x, y, epochs=3000,
            #  callbacks=[my_callback,save_model_callback], 
            verbose=0)
    mod.save(modFilename)

def test(modFilename = '/src/data/20240428_model.h5',startDayStr='20240101',endDayStr='20240201'):
    # 加载模型
    model = load_model(modFilename)
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


if __name__ == '__main__':
    for d in [
        ['20240101','20240425'],
        ['20240101','20240131'],
        ['20240201','20240228'],
        ['20240301','20240331'],
        ['20240401','20240425']
    ]:
        print(d)
        train(d[0],d[1])
        test(modFilename = '/src/data/20240428_model.h5',startDayStr=d[0],endDayStr=d[1])
    
    
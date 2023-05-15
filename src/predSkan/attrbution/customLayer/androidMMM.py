# 用安卓数据进行MMM尝试，只是类似，自建模型，没有那么复杂
# 不再使用SKAN数据，原因是SKAN数据的安装时间偏差大，只能用均线来做，没有价值。另外2023-02-28更换了CV Map，导致CV值变化。

# 暂时选用媒体数据：花费（美元），展示数，点击数，安装数
# 选用用户数据：7日付费金额总额（分媒体，用于验算）

# 步骤
# 1、获取媒体数据
# 2、获取用户数据
# 3、数据整理
# 4、建立模型，并训练
# 5、记录日志

import datetime
import pandas as pd

import os
import sys
import numpy as np
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename):
    return '/src/data/customLayer/%s.csv'%(filename)

# 1、获取媒体数据
def getMediaData():
    sql = '''
        select
            mediasource as media,
            to_char(
                to_date(day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(installs) as installs,
            sum(cost) as cost
        from
        (
            select
                day,
                mediasource,
                getapppackagev2(
                    app,
                    mediasource,
                    campaign_name,
                    adset_name,
                    ad_name
                ) as app_package,
                campaign_name,
                adset_name,
                ad_name,
                impressions,
                clicks,
                installs,
                cost
            from
                ods_realtime_mediasource_cost
            where
                app = 102
                and day >= 20220501
                and day < 20230228
        )
        where
            app_package = 'com.topwar.gp'
        group by
            mediasource,
            day
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('media_20220501_20230227'),index=False)
    return df

def loadMediaData():
    df = pd.read_csv(getFilename('media_20220501_20230227'))
    return df

# 2、获取用户数据
def getUserData():
    # 从AF事件表中获取用户数据
    # 安装日期在2022-05-01~2023-02-27之间
    # 用户7日内付费金额
    # 海外安卓
    # 用af id做用户区分
    # 按照安装日期（天）与媒体进行汇总
    sql = '''
        WITH install_data AS (
        SELECT
            appsflyer_id,
            media_source,
            to_char(
            to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
            "yyyy-mm-dd"
            ) AS install_date
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'com.topwar.gp'
            AND zone = 0
            AND event_name = 'install'
            AND `day` BETWEEN '20220501'
            AND '20230227'
        )
        SELECT
        install_data.install_date,
        install_data.media_source,
        COUNT(
            DISTINCT ods_platform_appsflyer_events.appsflyer_id
        ) AS user_count,
        SUM(
            CASE
            WHEN event_name = 'af_purchase' THEN event_revenue_usd
            ELSE 0
            END
        ) AS revenue_7d
        FROM
        ods_platform_appsflyer_events
        JOIN install_data ON ods_platform_appsflyer_events.appsflyer_id = install_data.appsflyer_id
        WHERE
        app_id = 'com.topwar.gp'
        AND zone = 0
        AND event_name = 'af_purchase'
        AND ods_platform_appsflyer_events.`day` BETWEEN '20220501'
        AND '20230227'
        GROUP BY
        install_data.install_date,
        install_data.media_source;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('user_20220501_20230227'),index=False)
    return df

def loadUserData():
    df = pd.read_csv(getFilename('user_20220501_20230227'))
    return df

# 3、数据整理
def data():
    mediaDf = loadMediaData()
    userDf = loadUserData()
    # mediaDf head
    # media,install_date,impressions,clicks,installs,cost
    # googleadwords_int,2022-06-26,4464073,60158,14485,58545.160183
    # bytedanceglobal_int,2022-07-01,4863548,74629,6098,41180.009999999995
    # bytedanceglobal_int,2022-12-02,2577609,16161,2942,5855.0999999999985
    # googleadwords_int,2023-02-25,12751384,66414,16585,172018.28122000003
    # googleadwords_int,2022-07-05,5123034,61063,14832,74051.356882
    # bilibili_int,2023-01-19,0,0,0,0.0
    # sinaweibo_int,2022-08-10,33735,52,7,200.0
    # baiduyuansheng_int,2023-01-01,819410,5404,1496,14751.579999999998
    # googleadwords_int,2022-06-28,7110427,77293,17364,78999.56730699999

    # userDf head
    # install_date,media_source,user_count,revenue_7d
    # 2022-05-01,Facebook Ads,177,1718.2606950476006
    # 2022-05-01,applovin_int,3,23.02929799980481
    # 2022-05-01,bytedanceglobal_int,96,1047.2647974666934
    # 2022-05-01,googleadwords_int,314,5873.826107844505
    # 2022-05-01,ironsource_int,15,104.47545783444497
    # 2022-05-01,moloco_int,1,20.6194653997699
    # 2022-05-01,restricted,22,489.2903333874733
    # 2022-05-01,unityads_int,3,4.877042383861182
    # 2022-05-02,Facebook Ads,128,1496.5599624708673

    # 要求
    # 将媒体进行分组，googleadwords_int，bytedanceglobal_int，Facebook Ads和其他
    # 制作X，按照上面分组顺序，每个媒体的impressions,clicks,installs,cost，共4组
    # 制作Y，按照安装日期汇总，计算7日回收
    # 制作分媒体每天7日回收，按照上面提到的媒体分组与安装日期进行分组计算7日回收汇总，用于模型验算
    # 最终将X，Y，分媒体每天7日回收，共3个文件，保存到'/src/data/customLayer/',命名为x.csv，y.csv，yMedia.csv
    
    # 将媒体进行分组
    mediaDf['media_group'] = mediaDf['media'].apply(lambda x: x if x in ['googleadwords_int', 'bytedanceglobal_int', 'Facebook Ads'] else '其他')

    # 制作X
    x = mediaDf.pivot_table(index='install_date', columns='media_group', values=['impressions', 'clicks', 'installs', 'cost']).reset_index()
    x.columns = ['_'.join(col) if col[1] != '' else col[0] for col in x.columns]

    # 检查并添加缺失的列
    media_groups = ['googleadwords_int', 'bytedanceglobal_int', 'Facebook Ads', '其他']
    for media in media_groups:
        for col in ['impressions', 'clicks', 'installs', 'cost']:
            column_name = f'{col}_{media}'
            if column_name not in x.columns:
                x[column_name] = 0

    
    # 在x中加入一列，weekday，计算install_date对应的是周几，0~6
    x['weekday'] = pd.to_datetime(x['install_date']).dt.weekday

    # 按照指定的顺序整理列
    columns_order = ['install_date', 'weekday']

    for media in media_groups:
        columns_order.extend([f'impressions_{media}', f'clicks_{media}', f'installs_{media}', f'cost_{media}'])

    x = x[columns_order]

    # 制作Y
    y = userDf.groupby('install_date').agg({'revenue_7d': 'sum'}).reset_index()

    # 制作分媒体每天7日回收
    yMedia = userDf.groupby(['media_source', 'install_date']).agg({'revenue_7d': 'sum'}).reset_index()

    # 保存X，Y，分媒体每天7日回收到文件
    x.to_csv('/src/data/customLayer/x.csv', index=False)
    y.to_csv('/src/data/customLayer/y.csv', index=False)
    yMedia.to_csv('/src/data/customLayer/yMedia.csv', index=False)

import os
import numpy as np
import pandas as pd
import tensorflow as tf
import matplotlib.pyplot as plt
from sklearn.model_selection import KFold
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, Add
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.regularizers import l2
from tensorflow.keras.layers.experimental.preprocessing import Normalization

from tqdm import tqdm

# 4、建立模型，并训练
def train(num_iterations=10, k_folds=3, l2_reg=0.01):
    x = pd.read_csv('/src/data/customLayer/x.csv')
    y = pd.read_csv('/src/data/customLayer/y.csv')

    x = x.drop(columns=['install_date'])
    y = y.drop(columns=['install_date'])

    # One-hot encode the 'weekday' column
    x = pd.concat([pd.get_dummies(x['weekday'], prefix='weekday'), x.drop(columns=['weekday'])], axis=1)

    # print(x.head())

    headers = [
        'weekday_0','weekday_1','weekday_2','weekday_3','weekday_4','weekday_5','weekday_6',
        'impressions_googleadwords_int','clicks_googleadwords_int','installs_googleadwords_int','cost_googleadwords_int',
        'weekday_0','weekday_1','weekday_2','weekday_3','weekday_4','weekday_5','weekday_6',
        'impressions_bytedanceglobal_int','clicks_bytedanceglobal_int','installs_bytedanceglobal_int','cost_bytedanceglobal_int',
        'weekday_0','weekday_1','weekday_2','weekday_3','weekday_4','weekday_5','weekday_6',
        'impressions_Facebook Ads','clicks_Facebook Ads','installs_Facebook Ads','cost_Facebook Ads',
        'weekday_0','weekday_1','weekday_2','weekday_3','weekday_4','weekday_5','weekday_6',
        'impressions_其他','clicks_其他','installs_其他','cost_其他'
    ]
    x = x[headers]
    

    def create_model():
        inputs = []
        outputs = []
        for i in range(4):
            input_layer = Input(shape=(11,))  # Change the input shape to (11,)
            inputs.append(input_layer)
            x_normalized = Normalization()(input_layer)
            hidden1 = Dense(32, activation='relu', kernel_regularizer=l2(l2_reg))(x_normalized)
            output_layer = Dense(1)(hidden1)
            outputs.append(output_layer)

        summed_output = Add()(outputs)
        final_output = Dense(1)(summed_output)

        model = Model(inputs=inputs, outputs=final_output)
        model.compile(optimizer=Adam(), loss='mse', metrics=[tf.keras.losses.MeanAbsolutePercentageError()])

        return model

    best_model = None
    best_performance = float('inf')
    best_history = None

    for _ in tqdm(range(num_iterations), desc="Training iterations", unit="iteration"):
        # kfold = KFold(n_splits=k_folds, shuffle=True, random_state=42)
        kfold = KFold(n_splits=k_folds, shuffle=True)

        overall_train_mapes = []
        overall_val_mapes = []

        model = create_model()

        for train_index, val_index in kfold.split(x, y):
            x_train, x_val = x.iloc[train_index], x.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            x_train_splits = [x_train.iloc[:, i*11:(i+1)*11] for i in range(4)]
            x_val_splits = [x_val.iloc[:, i*11:(i+1)*11] for i in range(4)]

            x_train_splits = [split.values for split in x_train_splits]
            x_val_splits = [split.values for split in x_val_splits]

            # print('AAA:',x_train_splits)

            history = model.fit(x_train_splits, y_train.values, epochs=300, batch_size=32, validation_data=(x_val_splits, y_val.values), verbose=0)

            overall_train_mapes.append(history.history['mean_absolute_percentage_error'][-1])
            overall_val_mapes.append(history.history['val_mean_absolute_percentage_error'][-1])

        overall_mape = np.mean(overall_val_mapes)

        if overall_mape < best_performance:
            best_performance = overall_mape
            best_model = model
            best_history = history

    print(f"Best Overall Validation MAPE: {best_performance:.2f}")

    os.makedirs('/src/data/customLayer/mod/', exist_ok=True)
    best_model.save_weights('/src/data/customLayer/mod/weights.h5')
    with open('/src/data/customLayer/mod/model.json', 'w') as f:
        f.write(best_model.to_json())

    plt.plot(best_history.history['loss'], label='Training Loss')
    plt.plot(best_history.history['val_loss'], label='Validation Loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()

    os.makedirs('/src/data/customLayer/', exist_ok=True)
    plt.savefig('/src/data/customLayer/loss.jpg')

def evaluate():
    x = pd.read_csv('/src/data/customLayer/x.csv')
    y = pd.read_csv('/src/data/customLayer/y.csv')

    # 这里需要将x和y中的install_date列都去掉
    x = x.drop(columns=['install_date'])
    y = y.drop(columns=['install_date'])

    # 2、加载模型
    # 加载模型的权重和结构
    # 加载路径在'/src/data/customLayer/mod/'

    with open('/src/data/customLayer/mod/model.json', 'r') as f:
        model = tf.keras.models.model_from_json(f.read())
    model.load_weights('/src/data/customLayer/mod/weights.h5')

    # 进行整体预测，并计算MAPE
    x_splits = np.split(x.values, 4, axis=1)
    y_pred = model.predict(x_splits)
    mape = np.mean(np.abs((y.values - y_pred) / y.values)) * 100
    print('MAPE: %.2f%%' % mape)

from sklearn.model_selection import train_test_split
def train2():
    x = pd.read_csv('/src/data/customLayer/x.csv')
    y = pd.read_csv('/src/data/customLayer/y.csv')

    # 去掉install_date列
    x = x.drop(columns=['install_date'])
    y = y.drop(columns=['install_date'])

    def create_model():
        inputs = []
        outputs = []
        for i in range(4):
            input_layer = Input(shape=(4,))
            inputs.append(input_layer)
            x_normalized = Normalization()(input_layer)
            hidden1 = Dense(32, activation='relu')(x_normalized)
            hidden2 = Dense(32, activation='relu')(hidden1)
            output_layer = Dense(1)(hidden2)
            outputs.append(output_layer)

        summed_output = Add()(outputs)
        final_output = Dense(1)(summed_output)

        model = Model(inputs=inputs, outputs=final_output)
        model.compile(optimizer=Adam(), loss='mse', metrics=[tf.keras.losses.MeanAbsolutePercentageError()])

        return model

    # 划分训练集和测试集
    x_train, x_val, y_train, y_val = train_test_split(x, y, test_size=0.3, random_state=42)

    model = create_model()

    x_train_splits = np.split(x_train.values, 4, axis=1)
    x_val_splits = np.split(x_val.values, 4, axis=1)

    history = model.fit(x_train_splits, y_train.values,
                         epochs=100, 
                         batch_size=32, 
                         validation_data=(x_val_splits, y_val.values)
                        , verbose=0
                        )

    # 确保路径存在
    os.makedirs('/src/data/customLayer/mod/', exist_ok=True)

    # 保存模型的权重和结构
    model.save_weights('/src/data/customLayer/mod/weights.h5')
    with open('/src/data/customLayer/mod/model.json', 'w') as f:
        f.write(model.to_json())

    # 绘制损失变化曲线
    plt.plot(history.history['loss'], label='Training Loss')
    plt.plot(history.history['val_loss'], label='Validation Loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()

    # 保存损失曲线图像
    os.makedirs('/src/data/customLayer/', exist_ok=True)
    plt.savefig('/src/data/customLayer/loss.jpg')

    # 输出模型在训练集和测试集上的MAPE
    train_mape = model.evaluate(x_train_splits, y_train.values, verbose=0)[1]
    val_mape = model.evaluate(x_val_splits, y_val.values, verbose=0)[1]
    print(f"Train MAPE: {train_mape:.2f}")
    print(f"Validation MAPE: {val_mape:.2f}")

if __name__ == '__main__':
    # getMediaData()
    # loadMediaData()

    # getUserData()
    # loadUserData()

    # data()
    train()
    # evaluate()

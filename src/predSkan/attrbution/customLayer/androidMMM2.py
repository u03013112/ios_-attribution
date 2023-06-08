# androidMMM效果不好，分析原因是输入特征和输出结论之间的相关系数太低
# 这里还是考虑找到一个更好的特征提，然后用新特征进行MMM
# 最直接的特征就是首日付费金额，但是目前可以有效归因的只有AF模糊归因（或IDFA）和SKAN
# 模糊归因的缺陷是每个媒体都只有一部分用户可以有效归因，而且每个媒体的归因比例不一样，模糊归因部分不太好模拟，只能用随机进行模拟，然后看结论
# SKAN的归因缺陷是安装日期的不准确
# 所以在进行特征确认之前需要先做一些实验
# 1. 首先看看AF模糊归因后的首日付费金额与媒体真实首日付费金额，7日付费金额的相关度。
# 2. 然后看看SKAN归因后的首日付费金额与媒体真实首日付费金额，7日付费金额的相关度。
# 找到比较好的特征之后，再进行MMM，MMM可能还是按照之前的思路，还是用首日付费金额相关度高的特征*系数的方式进行预测，模型用来预测系数

# 在androidFpNew中获得了融合归因方案的首日付费金额是比较好的特征，但是这个特征的缺陷是只有一部分用户可以归因，而且归因比例不一样
# 比AF的SKAN平均安装日期的方案好

# 初步设想：
# 整体模型的设计：
# 融合归因的首日收入金额简称r1usd，最终预测目标简称r7usd
# 既然得到了相关系数如此高的特征，就还是要将模型拆分成4个独立的神经网络
# 每个神经网络估测出一个比率，即r7usd/r1usd
# 然后将4个比率与r1usd相乘，再将4个结果相加，得到最终的r7usd
# 然后直接用r7usd与真实r7usd的mse作为loss，训练模型

# 目前遇到的困难是：
# 1、自然量的评估

import tensorflow as tf
import pandas as pd
import numpy as np
from keras.constraints import MinMaxNorm, Constraint

mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    'snapchat_int',
    # 'other'
]
# 准备数据
# 用安卓数据，2022-01-01~2023-03-31
# 之前曾经获得，详见androidFpNew.py的getDataFromMC
# 直接读取文件'/src/data/zk/androidFp03.csv'

# 目前需要数据
# 1、收日收入（融合归因版本），按安装日期和媒体分组
# 2、7日收入（真实版本），按安装日期和媒体分组
# 3、7日收入（真实版本），按安装日期分组 作为Y
# 4、CV 分布，按安装日期和媒体分组 作为X的一部分
# 5、广告信息，按安装日期和媒体分组 作为X的一部分
# 6、其他信息，按安装日期 作为X的一部分
def prepareData(mediaList = mediaList):
    # userDf = pd.read_csv('/src/data/zk/attribution1ReStep6.csv')
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
    userDf_r1usd.to_csv('/src/data/zk/userDf_r1usd_mmm.csv', index=False )

    userDf_r3usd = userDf_r3usd.melt(id_vars=['install_date'], var_name='media', value_name='r3usd')
    userDf_r3usd['media'] = userDf_r3usd['media'].str.replace(' r3usd', '')
    userDf_r3usd = userDf_r3usd.groupby(['install_date', 'media']).sum().reset_index()
    userDf_r3usd.to_csv('/src/data/zk/userDf_r3usd_mmm.csv', index=False )

    userDf_r7usd = userDf_r7usd.melt(id_vars=['install_date'], var_name='media', value_name='r7usd')
    userDf_r7usd['media'] = userDf_r7usd['media'].str.replace(' r7usd', '')
    userDf_r7usd = userDf_r7usd.groupby(['install_date', 'media']).sum().reset_index()
    userDf_r7usd.to_csv('/src/data/zk/userDf_r7usd_mmm.csv', index=False )

    userDf = userDf_r1usd.merge(userDf_r3usd, on=['install_date', 'media'])
    userDf = userDf.merge(userDf_r7usd, on=['install_date', 'media'])
    userDf.to_csv('/src/data/zk/userDf_mmm.csv', index=False )
# 验算一下,得到的结论是融合归因的r1usd与真实的r1usd偏差还是挺大的，MAPE很高
# 但是融合归因的r1usd与r7usd的相关度很高，所以还是可以用来预测r7usd的。
def debug3():
    userDf_r1usd = pd.read_csv('/src/data/zk/userDf_mmm.csv')
    
    df = pd.read_csv('/src/data/zk/androidFp03.csv')
    df = df.rename(columns={'media_source':'media'})
    # 将df中media列中，不在mediaList中的值，替换为'other'
    df.loc[~df['media'].isin(mediaList), 'media'] = 'other'
    rawDf = df.groupby(['install_date', 'media']).agg({'r1usd':'sum','r3usd':'sum','r7usd':'sum'}).reset_index()
    # print(rawDf.head(10))

    df = pd.merge(rawDf, userDf_r1usd, on=['install_date', 'media'], how='left',suffixes=('_raw', '_mmm'))
    # df['mape1'] = abs(df['r1usd_mmm'] - df['r1usd_raw']) / df['r1usd_raw']
    # df['mape7'] = abs(df['r7usd_mmm'] - df['r7usd_raw']) / df['r7usd_raw']

    # print(df.head(10))
    df.to_csv('/src/data/zk/check1_mmm.csv', index=False)
    # df.to_csv('/src/data/zk/check1_mmm2.csv', index=False)
    mediaList.append('other')
    for media in mediaList:
        df_media = df[df['media']==media]
        # print(media, '\nmape1:',df_media['mape1'].mean())
        # print(media, '\nmape7:',df_media['mape7'].mean())
        print(media,':')
        print(df_media.corr())

from tensorflow import keras
from sklearn.model_selection import train_test_split
from keras.models import Model
from keras.layers import Input, Dense, Add, Lambda
from tensorflow.keras.optimizers import Adam
from keras.initializers import Zeros
from keras.callbacks import EarlyStopping

import matplotlib.pyplot as plt
# https://rivergame.feishu.cn/docx/UIpjdW3tIohOHdxXnG9crBKonPg

class CustomWeightConstraint(Constraint):
    def __call__(self, w):
        return tf.clip_by_value(w, 1.5, 2.5)

def train1(rNUsd = 'r1usd_mmm'):
    # 读取数据
    
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = dateFilter(df)

    df = df[['install_date', 'media', 'r7usd_raw', rNUsd]]

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values=rNUsd).reset_index()
    media_df = media_df.fillna(0)

    # 将y，即r7usd_raw的按天汇总做成df
    y_df = df.groupby('install_date')['r7usd_raw'].sum().reset_index()

    # 将两个df进行merge
    merged_df = pd.merge(media_df, y_df, on='install_date')

    unique_install_dates = merged_df['install_date'].unique()
    train_dates, test_dates = train_test_split(unique_install_dates, test_size=0.3)

    # 根据分割的安装日期划分训练集和测试集
    train_df = merged_df[merged_df['install_date'].isin(train_dates)].sort_values('install_date')
    test_df = merged_df[merged_df['install_date'].isin(test_dates)].sort_values('install_date')

    # 准备训练和测试数据
    input_columns = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    X_train = train_df[input_columns].values
    y_train = train_df['r7usd_raw'].values

    X_test = test_df[input_columns].values
    y_test = test_df['r7usd_raw'].values

    # 创建模型
    inputs = Input(shape=(5,))
    inputs_list = [Lambda(lambda x, i=i: x[:, i:i+1])(inputs) for i in range(5)]
    


    # outputs_list = [Dense(1, activation='linear', bias_initializer=Zeros())(input) for input in inputs_list]

    outputs_list = [Dense(1, activation='linear', kernel_constraint=CustomWeightConstraint(), use_bias=False)(input) for input in inputs_list]

    outputs_sum = Add()(outputs_list)
    model = Model(inputs=inputs, outputs=outputs_sum)
    keras.utils.plot_model(model, '/src/data/zk2/model1.jpg', show_shapes=True)
    # 编译模型
    # model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])
    model.compile(optimizer='Adam', loss='mse', metrics=['mape'])

    # # 创建一个包含输入层和Lambda层的模型
    # split_model = Model(inputs=inputs, outputs=inputs_list)

    # # 使用模型查看拆分后的结果
    # split_results = split_model.predict(X_train)
    # print(X_train[0])
    # for i, result in enumerate(split_results):
    #     print(f"Input {i}:")
    #     print(result[0])
    # return

    early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
    # 训练模型
    history = model.fit(X_train, y_train, validation_data=(X_test, y_test), epochs=3000, batch_size=32,
            callbacks=[early_stopping]
        )

    # 画出loss曲线
    plt.plot(history.history['loss'], label='train_loss')
    plt.plot(history.history['val_loss'], label='val_loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()
    plt.savefig('/src/data/zk2/history1.jpg')
    plt.show()

    # 评估模型
    test_mape = np.min(history.history['val_mape'])
    print("Test MAPE: {:.4f}".format(test_mape))

    # 输出5个媒体对应的ax+b中的a和b
    # all_weights = model.get_weights()
    # for i, media in enumerate(input_columns):
    #     a = all_weights[2 * i][0][0]
    #     b = all_weights[2 * i + 1][0]
    #     print(f"Media {media}: a = {a}, b = {b}")

    all_weights = model.get_weights()
    for i, media in enumerate(input_columns):
        w = all_weights[i][0][0]
        print(f"Media {media}: w = {w}")

        
    # 保存模型
    model.save('/src/data/zk2/model1.h5')

from sklearn.metrics import r2_score,mean_absolute_percentage_error

from keras.models import load_model
# 对train1训练的模型进行验算
def check1(rNUsd = 'r1usd_mmm'):
    # 读取数据
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = dateFilter(df)
    df = df[['install_date', 'media', 'r7usd_raw', rNUsd]]

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values=rNUsd).reset_index()
    media_df = media_df.fillna(0)

    # 将y，即r7usd_raw的按天汇总做成df
    y_df = df.groupby('install_date')['r7usd_raw'].sum().reset_index()

    # 将两个df进行merge
    merged_df = pd.merge(media_df, y_df,how = 'left', on='install_date')

    # print(merged_df.head())
    # #   install_date  Facebook Ads  bytedanceglobal_int  googleadwords_int       other  snapchat_int     r7usd_raw
    # # 0   2022-01-01    582.197579           168.312738        2280.083308  830.640086           0.0  13000.799111
    # # 1   2022-01-02    978.634895           259.451101        1973.497116  741.283880           0.0  11550.101998
    # # 2   2022-01-03    928.413331           186.509468        2451.319941  902.523207           0.0  15641.582780
    # # 3   2022-01-04    810.245667           243.669965        1683.907281  479.359040           0.0  11132.715836
    # # 4   2022-01-05   1215.309782           341.847853        1604.688325  444.963346           0.0  12373.840515

    # 准备训练和测试数据
    input_columns = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    X = merged_df[input_columns].values
    y = merged_df['r7usd_raw'].values

    # 加载模型
    # model = load_model('/src/data/zk2/model1.h5')
    model = load_model('/src/data/zk2/model1.h5', custom_objects={'CustomWeightConstraint': CustomWeightConstraint})
    # 用此模型预测r7usd_pred，并计算与r7usd_raw的mape
    y_pred = model.predict(X)
    
    merged_df['r7usd_pred'] = y_pred
    merged_df['mape'] = abs(merged_df['r7usd_raw'] - merged_df['r7usd_pred']) / merged_df['r7usd_raw']
    merged_df.to_csv('/src/data/zk2/check1_mmm.csv', index=False)
    print("Global MAPE:", merged_df['mape'].mean())

    # 获取每个媒体的真实付费金额
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = dateFilter(df)
    df = df[['install_date', 'media', 'r7usd_raw']]

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values='r7usd_raw').reset_index()
    media_df = media_df.fillna(0)
    # print(media_df.head())

    # 对每个媒体进行预测并计算MAPE
    for i, media in enumerate(input_columns):
        print(media)
        X_single_media = np.zeros_like(X)
        X_single_media[:, i] = X[:, i]
        # print(X_single_media[0])
        y_pred = model.predict(X_single_media)

        media_df['%s_pred' % media] = y_pred
        media_df['%s_mape' % media] = abs(media_df[media] - media_df['%s_pred' % media]) / media_df[media]

        print(media_df['%s_mape' % media].mean())
    

    media_df.to_csv('/src/data/zk2/check1_mmm2.csv', index=False)

# 原本的train1和check1是针对所有数据只拟合出一套用户质量（即7日付费金额与N日付费金额的比例），并计算拟合值与真实值的总体MAPE和分媒体MAPE
# 现在希望对数据进行分组，按照install_date（天）进行分组，每N天分一组，默认是30天
# 每一组数据都拟合出一套用户质量，即一套参数（模型）。仍然采用train1的方式将每一组数据都拿出一部分做测试集
# 然后跨越分组，计算拟合值与真实值的总体MAPE和分媒体MAPE
import datetime

def parse_date(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

# 将日期分组，每N天为一组
def group_dates(dates, days_per_group=30):
    min_date = min(dates)
    max_date = max(dates)
    date_ranges = []
    start_date = min_date
    while start_date <= max_date:
        end_date = start_date + datetime.timedelta(days=days_per_group - 1)
        date_ranges.append((start_date, end_date))
        start_date = end_date + datetime.timedelta(days=1)
    return date_ranges

# 根据日期范围筛选数据
def filter_data_by_date_range(df, start_date, end_date):
    return df[(df['install_date'] >= start_date) & (df['install_date'] <= end_date)]

def train2(rNUsd='r1usd_mmm', days_per_group=30):
    # 读取数据
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = df[['install_date', 'media', 'r7usd_raw', rNUsd]]
    df['install_date'] = df['install_date'].apply(parse_date)

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values=rNUsd).reset_index()
    media_df = media_df.fillna(0)

    # 将y，即r7usd_raw的按天汇总做成df
    y_df = df.groupby('install_date')['r7usd_raw'].sum().reset_index()

    # 将两个df进行merge
    merged_df = pd.merge(media_df, y_df, on='install_date')

    # 获取日期分组
    unique_install_dates = df['install_date'].unique()
    date_ranges = group_dates(unique_install_dates, days_per_group)

    models = []

    # 对每个日期范围进行处理
    for start_date, end_date in date_ranges:
        print(f"Processing date range: {start_date} - {end_date}")

        # 筛选出当前日期范围内的数据
        date_range_df = filter_data_by_date_range(merged_df, start_date, end_date)

        # 准备训练数据
        input_columns = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
        X_train = date_range_df[input_columns].values
        y_train = date_range_df['r7usd_raw'].values

        # 创建模型
        inputs = Input(shape=(5,))
        inputs_list = [Lambda(lambda x, i=i: x[:, i:i+1])(inputs) for i in range(5)]

        outputs_list = [Dense(1, activation='linear', bias_initializer=Zeros())(input) for input in inputs_list]
        # 添加权重限制：w至少为1，b只能是0
        # outputs_list = [Dense(1, activation='linear', kernel_constraint=MinMaxNorm(min_value=1.0), bias_initializer=Zeros())(input) for input in inputs_list]
        
        outputs_sum = Add()(outputs_list)
        model = Model(inputs=inputs, outputs=outputs_sum)

        # 编译模型
        model.compile(optimizer='RMSprop', loss='mse', metrics=['mape'])
        # model.compile(optimizer='Adam', loss='mse', metrics=['mape'])

        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

        # 训练模型，使用validation_split参数划分30%的数据作为测试集
        history = model.fit(X_train, y_train, validation_split=0.3, epochs=5000, batch_size=20,
                            callbacks=[early_stopping])

        # 保存模型
        model.save(f'/src/data/zk2/model_{start_date}_to_{end_date}.h5')
        models.append(model)

    return models

def check2(models=None, rNUsd='r1usd_mmm', days_per_group=30):
    # 读取数据
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = df[['install_date', 'media', 'r7usd_raw', rNUsd]]
    df['install_date'] = df['install_date'].apply(parse_date)

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values=[rNUsd, 'r7usd_raw']).reset_index()
    media_df = media_df.fillna(0)

    # 获取日期分组
    unique_install_dates = df['install_date'].unique()
    date_ranges = group_dates(unique_install_dates, days_per_group)

    if not models:
        models = []
        for start_date, end_date in date_ranges:
            model_path = f'/src/data/zk2/model_{start_date}_to_{end_date}.h5'
            model = load_model(model_path)
            models.append(model)

    all_date_range_df = pd.DataFrame()

    # 对每个日期范围进行处理
    for (start_date, end_date), model in zip(date_ranges, models):
        print(f"Processing date range: {start_date} - {end_date}")

        # 筛选出当前日期范围内的数据
        date_range_df = filter_data_by_date_range(media_df, start_date, end_date)

        # 准备测试数据
        input_columns = [(rNUsd, 'googleadwords_int'), (rNUsd, 'Facebook Ads'), (rNUsd, 'bytedanceglobal_int'), (rNUsd, 'snapchat_int'), (rNUsd, 'other')]

        # 对每个媒体进行预测并计算MAPE
        for i, media in enumerate(input_columns):
            X_single_media = np.zeros_like(date_range_df[input_columns].values)
            X_single_media[:, i] = date_range_df[media].values
            y_pred = model.predict(X_single_media)

            date_range_df[f'{media[1]}_pred'] = y_pred
            date_range_df[f'{media[1]}_mape'] = abs(date_range_df[('r7usd_raw', media[1])] - date_range_df[f'{media[1]}_pred']) / date_range_df[('r7usd_raw', media[1])]
            
            # 计算每个媒体在当前日期范围内的MAPE
            media_mape = date_range_df[f'{media[1]}_mape'].mean()
            print(f"{media[1]} MAPE in date range {start_date} - {end_date}: {media_mape}")
        
        # 输出5个媒体对应的ax+b中的a和b
        all_weights = model.get_weights()
        for i, media in enumerate(input_columns):
            a = all_weights[2 * i][0][0]
            b = all_weights[2 * i + 1][0]
            print(f"Media {media}: a = {a}, b = {b}")

        all_date_range_df = all_date_range_df.append(date_range_df)

    all_date_range_df.to_csv('/src/data/zk2/check2.csv', index=False)

    # 计算每个媒体的总体MAPE
    media_mape_df = pd.DataFrame(columns=['media', 'mape'])
    for media in input_columns:
        media_mape = np.mean(np.abs((all_date_range_df[('r7usd_raw', media[1])] - all_date_range_df[f'{media[1]}_pred']) / all_date_range_df[('r7usd_raw', media[1])]))
        media_mape_df = media_mape_df.append({'media': media[1], 'mape': media_mape}, ignore_index=True)

    print("\nMedia total MAPEs:")
    print(media_mape_df)

def debug1():
    # input_columns = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    input_columns = [
        'googleadwords_int', 
        'Facebook Ads', 
        'bytedanceglobal_int', 
        'snapchat_int', 
        'other'
    ]

    model = load_model('/src/data/zk2/model1.h5')
    all_weights = model.get_weights()
    for i, media in enumerate(input_columns):
        a = all_weights[2 * i][0][0]
        b = all_weights[2 * i + 1][0]
        print(f"Media {media}: a = {a}, b = {b}")

    modelPathList = [
        '/src/data/zk2/model_2022-08-29_to_2022-09-27.h5',
        '/src/data/zk2/model_2022-01-01_to_2022-01-30.h5',
        '/src/data/zk2/model_2022-09-28_to_2022-10-27.h5',
        '/src/data/zk2/model_2022-01-31_to_2022-03-01.h5',
        '/src/data/zk2/model_2022-10-28_to_2022-11-26.h5',
        '/src/data/zk2/model_2022-03-02_to_2022-03-31.h5',
        '/src/data/zk2/model_2022-11-27_to_2022-12-26.h5',
        '/src/data/zk2/model_2022-04-01_to_2022-04-30.h5',
        '/src/data/zk2/model_2022-12-27_to_2023-01-25.h5',
        '/src/data/zk2/model_2022-05-01_to_2022-05-30.h5',
        '/src/data/zk2/model_2023-01-26_to_2023-02-24.h5',
        '/src/data/zk2/model_2022-05-31_to_2022-06-29.h5',
        '/src/data/zk2/model_2023-02-25_to_2023-03-26.h5',
        '/src/data/zk2/model_2022-06-30_to_2022-07-29.h5',
        '/src/data/zk2/model_2023-03-27_to_2023-04-25.h5',
        '/src/data/zk2/model_2022-07-30_to_2022-08-28.h5',
    ]

    for modelPath in modelPathList:
        print(f"Model: {modelPath}")
        model = load_model(modelPath)
        all_weights = model.get_weights()
        for i, media in enumerate(input_columns):
            a = all_weights[2 * i][0][0]
            b = all_weights[2 * i + 1][0]
            print(f"Media {media}: a = {a}, b = {b}")

import matplotlib.dates as mdates
def debug2():
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    facebook_df = df[df['media']=='Facebook Ads']
    print(facebook_df.head())

    facebook_df['r7/r1'] = facebook_df['r7usd_raw'] / facebook_df['r1usd_raw']
    facebook_df['r7/r1 mmm'] = facebook_df['r7usd_raw'] / facebook_df['r1usd_mmm']
    facebook_df['r7/r3 mmm'] = facebook_df['r7usd_raw'] / facebook_df['r3usd_mmm']

    facebook_df['install_date'] = pd.to_datetime(facebook_df['install_date'])

    # 设置图像大小
    plt.figure(figsize=(12, 6))

    # 画图，install_date是x
    plt.plot(facebook_df['install_date'], facebook_df['r7/r1'], label='r7/r1')
    plt.plot(facebook_df['install_date'], facebook_df['r7/r1 mmm'], label='r7/r1 mmm')
    plt.plot(facebook_df['install_date'], facebook_df['r7/r3 mmm'], label='r7/r3 mmm')
    plt.xlabel('install_date')
    plt.ylabel('r7/r1')

    # 设置x轴刻度为每月一个点
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

    plt.legend()
    plt.savefig('/src/data/zk2/debug2.jpg')

def debug4():
    df = pd.read_csv('/src/data/zk2/check1_mmm2.csv')
    df = df.sort_values('install_date')

    for media in mediaList:
        print(media)
        print('按天MAPE：',df['%s_mape'%(media)].mean())
        print('整体MAPE：',(df['%s'%(media)].mean() - df['%s_pred'%(media)].mean())/df['%s'%(media)].mean())

        df['%s_3d'%(media)] = df['%s'%(media)].rolling(3).mean()
        df['%s_pred_3d'%(media)] = df['%s_pred'%(media)].rolling(3).mean()
        df['%s_3d_mape'%(media)] = abs(df['%s_3d'%(media)] - df['%s_pred_3d'%(media)]) / df['%s_3d'%(media)]
        print('3日均线MAPE：',df['%s_3d_mape'%(media)].mean())

        df['%s_3d'%(media)] = df['%s'%(media)].rolling(7).mean()
        df['%s_pred_3d'%(media)] = df['%s_pred'%(media)].rolling(7).mean()
        df['%s_3d_mape'%(media)] = abs(df['%s_3d'%(media)] - df['%s_pred_3d'%(media)]) / df['%s_3d'%(media)]
        print('7日均线MAPE：',df['%s_3d_mape'%(media)].mean())

        df['%s_3d_ema'%(media)] = df['%s'%(media)].ewm(span=3).mean()
        df['%s_pred_3d_ema'%(media)] = df['%s_pred'%(media)].ewm(span=3).mean()
        df['%s_3d_ema_mape'%(media)] = abs(df['%s_3d_ema'%(media)] - df['%s_pred_3d_ema'%(media)]) / df['%s_3d_ema'%(media)]
        print('3日EMA MAPE：',df['%s_3d_ema_mape'%(media)].mean())

        df['%s_7d_ema'%(media)] = df['%s'%(media)].ewm(span=7).mean()
        df['%s_pred_7d_ema'%(media)] = df['%s_pred'%(media)].ewm(span=7).mean()
        df['%s_7d_ema_mape'%(media)] = abs(df['%s_7d_ema'%(media)] - df['%s_pred_7d_ema'%(media)]) / df['%s_7d_ema'%(media)]
        print('7日EMA MAPE：',df['%s_7d_ema_mape'%(media)].mean())

def draw():
    df = pd.read_csv('/src/data/zk2/check1_mmm2.csv')
    df = dateFilter(df)

    # 目前df 中列 install_date 是 str 类型，需要转换成 datetime 类型
    # 画图，install_date是x
    # 分媒体画图，目前的列install_date,Facebook Ads,bytedanceglobal_int,googleadwords_int,other,snapchat_int,googleadwords_int_pred,googleadwords_int_mape,Facebook Ads_pred,Facebook Ads_mape,bytedanceglobal_int_pred,bytedanceglobal_int_mape,snapchat_int_pred,snapchat_int_mape,other_pred,other_mape
    # 用媒体和媒体预测两个指标做y划线，比如facebook Ads和facebook Ads_pred
    # x点一个月一个就好，太密看不清
    # 保存早src/data/zk2/media.jpg下
    # 将install_date转换为datetime类型
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 设置绘图风格
    plt.style.use('seaborn-darkgrid')

    # 媒体列表
    media_list = [
        ('Facebook Ads', 'Facebook Ads_pred'),
        ('bytedanceglobal_int', 'bytedanceglobal_int_pred'),
        ('googleadwords_int', 'googleadwords_int_pred'),
        ('other', 'other_pred'),
        ('snapchat_int', 'snapchat_int_pred')
    ]

    # 颜色列表
    colors = [
        ('blue', 'cyan'),
        ('green', 'lime'),
        ('red', 'orange'),
        ('purple', 'magenta'),
        ('brown', 'yellow')
    ]

    for (media, media_pred), (color, pred_color) in zip(media_list, colors):
        # 创建一个新的图形，包含两个子图
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), sharex=True)

        # 第一个子图：绘制媒体的线条和预测线条
        ax1.plot(df['install_date'], df[media], label=media, linewidth=2, color=color)
        ax1.plot(df['install_date'], df[media_pred], label=media_pred, linewidth=2, color=pred_color)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax1.xaxis.set_major_locator(mdates.MonthLocator())
        ax1.legend()

        # 计算7日均线
        df[f'{media}_7d'] = df[media].rolling(7).mean()
        df[f'{media_pred}_7d'] = df[media_pred].rolling(7).mean()

        df[f'{media}_7d mape'] = abs(df[f'{media}_7d'] - df[f'{media_pred}_7d']) / df[f'{media}_7d']
        print(f"{media} 7-day average MAPE: {df[f'{media}_7d mape'].mean()}")

        # 第二个子图：绘制7日均线
        ax2.plot(df['install_date'], df[f'{media}_7d'], label=f'{media} 7-day average', linewidth=2, color=color)
        ax2.plot(df['install_date'], df[f'{media_pred}_7d'], label=f'{media_pred} 7-day average', linewidth=2, color=pred_color)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax2.xaxis.set_major_locator(mdates.MonthLocator())
        ax2.legend()

        # 旋转X轴刻度标签以避免重叠
        plt.xticks(rotation=45)

        # 保存图形到文件
        plt.savefig(f'/src/data/zk2/{media}_with_7d_avg.jpg', bbox_inches='tight')
        plt.close(fig)

# 尝试过滤一些时间，用短一点的时间段来训练模型，看看效果
# df中必须有install_date列
def dateFilter(df):
    df = df.loc[
        (df['install_date'] >= '2023-02-01') &
        (df['install_date'] <= '2023-03-01')
    ]

    return df


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning, module="keras")

    prepareData()
    
    debug3()

    # train1(rNUsd = 'r3usd_mmm')
    # check1(rNUsd = 'r3usd_mmm')

    # debug1()
    # debug2()
    
    # debug4()

    # mods = train2(rNUsd='r3usd_mmm', days_per_group=60)
    # check2(rNUsd='r3usd_mmm', days_per_group=60)

    # debug2()
    draw()


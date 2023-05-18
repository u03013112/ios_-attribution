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

import pandas as pd
import numpy as np

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
    userDf = pd.read_csv('/src/data/zk/attribution1ReStep6.csv')
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date
    # df列install_timestamp,cv,user_count,r1usd,r7usd,googleadwords_int count,Facebook Ads count,bytedanceglobal_int count,snapchat_int count
    # 新增加一列 'other count'
    userDf['other count'] = 1 - userDf[[media + ' count' for media in mediaList]].sum(axis=1)
    userDf.loc[userDf['other count']<0,'other count'] = 0
    print(userDf.head(10))
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
# def train1():
#     # 读取数据
#     df = pd.read_csv('/src/data/zk/check1_mmm.csv')
#     df = df[['install_date', 'media', 'r7usd_raw', 'r7usd_mmm']]

#     mediaList.append('other')
#     media_list = mediaList
#     media_count = len(media_list)

#     # 按照安装日期进行8:2分割
#     unique_install_dates = df['install_date'].unique()
#     train_dates, test_dates = train_test_split(unique_install_dates, test_size=0.2, random_state=42)

#     # 根据分割的安装日期划分训练集和测试集
#     train_df = df[df['install_date'].isin(train_dates)].sort_values('install_date')
#     test_df = df[df['install_date'].isin(test_dates)].sort_values('install_date')

#     # 准备训练和测试数据
#     train_pivot = train_df.pivot_table(index='install_date', columns='media', values='r7usd_mmm').reset_index()
#     test_pivot = test_df.pivot_table(index='install_date', columns='media', values='r7usd_mmm').reset_index()

#     # train_pivot,test_pivot 填充缺失值
#     train_pivot = train_pivot.fillna(0)
#     test_pivot = test_pivot.fillna(0)

#     X_train_list = [train_pivot[media].values.reshape(-1, 1) for media in media_list]
#     y_train = train_df.groupby('install_date')['r7usd_raw'].sum().values

#     X_test_list = [test_pivot[media].values.reshape(-1, 1) for media in media_list]
#     y_test = test_df.groupby('install_date')['r7usd_raw'].sum().values

#     # 创建模型
#     inputs_list = [Input(shape=(1,)) for _ in range(media_count)]
#     outputs_list = [Dense(1, activation='linear', bias_initializer=Zeros())(inputs) for inputs in inputs_list]
#     outputs_sum = Add()(outputs_list)
#     model = Model(inputs=inputs_list, outputs=outputs_sum)

#     # 编译模型
#     model.compile(optimizer=Adam(lr=0.001), loss='mse', metrics=['mape'])

#     early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
#     # 训练模型
#     history = model.fit(X_train_list, y_train, validation_data=(X_test_list, y_test), epochs=3000, batch_size=32,
#             callbacks=[early_stopping]
#         )

#     # 画出loss曲线
#     plt.plot(history.history['loss'], label='train_loss')
#     plt.plot(history.history['val_loss'], label='val_loss')
#     plt.xlabel('Epochs')
#     plt.ylabel('Loss')
#     plt.legend()
#     plt.savefig('/src/data/zk2/history1.jpg')
#     plt.show()

#     # 评估模型
#     test_mape = np.min(history.history['val_mape'])
#     print("Test MAPE: {:.4f}".format(test_mape))

#     # 输出5个媒体对应的ax+b中的a和b
#     all_weights = model.get_weights()
#     print(all_weights)
#     for i, media in enumerate(media_list):
#         a = all_weights[2 * i][0][0]
#         b = all_weights[2 * i + 1][0]
#         print(f"Media {media}: a = {a}, b = {b}")
#     # 保存模型
#     model.save('/src/data/zk2/model1.h5')

def train1(rNUsd = 'r1usd_mmm'):
    # 读取数据
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
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
    # inputs_list = [Lambda(lambda x: x[:, i:i+1])(inputs) for i in range(len(input_columns))]
    inputs_list = [Lambda(lambda x, i=i: x[:, i:i+1])(inputs) for i in range(5)]

    
    outputs_list = [Dense(1, activation='linear', bias_initializer=Zeros())(input) for input in inputs_list]
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
    all_weights = model.get_weights()
    for i, media in enumerate(input_columns):
        a = all_weights[2 * i][0][0]
        b = all_weights[2 * i + 1][0]
        print(f"Media {media}: a = {a}, b = {b}")
    # 保存模型
    model.save('/src/data/zk2/model1.h5')

from sklearn.metrics import r2_score,mean_absolute_percentage_error

from keras.models import load_model
# 对train1训练的模型进行验算
def check1(rNUsd = 'r1usd_mmm'):
    # 读取数据
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    df = df[['install_date', 'media', 'r7usd_raw', rNUsd]]

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values=rNUsd).reset_index()
    media_df = media_df.fillna(0)

    # 将y，即r7usd_raw的按天汇总做成df
    y_df = df.groupby('install_date')['r7usd_raw'].sum().reset_index()

    # 将两个df进行merge
    merged_df = pd.merge(media_df, y_df, on='install_date')

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
    model = load_model('/src/data/zk2/model1.h5')
    # 用此模型预测r7usd_pred，并计算与r7usd_raw的mape
    y_pred = model.predict(X)
    
    merged_df['r7usd_pred'] = y_pred
    merged_df['mape'] = abs(merged_df['r7usd_raw'] - merged_df['r7usd_pred']) / merged_df['r7usd_raw']
    merged_df.to_csv('/src/data/zk2/check1_mmm.csv', index=False)
    print("Global MAPE:", merged_df['mape'].mean())

    # 创建一个新的数据框，用于存储每个媒体的MAPE
    media_mape_df = pd.DataFrame(columns=['media', 'mape'])

    # 获取每个媒体的真实付费金额
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
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



def debug1():
    df = pd.read_csv('/src/data/zk/check1_mmm.csv')
    groupDf = df.groupby('install_date').agg('sum').reset_index()
    print(groupDf.corr())
    for media in mediaList:
        mediaDf = df[df['media'] == media]
        print(media)
        print(mediaDf.corr())

def debug2():
    df = pd.DataFrame(
        {
            'a':[1,2,3,4,5],
        }
    )
    random_factors = np.random.uniform(3.0, 5.0, len(df))
    random_b = np.random.uniform(0, 1.0, len(df))
    df['b'] = df['a'] * random_factors 
    # + random_b
    df['c'] = df['a'] * 4.0 
    # + 0.5

    print(mean_absolute_percentage_error(df['c'].values, df['b'].values))
    print(df.corr())

if __name__ == '__main__':
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning, module="keras")

    # prepareData()
    
    train1()
    check1()

    # debug1()
    # debug2()
    # debug3()



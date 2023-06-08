import pandas as pd
import numpy as np
from keras.constraints import MinMaxNorm, Constraint
from keras.models import Model
from keras.layers import Input, Dense, Add, Lambda
from keras.callbacks import EarlyStopping
from keras import backend as K
from tqdm import tqdm
import tensorflow as tf

class CustomWeightConstraint(Constraint):
    def __call__(self, w):
        return tf.clip_by_value(w, 0.8, 2.3)

def dateFilter(df):
    df = df.loc[
        (df['install_date'] >= '2022-01-01') &
        (df['install_date'] <= '2023-03-01')
    ]
    return df

def train_and_predict(df, train_days, predict_days,output_path):

    df = dateFilter(df)
    df = df[['install_date', 'media', 'r7usd_raw', 'r3usd_mmm']]

    # 按照安装日期进行汇总，并pivot_table
    media_df = df.pivot_table(index='install_date', columns='media', values='r3usd_mmm').reset_index()
    media_df = media_df.fillna(0)

    # 将y，即r7usd_raw的按天汇总做成df
    y_df = df.groupby('install_date')['r7usd_raw'].sum().reset_index()

    # 将两个df进行merge
    merged_df = pd.merge(media_df, y_df, on='install_date')

    # 按照安装日期排序
    merged_df = merged_df.sort_values('install_date')

    input_columns = ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']
    X = merged_df[input_columns].values
    y = merged_df['r7usd_raw'].values

    # 用于存储预测结果的 DataFrame
    predictions_df = pd.DataFrame(columns=['install_date', 'media', 'r7usd_raw', 'r7usd_pred'])

    # 进度条
    with tqdm(total=(len(merged_df) - train_days - predict_days) // predict_days, desc="Training and predicting") as pbar:
        # 循环取出连续 train_days 天的数据进行训练
        for i in range(0, len(merged_df) - train_days - predict_days, predict_days):
            X_train = X[i:i+train_days]
            y_train = y[i:i+train_days]

            # 创建模型
            inputs = Input(shape=(5,))
            inputs_list = [Lambda(lambda x, i=i: x[:, i:i+1])(inputs) for i in range(5)]

            outputs_list = [Dense(1, activation='linear', kernel_constraint=CustomWeightConstraint(), use_bias=False)(input) for input in inputs_list]

            outputs_sum = Add()(outputs_list)
            model = Model(inputs=inputs, outputs=outputs_sum)

            # 编译模型
            model.compile(optimizer='Adam', loss='mse', metrics=['mape'])

            # 训练模型，使用 EarlyStopping
            early_stopping = EarlyStopping(monitor='loss', patience=10, restore_best_weights=True)
            model.fit(X_train, y_train, epochs=3000, batch_size=16, validation_split=0.3,
                      callbacks=[early_stopping], 
                      verbose=0
            )

            # 用 train_days 天的模型预测接下来 predict_days 天的数据
            for j in range(predict_days):
                X_test = X[i+train_days+j].reshape(1, -1)
                y_test = y[i+train_days+j]

                # 将每个媒体的真实 7 日收入按照安装日期和媒体进行分组
                media_r7usd_raw = df.pivot_table(index='install_date', columns='media', values='r7usd_raw').reset_index()
                media_r7usd_raw = media_r7usd_raw.fillna(0)

                # 获取每个媒体的预测值
                for k, media in enumerate(input_columns):
                    X_single_media = np.zeros_like(X_test)
                    X_single_media[:, k] = X_test[:, k]
                    y_pred_single_media = model.predict(X_single_media)

                    # 获取每个媒体的真实 7 日收入
                    y_test_single_media = media_r7usd_raw.loc[i+train_days+j, media]

                    # 计算 MAPE
                    if y_test_single_media == 0:
                        mape = 0
                    else:
                        mape = abs(y_test_single_media - y_pred_single_media[0][0]) / y_test_single_media

                    # 添加预测结果到 predictions_df
                    predictions_df = predictions_df.append({
                        'install_date': merged_df.loc[i+train_days+j, 'install_date'],
                        'media': media,
                        'r7usd_raw': y_test_single_media,
                        'r7usd_pred': y_pred_single_media[0][0],
                        'mape': mape
                    }, ignore_index=True)

            # 更新进度条
            pbar.update(1)

    # 计算整体的MAPE和媒体的MAPE
    print("Global MAPE:", predictions_df['mape'].mean())

    # 计算并打印每个媒体的 MAPE
    media_mape = predictions_df.groupby('media')['mape'].mean()
    print("Media MAPE:")
    print(media_mape)

    # 保存预测结果到 CSV 文件
    predictions_df.to_csv(output_path, index=False)

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from datetime import datetime

def draw(df, output_path):
    # 将install_date列转换为日期类型
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 对每个媒体进行绘图
    for media in df['media'].unique():
        media_df = df[df['media'] == media]

        # 创建一个新的图形和轴，设置图形大小为宽 12 英寸，高 6 英寸
        fig, ax1 = plt.subplots(figsize=(12, 6))

        # 绘制r7usd_raw和r7usd_pred曲线
        ax1.plot(media_df['install_date'], media_df['r7usd_raw'], label='r7usd_raw', color='blue')
        ax1.plot(media_df['install_date'], media_df['r7usd_pred'], label='r7usd_pred', color='green')

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
    
def roll(df):
    # df 拥有列 install_date,media,r7usd_raw,r7usd_pred,mape
    # 按照媒体分组，再按照安装日期排序
    # 计算r7usd_raw和r7usd_pred的7日滚动平均值
    # 计算上面滚动平均值的mape
    # 按照 media mape 的类似方式打印出来
    # 将install_date列转换为日期类型
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 重置索引，以避免歧义
    df = df.reset_index(drop=True)

    # 对每个媒体分组并按安装日期排序
    grouped = df.groupby('media').apply(lambda x: x.sort_values('install_date')).reset_index(drop=True)

    # 计算r7usd_raw和r7usd_pred的7日滚动平均值
    grouped['r7usd_raw_rolling'] = grouped.groupby('media')['r7usd_raw'].rolling(window=7).mean().reset_index(drop=True)
    grouped['r7usd_pred_rolling'] = grouped.groupby('media')['r7usd_pred'].rolling(window=7).mean().reset_index(drop=True)

    # 计算滚动平均值的mape
    grouped['rolling_mape'] = abs(grouped['r7usd_raw_rolling'] - grouped['r7usd_pred_rolling']) / grouped['r7usd_raw_rolling'] * 100

    # 按照 media mape 的类似方式打印结果
    for media in grouped['media'].unique():
        media_df = grouped[grouped['media'] == media]
        print(f"Media: {media}")
        # print(media_df[['install_date', 'r7usd_raw_rolling', 'r7usd_pred_rolling', 'rolling_mape']])
        # print("\n")
        # 只获取rolling_mape不为NaN的行
        media_df = media_df[media_df['rolling_mape'].notna()]
        print('MAPE:',media_df['rolling_mape'].mean())
    
    print('--------------')

if __name__ == '__main__':
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning, module="keras")

    # for input_path in ['/src/data/zk/check1_mmm.csv','/src/data/zk/check1_mmm2.csv']:
    #     minMape = 100
    #     for train_days in [14,28,60]:
    #         for predict_days in [7,14,28]:

    #             output_path = '/src/data/zk2/prediction2_{}_{}_{}.csv'.format(input_path.split('/')[-1].split('.')[0],train_days,predict_days)
    #             # print(output_path,'start')
    #             # df = pd.read_csv(input_path)

    #             # train_and_predict(df, train_days, predict_days, output_path)

    #             df = pd.read_csv(output_path)
    #             if input_path == '/src/data/zk/check1_mmm.csv':
    #                 print('24小时版本')
    #             else:
    #                 print('48小时版本')

    #             print('train_days:',train_days,'predict_days:',predict_days)
                
    #             dfGroup = df.groupby('install_date').agg({'r7usd_raw':'sum','r7usd_pred':'sum'})
    #             dfGroup['mape'] = abs(dfGroup['r7usd_raw']-dfGroup['r7usd_pred'])/dfGroup['r7usd_raw']
    #             mape = dfGroup['mape'].mean()
    #             print('大盘MAPE：',mape)
    #             if mape < minMape:
    #                 minMape = mape

    #             for media in ['googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'other']:
    #                 print(media,df[df['media']==media]['mape'].mean())
    #             print('------------------')
        
    #     print('最小MAPE：',minMape)

    file24 = '/src/data/zk2/prediction2_check1_mmm_28_7.csv'
    file48 = '/src/data/zk2/prediction2_check1_mmm2_28_7.csv'

    df24 = pd.read_csv(file24)
    df48 = pd.read_csv(file48)

    # draw(df24,'/src/data/zk2/24')
    # draw(df48,'/src/data/zk2/48')
    roll(df24)
    roll(df48)


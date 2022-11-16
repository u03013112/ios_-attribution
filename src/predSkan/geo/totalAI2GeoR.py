# 尝试进行整体预测
# 按地区进行汇总
# 改为EMA数据来做预测
# 测试集仍用原始数据
# 尝试用最近N天的数据进行盲训
import pandas as pd
import numpy as np
import sys
sys.path.append('/src')
from src.predSkan.data import getTotalDataGroupByGeo,getTotalDataGroupByGeoMax
from src.tools import getFilename
from src.googleSheet import GSheet
# 暂定方案是先将数据分组，比如直接分为64组
groupList = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31],[32],[33],[34],[35],[36],[37],[38],[39],[40],[41],[42],[43],[44],[45],[46],[47],[48],[49],[50],[51],[52],[53],[54],[55],[56],[57],[58],[59],[60],[61],[62],[63]]
geoList = [
    {'name':'US','codeList':['US']},
    {'name':'T1','codeList':['CA','AU','GB','UK','NZ','DE','FR']},
    {'name':'KR','codeList':['KR']},
    {'name':'GCC','codeList':['AE','BH','KW','OM','QA','ZA','SA']}
]
import datetime
# 各种命名都用这个后缀，防止重名
filenameSuffix = datetime.datetime.now().strftime('_%Y%m%d_%H')

# 从maxCompute取数据
def dataStep0(sinceTimeStr,unitlTimeStr):
    df = getTotalDataGroupByGeo(sinceTimeStr,unitlTimeStr)
    df.to_csv(getFilename('totalGeoData%s_%s'%(sinceTimeStr,unitlTimeStr)))

# 从本地文件取数据，跑过步骤0的可以直接从这里开始，更快速
def dataStep1(sinceTimeStr,unitlTimeStr):
    df = pd.read_csv(getFilename('totalGeoData%s_%s'%(sinceTimeStr,unitlTimeStr)))
    return df

def dataStep0Max(sinceTimeStr,unitlTimeStr):
    df = getTotalDataGroupByGeoMax(sinceTimeStr,unitlTimeStr,max=500.0)
    df.to_csv(getFilename('totalGeoMaxData%s_%s'%(sinceTimeStr,unitlTimeStr)))

def dataStep1Max(sinceTimeStr,unitlTimeStr):
    df = pd.read_csv(getFilename('totalGeoMaxData%s_%s'%(sinceTimeStr,unitlTimeStr)))
    return df


# 对每组数据分别整理
def dataStep2(dataDf1):
    dataDf1.insert(dataDf1.shape[1],'group',0)
    for i in range(len(groupList)):
        l = groupList[i]
        for cv in l:
            dataDf1.loc[dataDf1.cv == cv,'group'] = i
    return dataDf1

# 添加geo属性
def dataStep3(dataDf2):
    dataDf2.insert(dataDf2.shape[1],'geo','unknown')
    for geo in geoList:
        name = geo['name']
        for code in geo['codeList']:
            dataDf2.loc[dataDf2.country_code == code,'geo'] = name
    return dataDf2

# 对数据做基础处理
def dataStep4(dataDf3):
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf3['install_date'].unique()
    for install_date in install_date_list:
        print(install_date)
        df = dataDf3.loc[(dataDf3.install_date == install_date)]
        dataNeedAppend = {
            'install_date':[],
            'count':[],
            'sumr7usd':[],
            'group':[],
            'geo':[]
        }
        for i in range(len(groupList)):
            for geo in geoList:
                name = geo['name']
                # 这里要为每一个geo做补充
                if df.loc[(df.group == i) & (df.geo == name),'sumr7usd'].sum() == 0 \
                    and df.loc[(df.group == i) & (df.geo == name),'count'].sum() == 0:

                    dataNeedAppend['install_date'].append(install_date)
                    dataNeedAppend['count'].append(0)
                    dataNeedAppend['sumr7usd'].append(0)
                    dataNeedAppend['group'].append(i)
                    dataNeedAppend['geo'].append(name)

        dataDf3 = dataDf3.append(pd.DataFrame(data=dataNeedAppend))
    return dataDf3

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

def createModFunc3():
    mod = keras.Sequential(
        [
            layers.Dense(512, kernel_initializer='random_normal',bias_initializer='zeros',activation="relu", input_shape=(64,)),
            layers.Dropout(0.3),
            layers.Dense(512, kernel_initializer='random_normal',bias_initializer='zeros',activation="relu"),
            layers.Dropout(0.3),
            layers.Dense(1, kernel_initializer='random_normal',bias_initializer='zeros',activation="relu")
        ]
    )
    mod.compile(optimizer='adadelta',loss='mape')
    return mod

epochMax = 3000
lossAndErrorPrintingCallbackSuffixStr = ''
class LossAndErrorPrintingCallback(keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        global lossAndErrorPrintingCallbackSuffixStr
        if epoch > 0 and epoch%100 == 0:
            keys = list(logs.keys())
            str = 'epoch %d/%d:'%(epoch,epochMax)
            for key in keys:
                str += '[%s]:%.2f '%(key,logs[key])
            print(lossAndErrorPrintingCallbackSuffixStr,str)

def dataAddEMA(df,day=3):
    df.insert(df.shape[1],'ema',0)
    df['ema'] = df['sumr7usd'].ewm(span=day).mean()
    return df

from sklearn.preprocessing import StandardScaler
def train(dataDf3):
    # n是向前取n天的数据进行预测
    n = 7
    global lossAndErrorPrintingCallbackSuffixStr
    data = {
        'geo':[],
        'install_date':[],
        'pred':[]
    }
    earlyStoppingLoss = tf.keras.callbacks.EarlyStopping(
        monitor='loss', 
        patience=5,
        min_delta=1,
    )
    for geo in geoList:
        name = geo['name']
        if name != 'US':
            continue
        sinceTimeStr = '2022-07-01'
        unitlTimeStr = '2022-09-30'
        sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y-%m-%d')
        unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y-%m-%d')
        # for i in range((unitlTime - sinceTime).days + 1):
        i = 0
        while i < (unitlTime - sinceTime).days + 1:
            day = sinceTime + datetime.timedelta(days=i)
            dayStr = day.strftime('%Y-%m-%d')
            # if dayStr != '2022-08-10':
            #     i+=1
            #     continue
            # 由于需要7日数据，所以最近的满7日数据应该是T-6
            # n日数据需要从T-6-n+1开始，至T-6
            day0 = day - datetime.timedelta(days= n+6-1)
            day1 = day - datetime.timedelta(days= 6)
            day0Str = day0.strftime('%Y-%m-%d')
            day1Str = day1.strftime('%Y-%m-%d')
            print('针对%s的预测，训练数据取自：%s~%s'%(dayStr,day0Str,day1Str))

            trainDf = dataDf3.loc[
                (dataDf3.install_date >= day0Str) & (dataDf3.install_date <= day1Str) & (dataDf3.geo == name)
            ].sort_values(by=['install_date','group'])
            trainDf = trainDf.groupby(['install_date','group']).agg('sum')
            trainX = trainDf['count'].to_numpy().reshape((-1,64))
            trainSumByDay = trainDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
            trainSumByDay = dataAddEMA(trainSumByDay,3)
            trainY = trainSumByDay['ema'].to_numpy()

            mod = createModFunc3()
            
            history = mod.fit(trainX, trainY, epochs=epochMax
                # ,callbacks=[earlyStoppingLoss,earlyStoppingValLoss]
                # ,callbacks=[earlyStoppingValLoss]
                # ,callbacks=[LossAndErrorPrintingCallback()]
                # ,callbacks=[earlyStoppingLoss,LossAndErrorPrintingCallback()]
                ,batch_size=128
                ,verbose=0
                )
            loss = history.history['loss'][-1]
            # if loss > 25:
            #     print('loss %.2f retry'%loss)
            #     continue
            
            testDf = dataDf3.loc[
                (dataDf3.install_date == dayStr) & (dataDf3.geo == name)
            ].sort_values(by=['install_date','group'])
            testDf = testDf.groupby(['install_date','group']).agg('sum')
            testX = testDf['count'].to_numpy().reshape((-1,64))
            testSumByDay = testDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
            yTrue = testSumByDay['sumr7usd'].to_numpy().reshape(-1)[0]
            # print('test shape:',testX.shape)
            # print(testX)
            yPred = mod.predict(testX).reshape(-1)[0]
            print('%s%s loss:%.2f 预测结果：%.2f，真实结果：%.2f，mape=%.2f%%'%(name,dayStr,loss,yPred,yTrue,np.abs((yPred - yTrue) / yTrue)* 100))
            if yPred < 10:
                print('retry')
                continue

            data['geo'].append(name)
            data['install_date'].append(dayStr)
            data['pred'].append(yPred)
            
            tf.keras.backend.clear_session()
            del mod

            i += 1
    
    return pd.DataFrame(data = data)


if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        # dataStep0('20220501','20220930')
        df = dataStep1('20220501','20220930')
        df2 = dataStep2(df)
        df3 = dataStep3(df2)
        df4 = dataStep4(df3)
        df4.to_csv(getFilename('totalGeoDataSum_20220501_20220930'))
    df4 = pd.read_csv(getFilename('totalGeoDataSum_20220501_20220930'))

    retDf = train(df4)
    retDf.to_csv(getFilename('totalGeoR_20220701_20220930'))

import numpy as np
import datetime
import tensorflow as tf
import pandas as pd

import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.tools import getFilename

from src.predSkan.total.totalAI0 import groupList,dataStep0,dataStep1,dataStep2

from src.predSkan.total.totalAI2 import dataStep3

def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100
    
# 然后对整体做测试
# def test(dataDf2,modList):
#     sinceTimeStr = '20220901'
#     unitlTimeStr = '20220930'
#     sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
#     unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')

#     # 为了画图用，将每一个group单独统计出来,二维数组，第一维度是0~63，第二维度是每天一个样本
#     count_list = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
#     y_true = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
#     y_pred = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    
#     for group in range(len(groupList)):
#         mod = modList[group]
        
#         for d in range((unitlTime - sinceTime).days + 1):
#             day = sinceTime + datetime.timedelta(days=d)
#             dayStr = day.strftime('%Y-%m-%d')

#             df = dataDf2.loc[(dataDf2.install_date == dayStr) & (dataDf2.group == group)]
#             # count 就是预测的input
#             count = df['count'].sum()
#             count_list[group].append(count)
#             if count == 0:
#                 # 没有这种，就不预测
#                 y_true[group].append(0.01)
#                 y_pred[group].append(0.01)
#                 continue
            
#             yt = df['sumr7usd'].sum()
#             if yt == 0:
#                 # 真实付费金额为0
#                 yt = 0.01
#             x = np.array([count])
#             yp = mod.predict(x).reshape(-1).sum()

#             y_true[group].append(yt)
#             y_pred[group].append(yp)

#     # print(y_true)
#     # print(y_pred)
    

#     y_true_np = np.array(y_true)
#     y_pred_np = np.array(y_pred)

#     # 按cv计算
#     # for cv in range(y_true_np.shape[0]):
#     #     yt = y_true_np[cv]
#     #     yp = y_pred_np[cv]
#     #     print('cv = %d,mape=%.2f%%'%(cv,mapeFunc(yt,yp)))
#     # 按天分开计算
#     # for day in range(y_true_np.shape[1]):
#     #     yt = y_true_np[:,day]
#     #     yp = y_pred_np[:,day]
#     #     print('day = %d,mape=%.2f%%'%(day,mapeFunc(yt,yp)))


#     yt = np.sum(y_true_np,axis=1)
#     yp = np.sum(y_pred_np,axis=1)
#     print('with cv mape=%.2f%%'%(mapeFunc(yt,yp)))
#     # print(yt)
#     # print(yp)

#     yd = np.abs(yp-yt)
#     sum = np.sum(yd)
#     ydp = yd/sum
#     for i in range(len(ydp)):
#         print('%d,%.2f'%(i,ydp[i]))

#     # yt = np.sum(y_true_np,axis=0)
#     # yp = np.sum(y_pred_np,axis=0)
#     # print('with day mape=%.2f%%'%(mapeFunc(yt,yp)))

#     # print(np.sum(y_true_np.reshape(-1)))
#     # print(np.sum(y_pred_np.reshape(-1)))

#     for i in range(len(groupList)):
#         count = count_list[i]
#         yTrue = y_true[i]
#         yPred = y_pred[i]
#         plt.title("cv = %d"%i) 
#         plt.xlabel("count") 
#         plt.ylabel("true blue,pred red") 
#         plt.plot(count,yTrue,'bo')
#         plt.plot(count,yPred,'ro')
#         plt.savefig('/src/data/testCv%d.png'%(i))
#         print('save pic /src/data/testCv%d.png'%(i))
#         plt.clf()

from sklearn import metrics
def test2():
    modName = '/src/src/predSkan/mod/mod02582-23.44.h5'
    df = dataStep0('20220901','20221020')
    df = dataStep1('20220901','20221020')
    df2 = dataStep2(df)
    dataDf3 = dataStep3(df2)

    mod = tf.keras.models.load_model(modName)

    testDf = dataDf3.loc[(dataDf3.install_date >= '2022-09-01')].sort_values(by=['install_date','group'])
    testX = testDf['count'].to_numpy().reshape((-1,64))
    testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
    testY = testSumByDay.to_numpy()

    yp = mod.predict(testX)
    print('mape:%.2f%%'%(mapeFunc(testY,yp)))
    r2 = metrics.r2_score(testY,yp)
    print('r2:%.2f%%'%(r2))

    x = np.arange(len(yp))
    plt.title("total ai 2") 
    plt.xlabel("date 2022-09-01~2022-10-20 ") 
    plt.ylabel("true blue,pred red") 
    plt.plot(x,testY.reshape(-1),'b-')
    plt.plot(x,yp.reshape(-1),'r-')
    plt.savefig('/src/data/testT2.png')
    print('save pic /src/data/testT2.png')
    plt.clf()
    
    print(yp.reshape(-1))
    print(testY.reshape(-1))
    

    cost = np.array([57034.42,95750.57,126744.26,143489.50,142104.70,134276.70,127092.36,117801.28,119383.61,131279.43,130407.37,117082.42,113094.84,110632.02,110886.79,119084.99,132653.75,137010.41,137844.95,137573.00,126642.97,122268.80,130570.28,147392.67,155731.68,143962.89,135094.40,124948.44,116235.33,129845.26,139265.62,143787.70,141225.32,123568.31,124753.01,123984.18,128913.85,146879.03,153982.17,163569.30,171097.67,161002.65,162102.65,162922.89,166237.05,180182.01,175119.18,174755.06,173332.52,177929.20])
    plt.title("roi ai 2") 
    plt.xlabel("date 2022-09-01~2022-10-20 ") 
    plt.ylabel("true blue,pred red") 
    plt.plot(x,np.divide(testY.reshape(-1),cost),'b-')
    plt.plot(x,np.divide(yp.reshape(-1),cost),'r-')
    plt.savefig('/src/data/testT2ROI.png')
    print('save pic /src/data/testT2ROI.png')

from src.predSkan.totalAI2NDay import dayList,dataStep0 as nds0,dataStep1 as nds1

def testNDay():
    # mods = {
    #     '1':'mod1_mod3__20221109_0209981-23.35.h5',
    #     '2':'mod2_mod3__20221109_0201005-23.35.h5',
    #     '3':'mod3_mod3__20221109_0200938-23.35.h5',
    #     '4':'mod4_mod3__20221109_0200915-23.35.h5',
    #     '5':'mod5_mod3__20221109_0200498-23.37.h5',
    #     '6':'mod6_mod3__20221109_0205724-23.37.h5',
    # }
    mods = {
        '1':'mod1_mod3__20221109_0209981-23.35.h5',
        '2':'mod1_mod3__20221109_0209981-23.35.h5',
        '3':'mod1_mod3__20221109_0209981-23.35.h5',
        '4':'mod1_mod3__20221109_0209981-23.35.h5',
        '5':'mod1_mod3__20221109_0209981-23.35.h5',
        '6':'mod1_mod3__20221109_0209981-23.35.h5',
    }

    for day in dayList:
        print ('day:',day)
        if __debug__:
            print('debug 模式，并未真的sql')
        else:
            nds0('20220901','20221020',n=day)
        df = nds1('20220901','20221020',n=day)
        df2 = dataStep2(df)
        df3 = dataStep3(df2)

        modName = '/src/src/predSkan/mod/nday/' + mods[str(day)]
    

        mod = tf.keras.models.load_model(modName)

        testDf = df3.loc[(df3.install_date >= '2022-09-01')].sort_values(by=['install_date','group'])
        testX = testDf['count'].to_numpy().reshape((-1,64))
        testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
        testY = testSumByDay.to_numpy()

        yp = mod.predict(testX)
        print('mape:%.2f%%'%(mapeFunc(testY,yp)))
        r2 = metrics.r2_score(testY,yp)
        print('r2:%.2f%%'%(r2))
        # print('y_true:',testY.reshape(-1))
        print('y_pred:',list(yp.reshape(-1)))

        x = np.arange(len(yp))
        plt.title("total ai %dDay"%(day)) 
        plt.xlabel("date 2022-09-01~2022-10-20 ") 
        plt.ylabel("true blue,pred red") 
        plt.plot(x,testY.reshape(-1),'b-')
        plt.plot(x,yp.reshape(-1),'r-')
        plt.savefig('/src/data/testNDay%d.png'%day)
        print('save pic /src/data/testNDay%d.png'%day)
        plt.clf()


def test():
    modName = '/src/src/predSkan/mod/nday/mod1_mod3__20221109_0209981-23.35.h5'

    mod = tf.keras.models.load_model(modName)

    testX = np.zeros(64*64).reshape(64,64)

    for i in range(64):
        testX[i][i] = 10000

    print(testX)
    yp = mod.predict(testX)
    print(list(yp.reshape(-1)))


def testR():
    # totalR3_20220701_20220930
    retDf = pd.read_csv(getFilename('totalR3_20220701_20220930'))
    df4 = pd.read_csv(getFilename('totalDataSum_20220501_20220930'))
    
    yTrueDf = df4.loc[(df4.install_date >= '2022-07-01') & (df4.install_date <= '2022-09-30')].sort_values(by=['install_date','group'])
    trainSumByDay = yTrueDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    # print('trainSumByDay:',trainSumByDay)
    # print('retDf:',retDf)
    yTrue = trainSumByDay['sumr7usd'].to_numpy()
    y1True = trainSumByDay['sumr1usd'].to_numpy()
    yPred = retDf['pred'].to_numpy()
    print('mape:%.2f%%'%(mapeFunc(yTrue,yPred)))
    r2 = metrics.r2_score(yTrue,yPred)
    print('r2:%.2f%%'%(r2))

    plt.title("total ai R") 
    plt.xlabel("date 2022-07-01~2022-09-30 ") 
    plt.ylabel("usd") 
    plt.plot(yTrue.reshape(-1),label='true 7d')
    # plt.plot(y1True.reshape(-1),label='true 1d')
    plt.plot(yPred.reshape(-1),label='pred')
    
    plt.legend()
    plt.savefig('/src/data/testTotalR.png')
    print('save pic /src/data/testTotalR.png')
    plt.clf()

if __name__ == '__main__':
    f1min = np.load('/src/data/doc/total//total_20230209_090643/min.npy')
    f2min = np.load('/src/data/doc/total//total_20230209_090005/min.npy')

    print(f1min)
    print(f2min)
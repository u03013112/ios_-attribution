import numpy as np
import datetime
import tensorflow as tf
import pandas as pd

import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.tools import getFilename

from src.predSkan.totalAI0 import groupList,dataStep1,dataStep2

from src.predSkan.totalAI2 import dataStep3

def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100
    
# 然后对整体做测试
def test(dataDf2,modList):
    sinceTimeStr = '20220901'
    unitlTimeStr = '20220930'
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')

    # 为了画图用，将每一个group单独统计出来,二维数组，第一维度是0~63，第二维度是每天一个样本
    count_list = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    y_true = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    y_pred = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    
    for group in range(len(groupList)):
        mod = modList[group]
        
        for d in range((unitlTime - sinceTime).days + 1):
            day = sinceTime + datetime.timedelta(days=d)
            dayStr = day.strftime('%Y-%m-%d')

            df = dataDf2.loc[(dataDf2.install_date == dayStr) & (dataDf2.group == group)]
            # count 就是预测的input
            count = df['count'].sum()
            count_list[group].append(count)
            if count == 0:
                # 没有这种，就不预测
                y_true[group].append(0.01)
                y_pred[group].append(0.01)
                continue
            
            yt = df['sumr7usd'].sum()
            if yt == 0:
                # 真实付费金额为0
                yt = 0.01
            x = np.array([count])
            yp = mod.predict(x).reshape(-1).sum()

            y_true[group].append(yt)
            y_pred[group].append(yp)

    # print(y_true)
    # print(y_pred)
    

    y_true_np = np.array(y_true)
    y_pred_np = np.array(y_pred)

    # 按cv计算
    # for cv in range(y_true_np.shape[0]):
    #     yt = y_true_np[cv]
    #     yp = y_pred_np[cv]
    #     print('cv = %d,mape=%.2f%%'%(cv,mapeFunc(yt,yp)))
    # 按天分开计算
    # for day in range(y_true_np.shape[1]):
    #     yt = y_true_np[:,day]
    #     yp = y_pred_np[:,day]
    #     print('day = %d,mape=%.2f%%'%(day,mapeFunc(yt,yp)))


    yt = np.sum(y_true_np,axis=1)
    yp = np.sum(y_pred_np,axis=1)
    print('with cv mape=%.2f%%'%(mapeFunc(yt,yp)))
    # print(yt)
    # print(yp)

    # yt=np.array([2.89394912e+04,6.60816235e+03,7.78579514e+03,6.84101179e+03
    #     ,7.83648611e+03,8.64721086e+03,9.67374213e+03,4.65266673e+03
    #     ,3.51777904e+03,4.02555079e+03,3.33068479e+03,3.14253099e+03
    #     ,1.98811954e+03,2.33464769e+03,3.74249127e+03,2.67770962e+03
    #     ,1.85020417e+03,1.61907110e+03,1.73728551e+03,2.26360601e+03
    #     ,3.51812914e+03,1.97131844e+03,1.36022815e+03,1.89087448e+03
    #     ,5.95969855e+02,1.36732119e+03,1.87125393e+03,1.01710751e+03
    #     ,1.15790170e+03,2.10302867e+03,1.00965207e+03,1.99623278e+03
    #     ,3.66653238e+03,3.32346644e+03,1.27424683e+03,1.36662798e+03
    #     ,1.57432456e+03,1.83321466e+03,1.67092664e+03,1.05833604e+03
    #     ,1.39321879e+03,2.99820287e+03,1.10495598e+03,2.67921402e+02
    #     ,5.59109929e+02,1.87515997e+03,3.00000000e-01,3.14327595e+02
    #     ,6.47857898e+02,7.68844694e+02,9.02454738e+02,7.05318058e+02
    #     ,1.72427511e+03,1.54418129e+02,1.44272213e+04,5.36022245e+02
    #     ,1.22049198e+03,3.00000000e-01,2.00889995e+02,5.90074017e+02
    #     ,1.93530023e+02,3.00000000e-01,2.75989996e+02,2.27323041e+04])

    # yp = np.array([2.22333766e+04,4.65687671e+03,5.36640939e+03,6.31759676e+03
    #     ,5.08221133e+03,7.28595422e+03,4.41787329e+03,2.34704986e+03
    #     ,1.96951307e+03,1.49278460e+03,1.65087294e+03,1.29168067e+03
    #     ,1.25986076e+03,1.26451250e+03,1.05695658e+03,8.21708398e+02
    #     ,7.50489857e+02,5.61516544e+02,5.30928486e+02,9.48960023e+02
    #     ,9.91842376e+02,4.86104733e+02,6.92839603e+02,7.80534438e+02
    #     ,3.32438351e+02,6.88476326e+02,4.43288203e+02,6.14185033e+02
    #     ,5.32979007e+02,7.16498496e+02,6.37207803e+02,1.06151819e+03
    #     ,7.41956134e+02,9.44946243e+02,4.33307060e+02,7.00773263e+02
    #     ,5.83449692e+02,5.96496271e+02,6.60454325e+02,4.58848213e+02
    #     ,3.16244556e+02,3.00516895e+02,5.29065689e+02,2.20530626e+02
    #     ,3.43133068e+02,4.36611019e+02,3.00000000e-01,1.24263152e+02
    #     ,3.86732769e+02,4.61096742e+02,5.05549633e+02,2.87866700e+02
    #     ,3.06528627e+02,1.54513129e+02,7.55646414e+02,3.50262239e+02
    #     ,5.42981838e+02,3.00000000e-01,1.84301963e+02,4.35321260e+02
    #     ,1.93916572e+02,3.00000000e-01,2.76015922e+02,9.13247965e+03])

    yd = np.abs(yp-yt)
    sum = np.sum(yd)
    ydp = yd/sum
    for i in range(len(ydp)):
        print('%d,%.2f'%(i,ydp[i]))

    # yt = np.sum(y_true_np,axis=0)
    # yp = np.sum(y_pred_np,axis=0)
    # print('with day mape=%.2f%%'%(mapeFunc(yt,yp)))

    # print(np.sum(y_true_np.reshape(-1)))
    # print(np.sum(y_pred_np.reshape(-1)))

    for i in range(len(groupList)):
        count = count_list[i]
        yTrue = y_true[i]
        yPred = y_pred[i]
        plt.title("cv = %d"%i) 
        plt.xlabel("count") 
        plt.ylabel("true blue,pred red") 
        plt.plot(count,yTrue,'bo')
        plt.plot(count,yPred,'ro')
        plt.savefig('/src/data/testCv%d.png'%(i))
        print('save pic /src/data/testCv%d.png'%(i))
        plt.clf()

from sklearn import metrics
def test2():
    modName = '/src/src/predSkan/mod/mod02582-23.44.h5'
    df = dataStep1('20220501','20220930')
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
    plt.xlabel("date 2022-09-01~2022-09-30 ") 
    plt.ylabel("true blue,pred red") 
    plt.plot(x,testY.reshape(-1),'b-')
    plt.plot(x,yp.reshape(-1),'r-')
    plt.savefig('/src/data/testT2.png')
    print('save pic /src/data/testT2.png')

if __name__ == '__main__':
    # modNameList = [
    #     '/src/src/predSkan/mod/mTotal0_mod1_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal1_mod1_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal2_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal3_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal4_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal5_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal6_mod1_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal7_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal8_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal9_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal10_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal11_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal12_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal13_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal14_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal15_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal16_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal17_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal18_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal19_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal20_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal21_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal22_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal23_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal24_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal25_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal26_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal27_mod3_20221021_02.h5',
    #     '/src/src/predSkan/mod/mTotal28_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal29_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal30_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal31_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal32_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal33_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal34_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal35_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal36_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal37_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal38_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal39_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal40_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal41_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal42_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal43_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal44_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal45_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal46_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal47_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal48_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal49_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal50_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal51_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal52_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal53_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal54_mod5_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal55_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal56_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal57_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal58_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal59_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal60_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal61_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal62_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal63_mod4_20221022_15.h5',
    # ]
    
    # modNameList = [
    #     '/src/src/predSkan/mod/mTotal0_mod1_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal1_mod1_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal2_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal3_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal4_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal5_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal6_mod1_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal7_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal8_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal9_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal10_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal11_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal12_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal13_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal14_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal15_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal16_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal17_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal18_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal19_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal20_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal21_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal22_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal23_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal24_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal25_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal26_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal27_mod3_20221021_02.h5',
    #     '/src/src/predSkan/mod/mTotal28_mod3_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal29_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal30_mod3_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal31_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal32_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal33_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal34_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal35_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal36_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal37_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal38_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal39_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal40_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal41_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal42_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal43_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal44_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal45_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal46_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal47_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal48_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal49_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal50_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal51_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal52_mod4_20221025_10.h5',
    #     '/src/src/predSkan/mod/mTotal53_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal54_mod4_20221025_10.h5',
    #     '/src/src/predSkan/mod/mTotal55_mod4_20221025_07.h5',
    #     '/src/src/predSkan/mod/mTotal56_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal57_mod4_20221025_08.h5',
    #     '/src/src/predSkan/mod/mTotal58_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal59_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal60_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal61_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal62_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal63_mod4_20221025_11.h5',
    # ]
    
    # modList = []
    # for modName in modNameList:
    #     mod = tf.keras.models.load_model(modName)
    #     modList.append(mod)

    

    # df = dataStep1('20220501','20220930')
    # df2 = dataStep2(df)
    # test(df2,modList)

    test2()
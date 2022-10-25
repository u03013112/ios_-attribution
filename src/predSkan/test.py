import numpy as np
import datetime
import tensorflow as tf
import pandas as pd

import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.tools import getFilename

from src.predSkan.totalAI0 import groupList,dataStep1,dataStep2

# 然后对整体做测试
def test(dataDf2,modList):
    sinceTimeStr = '20220901'
    unitlTimeStr = '20220930'
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')

    y_true = np.array([])
    y_pred = np.array([])
    # 为了画图用，将每一个group单独统计出来,二维数组，第一维度是0~63，第二维度是每天一个样本
    x_i = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    y_true_i = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    y_pred_i = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')

        y_true_day = 0
        y_pred_day = 0

        df = dataDf2.loc[(dataDf2.install_date == dayStr)]
        for i in range(len(groupList)):
            # count 就是预测的input
            count = df.loc[df.group == i,'count'].sum()
            if count == 0:
                # 没有这种，就不预测
                continue
            x = np.array([count])
            mod = modList[i]

            yt = df.loc[df.group == i,'sumr7usd'].sum()
            yp = mod.predict(x).reshape(-1).sum()
            y_true_day += yt
            y_pred_day += yp

            x_i[i].append(count)
            y_true_i[i].append(yt)
            y_pred_i[i].append(yp)

        y_pred = np.append(y_pred,y_pred_day)
        # y_true_day = dataDf2.loc[dataDf2.install_date == dayStr,'sumr7usd'].sum()
        y_true = np.append(y_true,y_true_day)
    
    # print(y_true.shape,y_pred.shape)

    def mapeFunc(y_true, y_pred):
        return np.mean(np.abs((y_pred - y_true) / y_true)) * 100
    
    mape = mapeFunc(y_true,y_pred)
    print(mape)

    for i in range(len(groupList)):
        count = x_i[i]
        y_true = y_true_i[i]
        y_pred = y_pred_i[i]
        plt.title("cv = %d"%i) 
        plt.xlabel("count") 
        plt.ylabel("true blue,pred red") 
        plt.plot(count,y_true,'bo')
        plt.plot(count,y_pred,'ro')
        plt.savefig('/src/data/testCv%d.png'%(i))
        print('save pic /src/data/testCv%d.png'%(i))
        plt.clf()

if __name__ == '__main__':
    modNameList = [
        '/src/src/predSkan/mod/mTotal0_mod4_20221024_02.h5',
        '/src/src/predSkan/mod/mTotal1_mod1_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal2_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal3_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal4_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal5_mod3_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal6_mod1_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal7_mod3_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal8_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal9_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal10_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal11_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal12_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal13_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal14_mod3_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal15_mod3_20221024_02.h5',
        '/src/src/predSkan/mod/mTotal16_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal17_mod4_20221024_02.h5',
        '/src/src/predSkan/mod/mTotal18_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal19_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal20_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal21_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal22_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal23_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal24_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal25_mod3_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal26_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal27_mod3_20221021_02.h5',
        '/src/src/predSkan/mod/mTotal28_mod3_20221024_02.h5',
        '/src/src/predSkan/mod/mTotal29_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal30_mod3_20221024_02.h5',
        '/src/src/predSkan/mod/mTotal31_mod3_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal32_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal33_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal34_mod3_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal35_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal36_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal37_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal38_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal39_mod3_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal40_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal41_mod3_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal42_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal43_mod4_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal44_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal45_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal46_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal47_mod4_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal48_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal49_mod3_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal50_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal51_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal52_mod3_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal53_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal54_mod1_20221025_08.h5',
        '/src/src/predSkan/mod/mTotal55_mod4_20221025_07.h5',
        '/src/src/predSkan/mod/mTotal56_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal57_mod4_20221025_08.h5',
        '/src/src/predSkan/mod/mTotal58_mod4_20221021_15.h5',
        '/src/src/predSkan/mod/mTotal59_mod4_20221021_10.h5',
        '/src/src/predSkan/mod/mTotal60_mod4_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal61_mod4_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal62_mod4_20221022_15.h5',
        '/src/src/predSkan/mod/mTotal63_mod4_20221022_15.h5',
    ]
    # modNameList = [
    #     '/src/src/predSkan/mod/mTotal0_mod4_20221024_02.h5',
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
    #     '/src/src/predSkan/mod/mTotal15_mod3_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal16_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal17_mod4_20221024_02.h5',
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
    #     '/src/src/predSkan/mod/mTotal52_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal53_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal54_mod1_20221025_08.h5',
    #     '/src/src/predSkan/mod/mTotal55_mod4_20221025_07.h5',
    #     '/src/src/predSkan/mod/mTotal56_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal57_mod4_20221025_08.h5',
    #     '/src/src/predSkan/mod/mTotal58_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal59_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal60_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal61_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal62_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal63_mod4_20221022_15.h5',
    # ]
    
    # modNameList = [
    #     '/src/src/predSkan/mod/mTotal0_mod4_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal1_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal2_mod3_20221021_08.h5',
    #     '/src/src/predSkan/mod/mTotal3_mod3_20221021_08.h5',
    #     '/src/src/predSkan/mod/mTotal4_mod3_20221021_08.h5',
    #     '/src/src/predSkan/mod/mTotal5_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal6_mod3_20221021_08.h5',
    #     '/src/src/predSkan/mod/mTotal7_mod4_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal8_mod3_20221021_08.h5',
    #     '/src/src/predSkan/mod/mTotal9_mod4_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal10_mod3_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal11_mod3_20221021_02.h5',
    #     '/src/src/predSkan/mod/mTotal12_mod3_20221021_02.h5',
    #     '/src/src/predSkan/mod/mTotal13_mod4_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal14_mod4_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal15_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal16_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal17_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal18_mod4_20221024_02.h5',
    #     '/src/src/predSkan/mod/mTotal19_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal20_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal21_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal22_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal23_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal24_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal25_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal26_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal27_mod3_20221021_02.h5',
    #     '/src/src/predSkan/mod/mTotal28_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal29_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal30_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal31_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal32_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal33_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal34_mod3_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal35_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal36_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal37_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal38_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal39_mod3_20221021_02.h5',
    #     '/src/src/predSkan/mod/mTotal40_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal41_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal42_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal43_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal44_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal45_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal46_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal47_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal48_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal49_mod3_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal50_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal51_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal52_mod3_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal53_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal54_mod1_20221025_08.h5',
    #     '/src/src/predSkan/mod/mTotal55_mod4_20221025_07.h5',
    #     '/src/src/predSkan/mod/mTotal56_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal57_mod4_20221025_08.h5',
    #     '/src/src/predSkan/mod/mTotal58_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal59_mod4_20221021_10.h5',
    #     '/src/src/predSkan/mod/mTotal60_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal61_mod4_20221022_15.h5',
    #     '/src/src/predSkan/mod/mTotal62_mod4_20221021_15.h5',
    #     '/src/src/predSkan/mod/mTotal63_mod4_20221022_15.h5',
    # ]
    modList = []
    for modName in modNameList:
        mod = tf.keras.models.load_model(modName)
        modList.append(mod)

    

    df = dataStep1('20220501','20220930')
    df2 = dataStep2(df)
    test(df2,modList)
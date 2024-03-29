# 使用指定模型做预测
# 

import os
import numpy as np
import tensorflow as tf

# docPath 绝对路径，比如/src/data/doc/total/xxx
# 要求路径里面包括bestMod.h5，min.npy和max.npy
# inputNpArray为对应的输入np结构，直接将结论print到终端
def predict(docPath,inputNpArray):
    mod = tf.keras.models.load_model(os.path.join(docPath,'bestMod.h5'))
    min = np.load(os.path.join(docPath,'min.npy'))
    max = np.load(os.path.join(docPath,'max.npy'))

    # print(min,max)
    sum = inputNpArray.sum(axis=1).reshape(-1,1)
    inputNpArray = inputNpArray/sum

    x = (inputNpArray-min)/(max-min)
    x[x == np.inf] = 0
    x[x == -np.inf] = 0
    input = np.nan_to_num(x)

    yp = mod.predict(input)

    # return yp,input
    return yp

import sys
sys.path.append('/src')
from src.predSkan.android.androidTotal import mergeData1AndData2,dataStep3,getTrainX
# 获得安卓大盘数据，返回数据用于predict的inputNpArray
def getAndroidTotalData(sinceTimeStr,unitlTimeStr):
    df = mergeData1AndData2(sinceTimeStr,unitlTimeStr)
    df = dataStep3(df)
    dataDf = df.sort_values(by=['install_date','cv'])
    dataDf = dataDf.groupby(['install_date','cv']).agg('sum')
    dataX = getTrainX(dataDf)
    return dataX


if __name__ == '__main__':
    # a = np.ones(64)*100
    # a[0] = 10000
    # a = a.reshape(-1,64)
    # # print(a)
    # print(predict('/src/data/doc/total/total_20221228_100638',a))
    # print(predict('/src/data/doc/total/total_20221228_105554',a))
    # print(predict('/src/data/doc/total/total_20221228_110747',a))

    # a = np.ones(128)*100
    # a[0] = 10000
    # a[64] = 10000
    # a = a.reshape(-1,128)
    # print(predict('/src/data/doc/total/total_20221229_040113',a))

    androidTotalMod = '/src/data/doc/androidTotal/total_20230113_083842'

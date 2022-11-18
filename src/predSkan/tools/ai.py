import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

class AI:
    def __init__(self):
        self.desc = '这是基类，不直接调用'

    # getData为获取数据集，并将数据放在self.dataDf里面
    # 注意保持一定的规律，比如安装日期统一用install_date
    def getData(self,sinceTimeStr,unitlTimeStr):
        print('获取数据，从%s至%s'%(sinceTimeStr,unitlTimeStr))
        self.dataDf = None

    def getTrainingData(self,sinceTimeStr,unitlTimeStr,arg1=None,arg2=None):
        print('获取训练数据集')
        # 从self.dataDf里根据参数拆出训练数据集
        # 如果有必要，可能最终训练的Y和真实的Y不一样，这个是否有价值，可能只有训练之后才会知道
        # 但是本质上不应该有类似问题，除非测试集也不是真实的Y

    def getTestingData(self,sinceTimeStr,unitlTimeStr,arg1=None,arg2=None):
        print('获取测试数据集')
        # 同获得训练集，基本一致

    # 目前好像就这个模型又快又稳
    def createModFunc(self,inputShape = (64,)):
        mod = keras.Sequential(
            [
                layers.Dense(512, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu", input_shape=inputShape),
                layers.Dropout(0.3),
                layers.Dense(512, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu"),
                layers.Dropout(0.3),
                layers.Dense(1, kernel_initializer='random_normal',bias_initializer='random_normal',activation="relu")
            ]
        )
        mod.compile(optimizer='adadelta',loss='mape')
        return mod

    # 这就是按照一般的方式train，那种R的方式另外做一个新的train
    def train(self,arg1=None,arg2=None):
        # 
        pass
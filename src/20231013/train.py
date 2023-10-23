# 进行训练
import numpy as np
import pandas as pd

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense,Dropout
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error

from data import getData10X,getDataY

def train():
    x = getData10X()
    x.drop(['install_date'], axis=1, inplace=True)
    
    # 只保留相关系数最高的第一个特征
    x = x.iloc[:,0:1]

    y = getDataY()
    y.drop(['install_date'], axis=1, inplace=True)

    # 数据预处理
    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=25)
    # scaler = StandardScaler()
    # X_train = scaler.fit_transform(X_train)
    # X_test = scaler.transform(X_test)

    # np.set_printoptions(precision=2, suppress=True, linewidth=100)
    # print('X_train:',X_train.values.reshape(-1))
    # print('y_train:',y_train.values.reshape(-1))
    # a = X_train.values.reshape(-1)
    # b = y_train.values.reshape(-1)
    # correlation = np.corrcoef(a,b)
    # print('correlation:',correlation)

    # 构建神经网络模型
    model = Sequential()
    model.add(Dense(1, activation='relu', input_dim=X_train.shape[1]))
    model.add(Dense(1))

    # 编译模型
    model.compile(optimizer='RMSprop', loss='mse',metrics=['mape'])

    earlyStoppingLoss = tf.keras.callbacks.EarlyStopping(monitor='loss', patience=5)
    earlyStoppingValLoss = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)
    # 训练模型
    model.fit(
        X_train, y_train, 
        validation_data=(X_test,y_test),
        callbacks=[earlyStoppingLoss,earlyStoppingValLoss],
        epochs=8000, 
        batch_size=64, 
        verbose=2
    )

train()
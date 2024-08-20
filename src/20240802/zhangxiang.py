from tensorflow import keras
import tensorflow as tf
from tensorflow.keras.layers import Input, Dense, Add
from tensorflow.keras.models import Model
from tensorflow.keras.constraints import MinMaxNorm
from tensorflow.keras.callbacks import Callback
from keras.initializers import Constant
from keras.models import load_model
import pandas as pd
import numpy as np

from tensorflow.keras import backend as K

def mape(y_true, y_pred):
    return K.mean(K.abs((y_true - y_pred) / y_true)) * 100

def create_model():

    # 创建3个输入层
    input1 = Input(shape=(1,), name='GDP_input')
    input2 = Input(shape=(1,), name='People_input')
    input3 = Input(shape=(1,), name='GPI_input')

    # 设置权重初始化方法
    initial_value = 1
    weight_initializer = Constant(value=initial_value)

    # 设置权重限制
    min_value = 0
    max_value = 100
    weight_constraint = MinMaxNorm(min_value=min_value
    , max_value=max_value
    )

    # 为每个输入层创建一个隐藏层（只有权重k，没有偏置b，且权重受到限制）
    hidden1 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer,name = 'GDP_hidden1'
                )(input1)
    hidden2 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer,name = 'People_hidden2'
                    )(input2)
    hidden3 = Dense(1, use_bias=False, kernel_constraint=weight_constraint
                    , kernel_initializer=weight_initializer,name = 'GPI_hidden3'
                    )(input3)

    # 将3个隐藏层的输出相加
    added_output = Add()([hidden1, hidden2, hidden3])

    # 创建并编译模型
    model = Model(inputs=[input1, input2, input3], outputs=added_output)
    # model.compile(optimizer='adam', loss='mse',metrics=['mean_absolute_percentage_error'])
    model.compile(optimizer='adam', loss=mape,metrics=['mean_absolute_percentage_error'])

    # # 打印模型结构
    # model.summary()
    keras.utils.plot_model(model, '/src/data/20240802_model1.jpg', show_shapes=True)
    return model

# from sklearn.preprocessing import StandardScaler

def min_max_normalize(X):
    # 计算每列的最大值
    max_vals = np.max(X, axis=0)
    # 使用最大值进行标准化
    X_normalized = X / max_vals
    return X_normalized

def getXY(df):    
    # 提取特征和目标值
    X = df[['GDP', '人口', 'GPI(Global Peace Index)']].values
    y = df['2023 年（Top3游戏月收入）'].values
    
    # 标准化数据
    # scaler_X = StandardScaler()
    # scaler_y = StandardScaler()
    # X = scaler_X.fit_transform(X)
    # y = scaler_y.fit_transform(y.reshape(-1, 1)).flatten()
    
    X_normalized = min_max_normalize(X)
    y_normalized = y / np.max(y)

    return X_normalized, y_normalized

class CustomCallback(keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        if (epoch + 1) % 1000 == 0:
            print(f"Epoch {epoch + 1}: loss = {logs['loss']}")
            for layer in self.model.layers:
                weights = layer.get_weights()
                if weights:
                    print(f"Layer {layer.name} weights: {weights}")

def main():
    filename = '全球推广.csv'
    # 读取 CSV 文件
    df = pd.read_csv(filename)
    # 去掉字符串中的逗号和美元符号，并转换为浮点数
    df['GDP'] = df['GDP'].replace('[\$,]', '', regex=True).astype(float)
    df['人口'] = df['人口'].replace('[,]', '', regex=True).astype(float)
    df['GPI(Global Peace Index)'] = df['GPI(Global Peace Index)'].astype(float)
    df['2023 年（Top3游戏月收入）'] = df['2023 年（Top3游戏月收入）'].replace('[\$,]', '', regex=True).astype(float)
    
    countryList = ['United States','Australia','Canada','France','Germany','Great britain','Italy','Japan','South Korea']
    df = df.loc[df['国家'].isin(countryList)]

    print(df)

    print('GDP max:',df['GDP'].max())
    print('人口 max:',df['人口'].max())
    print('GPI(Global Peace Index) max:',df['GPI(Global Peace Index)'].max())
    print('2023 年（Top3游戏月收入） max:',df['2023 年（Top3游戏月收入）'].max())

    

    X, y = getXY(df)
    print(X)
    print(y)

    # 创建模型
    model = create_model()
    
    # 拟合模型并使用自定义回调函数
    model.fit([X[:, 0], X[:, 1], X[:, 2]], y, epochs=10000, verbose=0, callbacks=[CustomCallback()])

    # 使用模型进行预测
    predictions = model.predict([X[:, 0], X[:, 1], X[:, 2]])

    # 计算 MAPE
    mape = np.mean(np.abs((y - predictions.squeeze()) / y)) * 100
    print(f'MAPE: {mape:.2f}%')
    


if __name__ == "__main__":
    main()
# 建立模型
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import Input,layers

# 只区分google和其他的模型
# 输入的数据应该是 （64+1） + （64+1）
# 其中64指一天中CV分布，1是首日真实收入（可能是用CV值转化来的）
def createMod01():
    def slice(x,index):
        if index == 0:
            return x[:,0:64]
        if index == 1:
            return x[:,64:65]
        if index == 2:
            return x[:,65:129]
        if index == 3:
            return x[:,129:130]

    def addOne(x):
        return x+1

    def addNN(inputLayer,name):
        layer0 = layers.Dense(
            128,
            kernel_initializer='random_normal',
            bias_initializer='random_normal', 
            activation="relu", 
            input_shape=(64,),
            name = '%s_nn0'%name
        )(inputLayer)
        layer0Dropout = layers.Dropout(
            0.3,
            name = '%s_dropout0'%name
        )(layer0)
        layer1 = layers.Dense(
            128, 
            kernel_initializer='random_normal',
            bias_initializer='random_normal',
            activation="relu",
            name = '%s_nn1'%name
        )(layer0Dropout)
        layer1Dropout = layers.Dropout(
            0.3,
            name = '%s_dropout1'%name
        )(layer1)
        outLayer = layers.Dense(
            1, 
            kernel_initializer='random_normal',
            bias_initializer='random_normal',
            activation="relu",
            name = '%s_out'%name
        )(layer1Dropout)
        return outLayer

    inputLayer = Input(
        shape = (130,),
        name = 'input'
    )

    layer0 = layers.Lambda(
        slice,
        arguments={'index':0},
        name = 'mediaCv'
    )(inputLayer)
    layer0_1 = addNN(layer0,'media')

    layer0_2 = layers.Lambda(
        addOne,
        name = 'meidaAddOne'
    )(layer0_1)

    layer1 = layers.Lambda(
        slice,
        arguments={'index':1},
        name = 'mediaR1usd'
    )(inputLayer)

    layerMulti0 = layers.Multiply(
        name = 'muti0'
    )([layer0_2,layer1])

    layer2 = layers.Lambda(
        slice,
        arguments={'index':2},
        name = 'otherCv'
    )(inputLayer)
    layer2_1 = addNN(layer2,'other')

    layer2_2 = layers.Lambda(
        addOne,
        name = 'otherAddOne'
    )(layer2_1)

    layer3 = layers.Lambda(
        slice,
        arguments={'index':3},
        name = 'otherR1usd'
    )(inputLayer)

    layerMulti1 = layers.Multiply(
        name = 'muti1'
    )([layer2_2,layer3])

    added = layers.Add(
        name = 'add'
    )([layerMulti0, layerMulti1])

    mod = keras.models.Model(inputLayer,added)

    # mod.compile(optimizer='adadelta',loss='mse')
    mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/mod.png', show_shapes=True)

    return mod


# 与createMod01相比，中间的nn简化，不再搞得太复杂，防止过拟合
def createMod02():
    def slice(x,index):
        if index == 0:
            return x[:,0:64]
        if index == 1:
            return x[:,64:65]
        if index == 2:
            return x[:,65:129]
        if index == 3:
            return x[:,129:130]

    def addOne(x):
        return x+1

    def addNN(inputLayer,name):
        layer0 = layers.Dense(
            64,
            kernel_initializer='random_normal',
            bias_initializer='random_normal', 
            activation="relu", 
            input_shape=(64,),
            name = '%s_nn0'%name
        )(inputLayer)
        
        outLayer = layers.Dense(
            1, 
            kernel_initializer='random_normal',
            bias_initializer='random_normal',
            activation="relu",
            name = '%s_out'%name
        )(layer0)
        return outLayer

    inputLayer = Input(
        shape = (130,),
        name = 'input'
    )

    layer0 = layers.Lambda(
        slice,
        arguments={'index':0},
        name = 'mediaCv'
    )(inputLayer)
    layer0_1 = addNN(layer0,'media')

    layer0_2 = layers.Lambda(
        addOne,
        name = 'meidaAddOne'
    )(layer0_1)

    layer1 = layers.Lambda(
        slice,
        arguments={'index':1},
        name = 'mediaR1usd'
    )(inputLayer)

    layerMulti0 = layers.Multiply(
        name = 'muti0'
    )([layer0_2,layer1])

    layer2 = layers.Lambda(
        slice,
        arguments={'index':2},
        name = 'otherCv'
    )(inputLayer)
    layer2_1 = addNN(layer2,'other')

    layer2_2 = layers.Lambda(
        addOne,
        name = 'otherAddOne'
    )(layer2_1)

    layer3 = layers.Lambda(
        slice,
        arguments={'index':3},
        name = 'otherR1usd'
    )(inputLayer)

    layerMulti1 = layers.Multiply(
        name = 'muti1'
    )([layer2_2,layer3])

    added = layers.Add(
        name = 'add'
    )([layerMulti0, layerMulti1])

    mod = keras.models.Model(inputLayer,added)

    # mod.compile(optimizer='adadelta',loss='mse')
    mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/mod.png', show_shapes=True)

    return mod


if __name__ == '__main__':
    createMod01()
# 建立模型
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import Input,layers,regularizers

# 简单版本模型，防止过拟合
# 分为4个部分，按照字母排序：字节，脸书，谷歌，自然量
def createModEasy04():
    # 为了方便切割和后续扩充，输入均是 64+1，4组
    # 先切分大组，按65为单位切
    def slice0(x,index):
        return x[:,index * 65 : (index + 1)*65]
    
    def slice1(x,index):
        if index == 0:
            return x[:,0:64]
        return x[:,64:65]

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
        shape = (65*4,),
        name = 'input'
    )

    lastLayerList = []
    for i in range(4):
        s0Layer65 = layers.Lambda(
            slice0,
            arguments={'index':i},
            name = 'slice65-%d'%(i)
        )(inputLayer)
        s1Layer64 = layers.Lambda(
            slice1,
            arguments={'index':0},
            name = 'slice64-%d'%(i)
        )(s0Layer65)
        s1Layer1 = layers.Lambda(
            slice1,
            arguments={'index':1},
            name = 'slice1-%d'%(i)
        )(s0Layer65)

        layer0_1 = addNN(s1Layer64,'g%d'%(i))

        layer0_2 = layers.Lambda(
            addOne,
            name = 'AddOne-%d'%(i),
        )(layer0_1)

        layerMulti = layers.Multiply(
            name = 'muti-%d'%(i),
        )([layer0_2,s1Layer1])

        lastLayerList.append(layerMulti)

    added = layers.Add(
        name = 'add'
    )(lastLayerList)

    mod = keras.models.Model(inputLayer,added)

    # mod.compile(optimizer='adadelta',loss='mse')
    mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/customLayer/createModEasy04.png', show_shapes=True)

    return mod

# 简易版本
def createModEasy04_a():
    # 为了方便切割和后续扩充，输入均是 64+1，4组
    # 先切分大组，按65为单位切
    def slice0(x,index):
        return x[:,index * 65 : (index + 1)*65]
    
    def slice1(x,index):
        if index == 0:
            return x[:,0:64]
        return x[:,64:65]

    def addOne(x):
        return x+1

    def addNN(inputLayer,name):
        layer0 = layers.Dense(
            16,
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
        shape = (65*4,),
        name = 'input'
    )

    lastLayerList = []
    for i in range(4):
        s0Layer65 = layers.Lambda(
            slice0,
            arguments={'index':i},
            name = 'slice65-%d'%(i)
        )(inputLayer)
        s1Layer64 = layers.Lambda(
            slice1,
            arguments={'index':0},
            name = 'slice64-%d'%(i)
        )(s0Layer65)
        s1Layer1 = layers.Lambda(
            slice1,
            arguments={'index':1},
            name = 'slice1-%d'%(i)
        )(s0Layer65)

        layer0_1 = addNN(s1Layer64,'g%d'%(i))

        layer0_2 = layers.Lambda(
            addOne,
            name = 'AddOne-%d'%(i),
        )(layer0_1)

        layerMulti = layers.Multiply(
            name = 'muti-%d'%(i),
        )([layer0_2,s1Layer1])

        lastLayerList.append(layerMulti)

    added = layers.Add(
        name = 'add'
    )(lastLayerList)

    mod = keras.models.Model(inputLayer,added)

    mod.compile(optimizer='adadelta',loss='mse')
    # mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/customLayer/createModEasy04.png', show_shapes=True)

    return mod

def createModEasy04_b():
    # 为了方便切割和后续扩充，输入均是 64+1，4组
    # 先切分大组，按65为单位切
    def slice0(x,index):
        return x[:,index * 65 : (index + 1)*65]
    
    def slice1(x,index):
        if index == 0:
            return x[:,0:64]
        return x[:,64:65]

    def addOne(x):
        return x+1

    def addNN(inputLayer,name):
        layer0 = layers.Dense(
            16,
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
        shape = (65*4,),
        name = 'input'
    )

    lastLayerList = []
    for i in range(4):
        s0Layer65 = layers.Lambda(
            slice0,
            arguments={'index':i},
            name = 'slice65-%d'%(i)
        )(inputLayer)
        s1Layer64 = layers.Lambda(
            slice1,
            arguments={'index':0},
            name = 'slice64-%d'%(i)
        )(s0Layer65)
        s1Layer1 = layers.Lambda(
            slice1,
            arguments={'index':1},
            name = 'slice1-%d'%(i)
        )(s0Layer65)

        layer0_1 = addNN(s1Layer64,'g%d'%(i))

        layer0_2 = layers.Lambda(
            addOne,
            name = 'AddOne-%d'%(i),
        )(layer0_1)

        layerMulti = layers.Multiply(
            name = 'muti-%d'%(i),
        )([layer0_2,s1Layer1])

        lastLayerList.append(layerMulti)

    added = layers.Add(
        name = 'add'
    )(lastLayerList)

    mod = keras.models.Model(inputLayer,added)

    # mod.compile(optimizer='adadelta',loss='mse')
    mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/customLayer/createModEasy04.png', show_shapes=True)

    return mod


def createModEasy04_adam():
    # 为了方便切割和后续扩充，输入均是 64+1，4组
    # 先切分大组，按65为单位切
    def slice0(x,index):
        return x[:,index * 65 : (index + 1)*65]
    
    def slice1(x,index):
        if index == 0:
            return x[:,0:64]
        return x[:,64:65]

    def addOne(x):
        return x+1

    def addNN(inputLayer,name):
        layer0 = layers.Dense(
            32,
            kernel_initializer='random_normal',
            bias_initializer='random_normal', 
            activation="relu", 
            name = '%s_nn0'%name
        )(inputLayer)

        layer1 = layers.Dense(
            32,
            kernel_initializer='random_normal',
            bias_initializer='random_normal', 
            activation="relu", 
            name = '%s_nn1'%name
        )(layer0)
        
        outLayer = layers.Dense(
            1, 
            kernel_initializer='random_normal',
            bias_initializer='random_normal',
            activation="relu",
            name = '%s_out'%name
        )(layer1)
        return outLayer

    inputLayer = Input(
        shape = (65*4,),
        name = 'input'
    )

    lastLayerList = []
    for i in range(4):
        s0Layer65 = layers.Lambda(
            slice0,
            arguments={'index':i},
            name = 'slice65-%d'%(i)
        )(inputLayer)
        s1Layer64 = layers.Lambda(
            slice1,
            arguments={'index':0},
            name = 'slice64-%d'%(i)
        )(s0Layer65)
        s1Layer1 = layers.Lambda(
            slice1,
            arguments={'index':1},
            name = 'slice1-%d'%(i)
        )(s0Layer65)

        layer0_1 = addNN(s1Layer64,'g%d'%(i))

        layer0_2 = layers.Lambda(
            addOne,
            name = 'AddOne-%d'%(i),
        )(layer0_1)

        layerMulti = layers.Multiply(
            name = 'muti-%d'%(i),
        )([layer0_2,s1Layer1])

        lastLayerList.append(layerMulti)

    added = layers.Add(
        name = 'add'
    )(lastLayerList)

    mod = keras.models.Model(inputLayer,added)

    mod.compile(optimizer='adadelta',loss='mse')
    # mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/customLayer/createModEasy04_adam.png', show_shapes=True)

    return mod

# 分了5部分，多加了一个applovin，从左到右按顺序 ap，bd，fb，gg，og
def createModEasy05_b():
    # 为了方便切割和后续扩充，输入均是 64+1，4组
    # 先切分大组，按65为单位切
    def slice0(x,index):
        return x[:,index * 65 : (index + 1)*65]
    
    def slice1(x,index):
        if index == 0:
            return x[:,0:64]
        return x[:,64:65]

    def addOne(x):
        return x+1

    def addNN(inputLayer,name):
        layer0 = layers.Dense(
            16,
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
        shape = (65*5,),
        name = 'input'
    )

    lastLayerList = []
    for i in range(5):
        s0Layer65 = layers.Lambda(
            slice0,
            arguments={'index':i},
            name = 'slice65-%d'%(i)
        )(inputLayer)
        s1Layer64 = layers.Lambda(
            slice1,
            arguments={'index':0},
            name = 'slice64-%d'%(i)
        )(s0Layer65)
        s1Layer1 = layers.Lambda(
            slice1,
            arguments={'index':1},
            name = 'slice1-%d'%(i)
        )(s0Layer65)

        layer0_1 = addNN(s1Layer64,'g%d'%(i))

        layer0_2 = layers.Lambda(
            addOne,
            name = 'AddOne-%d'%(i),
        )(layer0_1)

        layerMulti = layers.Multiply(
            name = 'muti-%d'%(i),
        )([layer0_2,s1Layer1])

        lastLayerList.append(layerMulti)

    added = layers.Add(
        name = 'add'
    )(lastLayerList)

    mod = keras.models.Model(inputLayer,added)

    # mod.compile(optimizer='adadelta',loss='mse')
    mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/customLayer/createModEasy05.png', show_shapes=True)

    return mod


# 正则化，这里选择l2正则化
def createModEasy05_l2(w = 0.01):
    # 为了方便切割和后续扩充，输入均是 64+1，4组
    # 先切分大组，按65为单位切
    def slice0(x,index):
        return x[:,index * 65 : (index + 1)*65]
    
    def slice1(x,index):
        if index == 0:
            return x[:,0:64]
        return x[:,64:65]

    def addOne(x):
        return x+1

    def addNN(inputLayer,name):
        layer0 = layers.Dense(
            16,
            kernel_initializer='random_normal',
            bias_initializer='random_normal',
            kernel_regularizer=regularizers.l2(w),
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
        shape = (65*5,),
        name = 'input'
    )

    lastLayerList = []
    for i in range(5):
        s0Layer65 = layers.Lambda(
            slice0,
            arguments={'index':i},
            name = 'slice65-%d'%(i)
        )(inputLayer)
        s1Layer64 = layers.Lambda(
            slice1,
            arguments={'index':0},
            name = 'slice64-%d'%(i)
        )(s0Layer65)
        s1Layer1 = layers.Lambda(
            slice1,
            arguments={'index':1},
            name = 'slice1-%d'%(i)
        )(s0Layer65)

        layer0_1 = addNN(s1Layer64,'g%d'%(i))

        layer0_2 = layers.Lambda(
            addOne,
            name = 'AddOne-%d'%(i),
        )(layer0_1)

        layerMulti = layers.Multiply(
            name = 'muti-%d'%(i),
        )([layer0_2,s1Layer1])

        lastLayerList.append(layerMulti)

    added = layers.Add(
        name = 'add'
    )(lastLayerList)

    mod = keras.models.Model(inputLayer,added)

    # mod.compile(optimizer='adadelta',loss='mse')
    mod.compile(optimizer='RMSprop',loss='mse')
    # mod.summary()
    keras.utils.plot_model(mod, '/src/data/customLayer/createModEasy05.png', show_shapes=True)

    return mod



if __name__ == '__main__':
    createModEasy04()
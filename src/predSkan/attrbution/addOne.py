# 测试如何实现+1的layer
import numpy as np

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import Input,layers


def addOne(x):
    return x+1

inputLayer = Input(
    shape = (1,),
    name = 'input'
)

outputLayer = layers.Lambda(
    addOne,
    name = 'addOne'
)(inputLayer)

mod = keras.models.Model(inputLayer,outputLayer)

keras.utils.plot_model(mod, '/src/data/addOne.png', show_shapes=True)


print('mod:',mod.predict(np.array([1,3,5,7]).reshape(1,-1)))

import numpy as np

import tensorflow as tf
from tensorflow import keras
from keras.models import Input, Model
from keras.layers import Lambda


mod = tf.keras.models.Sequential()

mod.add(Input(shape=(1,)))
mod.add(tf.keras.layers.Dense(1, activation='relu'))
mod.add(tf.keras.layers.Dense(1))

mod.compile(
    optimizer="rmsprop",
    loss='mse'
)

trainX = np.random.rand(100)
trainY = trainX * 5 + 2

print(trainX, trainY)

mod.fit(trainX, trainY,epochs= 500)

testX = np.array([1])

print(mod.predict(testX))


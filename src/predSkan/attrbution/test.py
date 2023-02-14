import numpy as np

from tensorflow import keras
from keras.models import Input, Model
from keras.layers import Lambda

def minus(inputs):
    x, y = inputs
    return (x+y)

a = Input(shape=(1,))
b = Input(shape=(1,))
minus_layer = Lambda(minus, name='minus')([a, b])
model = Model(inputs=[a, b], outputs=[minus_layer])

v0 = np.array([5, 2, 3])
v1 = np.array([8, 4, 1])

print(model.predict([v0.reshape(1, -1), v1.reshape(1, -1)]))


# androidMMM4改进版
# 主要是针对每个媒体进行约束，r7usd 必须介于 r3usd 的 1.5 倍和 2.5 倍之间（暂定）

import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from tensorflow.keras.layers import Input, Dense, Concatenate
from tensorflow.keras.models import Model
from sklearn.metrics import mean_absolute_percentage_error, r2_score

def custom_loss(y_true, y_pred):
    mse_loss = tf.keras.losses.MeanSquaredError()(y_true, tf.reduce_sum(y_pred[:, 5:], axis=1))

    lower_bound = 1.5
    upper_bound = 2.5
    penalty = tf.where(
        y_pred[:, :5] <= lower_bound, (lower_bound - y_pred[:, :5]) * 1000,
        tf.where(y_pred[:, :5] >= upper_bound, (y_pred[:, :5] - upper_bound) * 1000, 0.0)
    )
    penalty_loss = tf.reduce_mean(penalty)
    total_loss = mse_loss + penalty_loss
    return total_loss

def create_model():
    media_list = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int',
        'snapchat_int',
        'other'
    ]
    input_layers = [Input(shape=(5,)) for _ in media_list]
    r3usd_values = [input_layer[:, 0:1] for input_layer in input_layers]
    other_features = [input_layer[:, 1:] for input_layer in input_layers]

    dense_layers = []
    for features, r3usd in zip(other_features, r3usd_values):
        hidden_layer1 = Dense(8, activation='relu')(features)
        hidden_layer2 = Dense(8, activation='relu')(hidden_layer1)
        hidden_layer3 = Dense(8, activation='relu')(hidden_layer2)
        output = Dense(1, activation='linear')(hidden_layer3)
        output_with_r3usd = output * r3usd
        combined_output = Concatenate(axis=1)([output, output_with_r3usd])
        dense_layers.append(combined_output)

    concat_outputs = Concatenate(axis=1)(dense_layers)
    combined_model = Model(inputs=input_layers, outputs=concat_outputs)
    return combined_model

def train():
    df = pd.read_csv('/src/data/zk/df_zk2.csv')
    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)

    X_train = train_df.drop(columns=['install_date', 'r7usd'])
    X_train_filled = X_train.fillna(0)
    y_train = train_df['r7usd']

    X_val = test_df.drop(columns=['install_date', 'r7usd'])
    X_val_filled = X_val.fillna(0)
    y_val = test_df['r7usd']

    # 分离r3usd和其他特征
    r3usd_scaler = MinMaxScaler()
    other_scaler = StandardScaler()
    X_train_r3usd = r3usd_scaler.fit_transform(X_train_filled.iloc[:, [0, 5, 10, 15, 20]])
    X_train_other = other_scaler.fit_transform(X_train_filled.iloc[:, [1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19, 21, 22, 23, 24]])
    X_val_r3usd = r3usd_scaler.transform(X_val_filled.iloc[:, [0, 5, 10, 15, 20]])
    X_val_other = other_scaler.transform(X_val_filled.iloc[:, [1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19, 21, 22, 23, 24]])

    # 重组数据
    X_train_scaled = np.hstack([X_train_r3usd, X_train_other]).reshape(-1, 5, 5)
    X_val_scaled = np.hstack([X_val_r3usd, X_val_other]).reshape(-1, 5, 5)

    combined_model = create_model()
    optimizer = tf.keras.optimizers.RMSprop()
    epochs = 3000
    batch_size = 32

    for epoch in range(epochs):
        epoch_loss = 0
        mape_sum = 0
        num_batches = 0
        for i in range(0, len(X_train), batch_size):
            X_batch = [X_train_scaled[i:i + batch_size, j] for j in range(5)]
            y_batch = y_train.iloc[i:i + batch_size].values

            with tf.GradientTape() as tape:
                y_pred = combined_model(X_batch, training=True)
                loss = custom_loss(y_batch, y_pred)
                epoch_loss += loss.numpy()

            gradients = tape.gradient(loss, combined_model.trainable_variables)
            optimizer.apply_gradients(zip(gradients, combined_model.trainable_variables))

            y_pred_sum = np.sum(y_pred[:, 5:], axis=1)
            mape = mean_absolute_percentage_error(y_batch, y_pred_sum)
            mape_sum += mape
            num_batches += 1

        print(f"Epoch {epoch + 1}, Loss: {epoch_loss / num_batches}, MAPE: {mape_sum / num_batches}")

    combined_model.save('/src/data/zk/combined_model.h5')

train()

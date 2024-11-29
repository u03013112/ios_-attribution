import pandas as pd
import numpy as np
from prophet import Prophet

# 生成数据
dates = pd.date_range(start='2024-01-01', end='2024-05-30', freq='D')
cost = dates.dayofweek.map({0: 100, 1: 200, 2: 300, 3: 400, 4: 500, 5: 600, 6: 700})
revenue = dates.dayofweek.map({0: 10, 1: 20, 2: 30, 3: 40, 4: 50, 5: 60, 6: 140})

data = pd.DataFrame({'ds': dates, 'cost': cost, 'y': revenue})

# 初始化并训练Prophet模型
model = Prophet(seasonality_mode='multiplicative')
model.add_regressor('cost',standardize=False)
# model.add_regressor('cost')
model.fit(data)

# 生成预测数据
future_dates = pd.date_range(start='2024-05-02', end='2024-05-30', freq='D')
future_cost = future_dates.dayofweek.map({0: 100, 1: 200, 2: 300, 3: 400, 4: 500, 5: 600, 6: 700})
future = pd.DataFrame({'ds': future_dates, 'cost': future_cost})

# 进行预测
forecast = model.predict(future)
forecast['dayOfWeek'] = forecast['ds'].dt.dayofweek
# 打印预测结果

data.rename(columns={'cost':'costRaw'}, inplace=True)
forecast = forecast.merge(data[['ds','costRaw']], on='ds', how='left')

print(forecast.columns)

print(forecast[[
    'ds', 'yhat', 'weekly','trend','additive_terms','costRaw',
    # 'extra_regressors_additive',
    'extra_regressors_multiplicative',
    'multiplicative_terms','dayOfWeek']])


ret = forecast[[
    'ds', 'yhat', 'weekly','trend','additive_terms','cost','costRaw',
    # 'extra_regressors_additive',
    'extra_regressors_multiplicative',
    'multiplicative_terms','dayOfWeek']]

# ret['weekly+cost'] = ret['weekly'] + ret['cost']
# print(ret)
# print(data.head(20))

# from sklearn.linear_model import LinearRegression

# def wb(x = np.array([1, 2, 3, 4, 5]),y = np.array([2, 4, 6, 8, 10])):

#     x = x.reshape(-1, 1)
#     # 创建线性回归模型
#     model = LinearRegression()

#     # 拟合模型
#     model.fit(x, y)

#     # 获取 w 和 b
#     w = model.coef_[0]
#     b = model.intercept_

#     print(f"w: {w}, b: {b}")

#     return w,b

# w,b = wb(np.array(ret['costRaw']),ret['cost'])

# ret['cost1'] = ret['costRaw']*w + b
# print(ret[['costRaw','cost','cost1']])
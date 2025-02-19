import pandas as pd
from prophet import Prophet

df = pd.read_csv('https://raw.githubusercontent.com/facebook/prophet/main/examples/example_wp_log_peyton_manning.csv')
df['y'] = 5 - df['y']
df['cap'] = 8.5
df['floor'] = 1.5
m = Prophet(growth='logistic')
m.fit(df)

future = m.make_future_dataframe(periods=1826)
future['cap'] = 8.5
future['floor'] = 1.5
fcst = m.predict(future)
fig = m.plot(fcst)

# print('done')
print(fcst)
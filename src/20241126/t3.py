
import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql




sql = '''
select
install_day,
media,
country,
max_r,
actual_revenue,
predicted_revenue
from lastwar_predict_day1_revenue_by_cost__nerf_r_test
where
day between '20240902' and '20241030'
;
'''

df = execSql(sql)

df['mape'] = np.abs((df['actual_revenue'] - df['predicted_revenue']) / df['actual_revenue'])
df = df.sort_values('install_day', ascending=False)
df.to_csv('/src/data/20241126_revenue_mape_raw.csv', index=False)

df = df.groupby(['media', 'country', 'max_r']).agg({
    'mape': 'mean'
}).reset_index()

print(df)

df.to_csv('/src/data/20241126_revenue_mape.csv', index=False)
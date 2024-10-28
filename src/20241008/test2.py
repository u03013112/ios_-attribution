import pandas as pd

# 示例数据
data = {'install_day': ['20241012','20241013']}
df = pd.DataFrame(data)

# 将日期字符串转换为日期时间对象，并设置为UTC时区
df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d').dt.tz_localize('UTC')

df['week_day'] = df['install_day'].dt.day_name()
# 计算周数
df['week'] = df['install_day'].dt.strftime('%Y%W')

print(df)
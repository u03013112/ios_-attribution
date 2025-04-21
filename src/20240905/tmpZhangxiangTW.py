import pandas as pd
import numpy as np

def getData():
    df = pd.read_csv('tw_20241101_20250413.csv')
    df.rename(columns={'礼包美元价格': 'pay usd', '支付(实时).总次数': 'pay count'}, inplace=True)
    df = df[['pay usd', 'pay count']]
    # 转换成浮点数，"0.99" -> 0.99，如果不能转换成浮点数，则返回0
    df['pay usd'] = df['pay usd'].apply(lambda x: float(x) if str(x).replace('.', '', 1).isdigit() else 0)
    # df['pay usd'] = df['pay usd'].apply(lambda x: float(x))
    # # 转换成int，"1,319,681" -> 1319681
    # df['pay count'] = df['pay count'].apply(lambda x: int(x.replace(',', '')))

    df = df[df['pay usd'] > 0]

    df = df.sort_values(by='pay usd', ascending=True)
    
    print(df)
    return df


# 将每行数据添加一个新的列，名为 count
# 要求 count > 0的整数
# 并且 count和pay usd尽量正相关
# 另外需要 新的列 pay count2，pay count2 = pay count * count
# sum(pay count2) 尽量接近 sum(pay count) * 2.5

def f1():
    df = getData()
    # 计算目标总和
    target_sum = df['pay count'].sum() * 2.5

    # 初始化 count 列
    df['count'] = np.ceil(df['pay usd']).astype(int)

    # 计算 pay count2
    df['pay count2'] = df['pay count'] * df['count']

    # 调整 count 列以使 sum(pay count2) 接近 target_sum
    current_sum = df['pay count2'].sum()
    adjustment_factor = target_sum / current_sum

    df['count'] = (df['count'] * adjustment_factor).round().astype(int)
    df['count'] = df['count'].apply(lambda x: max(x, 1))  # 确保 count > 0

    # 重新计算 pay count2
    df['pay count2'] = df['pay count'] * df['count']


    df.rename(columns={
        'pay usd': '支付金额（档位）',
        'pay count': '原有支付次数（和）',
        'count': '新支付次数',
        'pay count2': '新支付次数（和）',
    }, inplace=True)
    print(df)
    df.to_csv('/src/data/tw_20241101_20250413.csv', index=False)
    print('新支付次数 与 支付金额（档位） 相关系数:',df.corr()['新支付次数（和）']['支付金额（档位）'])
    print("新支付次数（和）/原有支付次数（和）:", df['新支付次数（和）'].sum()/df['原有支付次数（和）'].sum())


if __name__ == '__main__':
    f1()
import pandas as pd


def costPredict():
    df = pd.read_csv('th.csv')

    df.rename(columns={
        '安装月份': 'month',
        '花费(USD)': 'cost'
    }, inplace=True)

    # cost 是类似 '1,000.00'的字符串
    # 转换成浮点数，"1,000.00" -> 1000.00
    df['cost'] = df['cost'].apply(lambda x: float(x.replace(',', '')))

    df = df[['month', 'cost']]

    # month 是类似 '202401'的字符串
    # 按照month分为2024年和2025年两个部分
    # 202401数据忽略不计，202504数据不完整忽略不计
    # 用202402~202403 对应 202502~202503
    # 然后用202405~202412 等比例计算 202505~202512 对应的花费
    df['month'] = df['month'].astype(str)
    df = df[df['month'] != '202401']
    df = df[df['month'] != '202504']

    cost25 = df[df['month'].isin(['202502', '202503'])]['cost'].sum()
    cost24 = df[df['month'].isin(['202402', '202403'])]['cost'].sum()
    ratio = cost25 / cost24
    print('ratio:', ratio)

    df25 = df[df['month'].isin(['202404', '202405', '202406', '202407', '202408', '202409', '202410', '202411', '202412'])].copy()
    df25['cost'] = df25['cost'] * ratio
    df25['month'] = df25['month'].apply(lambda x: str(int(x) + 100))
    

    df = df.append(df25, ignore_index=True)
    df = df.sort_values(by=['month'])

    df.to_csv('/src/data/20250422_th_cost.csv', index=False)

    # 画图 将month 拆成 year 和 month
    # month 是 x轴，cost 是 y轴
    # 2024年的画一条线，用蓝色
    # 2025年01~03的画一条线，用红色
    # 2025年04~12的画一条线，用绿色
    # 保存到文件 /src/data/20250422_th_cost.png
    df['year'] = df['month'].apply(lambda x: int(x[:4]))
    df['month'] = df['month'].apply(lambda x: int(x[4:]))

    df2024 = df[df['year'] == 2024]
    df2025 = df[df['year'] == 2025]
    df2025_01_03 = df2025[df2025['month'].isin([1, 2, 3, 4])]
    df2025_04_12 = df2025[df2025['month'].isin([4, 5, 6, 7, 8, 9, 10, 11, 12])]

    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    plt.plot(df2024['month'], df2024['cost'], label='2024')
    plt.plot(df2025_01_03['month'], df2025_01_03['cost'], label='2025 Jan-Mar')
    plt.plot(df2025_04_12['month'], df2025_04_12['cost'], linestyle='--', label='2025 Apr-Dec')
    plt.title('2024-2025 Cost Prediction')
    plt.xlabel('Month')
    plt.ylabel('Cost (USD)')
    plt.xticks(df['month'].unique())
    plt.legend()
    plt.grid()
    plt.savefig('/src/data/20250422_th_cost.png')
    plt.close()

if __name__ == '__main__':
    costPredict()

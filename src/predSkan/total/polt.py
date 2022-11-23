import pandas as pd
import matplotlib.pyplot as plt

def max(max = 200.0):
    df = pd.read_csv('/src/data/totalDataSum_20220501_20220930.csv')
    df2 = df.loc[
        (df.install_date >= '2022-05-01') & (df.install_date <= '2022-09-30')
    ].groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','cv_usd':'sum'})

    plt.title("total max cut")
    df2['sumr1usd'].plot(label = 'sum r1 usd')
    df2['sumr7usd'].plot(label = 'sum r7 usd')

    maxDf = pd.read_csv('/src/data/totalData_20220501_20220930_200.00.csv')
    maxDf2 = maxDf.loc[
        (maxDf.install_date >= '2022-05-01') & (maxDf.install_date <= '2022-09-30')
    ].groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
    maxDf2['sumr1usd'].plot(label = 'sum max r1 usd')
    maxDf2['sumr7usd'].plot(label = 'sum max r7 usd')

    plt.legend()
    plt.savefig('/src/data/total.png')
    plt.clf()

if __name__ == '__main__':
    max()
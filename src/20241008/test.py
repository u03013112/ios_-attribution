import pandas as pd
import numpy as np

def mape():
    arppuDf = pd.read_csv('/src/data/arppu_prediction_results.csv')
    arppuDf.rename(columns={'date': 'ds'}, inplace=True)
    arppuDf = arppuDf[['ds', 'predicted_arppu', 'real_arppu']]
    pud1Df = pd.read_csv('/src/data/pud1_pct_prediction_results.csv')
    pud1Df = pud1Df[['ds', 'revenue','pud1','predicted_pud1_pct','y']]
    pud1Df['predicted_pud1'] = pud1Df['pud1']/(1 + pud1Df['y'])*(1 + pud1Df['predicted_pud1_pct'])

    df = arppuDf.merge(pud1Df, on='ds', how='inner')

    # df['revenue'] = df['real_arppu'] * df['pud1']
    df['revenue prediction'] = df['predicted_arppu'] * df['predicted_pud1']

    df['mape'] = np.abs((df['revenue'] - df['revenue prediction']) / df['revenue']) * 100

    print("Results DataFrame:")
    print(df[['ds', 'mape']])

    print("MAPE: ", df['mape'].mean())

if __name__ == "__main__":
    mape()
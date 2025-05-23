
import pandas as pd

import sys
sys.path.append('/src')

from src.lastwar.ss.ss import ssSql

def getData():
    filename = f'/src/data/lastwarPredictRevenue3_36_sum_data_2025-03-02.csv'
    df = pd.read_csv(filename)

    return df

def getPredictData():
    filename = '/src/data/lastwarPredictRevenue3_36_sum_2025-03-03.csv'
    df = pd.read_csv(filename)

    return df

def main():

    df = getData().copy()

    df['month'] = df['day'].str.slice(0,7)
    df = df.groupby(['month','server_id']).agg({'revenue':'sum'}).reset_index()

    minRevenueDf = df.groupby('month').agg({'revenue':'min'}).reset_index()
    monthDf = df.groupby('month').agg({'revenue':'sum'}).reset_index()
    monthDf = monthDf.merge(minRevenueDf,on='month',how='left',suffixes=('_sum','_min'))
    monthDf['min/sum'] = monthDf['revenue_min'] / monthDf['revenue_sum']

    predictDf = getPredictData().copy()
    predictDf['month'] = predictDf['ds'].str.slice(0,7)
    predictDf = predictDf.groupby(['month']).agg({'predict2':'sum'}).reset_index()

    resultDf = monthDf.merge(predictDf,on='month',how='left')
    resultDf['predict2 * min/sum'] = resultDf['predict2'] * resultDf['min/sum']

    resultDf.to_csv('/src/data/20250303_result.csv',index=False)

def debug20250416():
    sql = """
SELECT 
    "lw_zone@id" AS b_server_id_str,
    "lw_zone@region_type" AS region_type,
    "lw_zone@start_time" AS start_time,
    LEAD("lw_zone@start_time") OVER (ORDER BY "lw_zone@start_time" ASC) AS next_start_time
FROM 
    ta_dim.dim_3_0_47843
WHERE 
    "lw_zone@region_type" IN ('日服')
ORDER BY 
    "lw_zone@start_time" ASC
limit 100    
;
    """
    lines = ssSql(sql)
    print(lines)

if __name__ == "__main__":
    # main()
    debug20250416()


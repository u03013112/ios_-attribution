import os
import datetime
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import sys
sys.path.append('/src')
sys.path.append('../..')
from src.maxCompute import execSql,getO


def getRevenueData(startDayStr, endDayStr):
    filename = f'/src/data/lw_cost_revenue_country_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f"""
select
    install_day,
    app_package,
    country,
    mediasource,
    campaign_name,
    sum(cost_value_usd) as cost,
    sum(revenue_h24) as revenue_h24,
    sum(revenue_d1) as revenue_d1,
    sum(revenue_d3) as revenue_d3,
    sum(revenue_d7) as revenue_d7,
    sum(revenue_d14) as revenue_d14,
    sum(revenue_d30) as revenue_d30,
    sum(revenue_d60) as revenue_d60,
    sum(revenue_d90) as revenue_d90,
    sum(revenue_d120) as revenue_d120,
    sum(revenue_d150) as revenue_d150
from
    dws_overseas_public_roi
where
    app = '502'
    and facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    app_package,
    country,
    mediasource,
    campaign_name
;
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)

        df.to_csv(filename, index=False)

    return df

def main():
    df = getRevenueData('20240101', '20250501')
    
    




if __name__ == "__main__":
    main()
    print("Script executed successfully.")
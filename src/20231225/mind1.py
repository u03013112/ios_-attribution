import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSqlBj


def getDataFromMC(startDayStr,endDayStr):
    filename = '/src/data/20231225_mind1_%s_%s.csv'%(startDayStr,endDayStr)
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        print('从MC获得数据')


    sql = f'''
        WITH material_cost AS (
            SELECT
                mediasource,
                app_package,
                material_name,
                SUBSTRING(install_day, 1, 6) AS install_month,
                max(video_url) as video_url,
                sum(cost_value_usd) as cost_value_usd
            FROM
                rg_bi.dws_material_overseas_country
            WHERE
                app = 102
                AND install_day between {startDayStr} and {endDayStr}
            GROUP BY
                mediasource,
                app_package,
                material_name,
                install_month
        ),
        daily_cost AS (
            SELECT
                mediasource,
                app_package,
                install_month,
                sum(cost_value_usd) as daily_cost_value_usd
            FROM
                material_cost
            GROUP BY
                mediasource,
                app_package,
                install_month
        )
        SELECT
            mc.mediasource,
            mc.app_package,
            mc.video_url,
            mc.material_name,
            mc.install_month,
            mc.cost_value_usd,
            mc.cost_value_usd / dc.daily_cost_value_usd as cost_rate
        FROM
            material_cost mc
            JOIN daily_cost dc ON mc.mediasource = dc.mediasource
            AND mc.app_package = dc.app_package
            AND mc.install_month = dc.install_month
        WHERE
            mc.cost_value_usd / dc.daily_cost_value_usd >= 0.1;
    '''
    print(sql)
    df = execSql(sql)

    df.to_csv(filename,index=False)
    print('已保存%s'%filename)
    return df
    

def main():
    df = getDataFromMC('20220101','20231231')
    print(df.head())

if __name__ == '__main__':
    main()
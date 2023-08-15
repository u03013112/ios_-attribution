# 获得 安卓越南 分campaign的7日回收数据

import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getVnR7Usd():
    sql = '''
        select
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) AS install_date,
            campaign,
            sum(revenue_d7_double) as r7usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp.vn'
            AND zone = 0
        group by
            install_date,
            campaign
        ;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/zk2/vnR7usd.csv', index=False)
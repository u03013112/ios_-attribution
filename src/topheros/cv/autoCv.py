# 定期的计算，最近一个月的cv值，是否比之前的版本有长足进步，如果有，就保存当前版本，并通知管理员
# 每周一早上10点，执行一次
# 10 10 * * 1 docker exec -t ios_attribution python /src/src/lastwar/cv/autoCv.py
import sys
sys.path.append('/src')

from src.maxCompute import execSql

import datetime
import pandas as pd
from src.tools.cvTools import makeLevels,makeLevelsByJenkspy,makeLevelsByKMeans,checkLevels

def getPayDataFromMC():
    todayStr = datetime.datetime.now().strftime('%Y%m%d')
    oneMonthAgoStr = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')
    print('获得%s~%s的付费数据'%(oneMonthAgoStr,todayStr))

    sql = f'''
        select
            install_day AS install_date,
            game_uid as uid,
            sum(
                case
                    when event_time - install_timestamp <= 24 * 3600 then revenue_value_usd
                    else 0
                end
            ) as revenue
        from
            dwd_overseas_revenue_allproject
        where
            app = 116
            and zone = 0
            and day between {oneMonthAgoStr} and {todayStr}
            and app_package = 'id6450953550'
        group by
            install_day,
            game_uid
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

from src.report.feishu.feishu import sendMessageDebug
def main():
    df = getPayDataFromMC()
    df.to_csv('/src/data/topherosPayData.csv',index=False)
    df = pd.read_csv('/src/data/topherosPayData.csv')
    # 进行一定的过滤，将收入超过2000美元的用户收入改为2000美元
    df.loc[df['revenue']>2000,'revenue'] = 2000

    message = 'topheros CV档位自动测试\n\n'

    # 计算旧版本的Mape
    cvMapDf = pd.read_csv('/src/src/topheros/cv/cvMap20240201.csv')
    cvMapDf = cvMapDf.loc[
        (cvMapDf['event_name'] == 'af_skad_revenue')
    ]
    levels = cvMapDf['max_event_revenue'].tolist()
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    message += '旧版本\n'
    message += f'{levels}\n'
    message += f'{mape*100:.2f}%\n\n'

    levels = makeLevels(df,usd='revenue',N=64)
    levels = [round(x,2) for x in levels]
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    message += 'makeLevels\n'
    message += f'{levels}\n'
    message += f'{mape*100:.2f}%\n\n'

    levels = makeLevelsByKMeans(df,usd='revenue',N=64)
    levels = [round(x,2) for x in levels]
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    message += 'makeLevelsByKMeans\n'
    message += f'{levels}\n'
    message += f'{mape*100:.2f}%\n\n'
    
    print(message)
    sendMessageDebug(message)

if __name__ == '__main__':
    main()
    

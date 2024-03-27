
# 检测融合归因是否正常的运行
# 每隔一段时间检测一次，将结果写入飞书
import time
import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def check(tableName):
    # dayStr 是T-2的日期，即前天的日期，类似20240101
    today = datetime.date.today()
    dayStr = (today - datetime.timedelta(days=2)).strftime('%Y%m%d')
    print('检测日期：',dayStr)

    sql = f'''
        select 
            count(*) as cnt
        from 
            {tableName} 
        where 
            day={dayStr}
        ;
    '''

    result = execSql(sql)
    cnt = result['cnt'][0]
    print('检测结果：',cnt)
    return cnt


import rpyc
def main():
    retryMax = 10
    reported = {'topwar': False, 'lastwar': False, 'topheros': False}
    tableNames = {
        'topwar': 'topwar_ios_funplus02_adv_uid_mutidays_campaign2',
        'lastwar': 'lastwar_ios_funplus02_adv_uid_mutidays_campaign2',
        'topheros': 'topheros_ios_funplus02_adv_uid_mutidays_campaign2'
    }

    for retry in range(retryMax):
        for prefix, tableName in tableNames.items():
            if not reported[prefix]:
                cnt = check(tableName)
                if cnt > 0:
                    conn = rpyc.connect("192.168.40.62", 10001)
                    conn.root.sendMessageDebug(f"{prefix} 融合归因今日完成!")
                    reported[prefix] = True

        if all(reported.values()):
            break
        else:
            if retry == retryMax - 1:
                not_reported = [k for k, v in reported.items() if not v]
                conn = rpyc.connect("192.168.40.62", 10001)
                conn.root.sendMessageDebug(f"{', '.join(not_reported)} 融合归因今日失败，已达到最大重试次数!")
                break

        time.sleep(60 * 20)    

if __name__ == '__main__':
    main()

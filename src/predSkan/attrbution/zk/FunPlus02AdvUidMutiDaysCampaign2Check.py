
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
    for retry in range(retryMax):
        topwarCnt = check('topwar_ios_funplus02_adv_uid_mutidays_campaign2')
        lastwarCnt = check('lastwar_ios_funplus02_adv_uid_mutidays_campaign2')
        if topwarCnt > 0 and lastwarCnt > 0:
            conn = rpyc.connect("192.168.40.62", 10001)
            conn.root.sendMessageDebug("融合归因今日完成!")
            break
        else:
            if retry == retryMax - 1:
                conn = rpyc.connect("192.168.40.62", 10001)
                conn.root.sendMessageDebug("融合归因今日失败，已达到最大重试次数!")
                break
        # 20分钟检测一次
        time.sleep(60*20)
    

if __name__ == '__main__':
    main()

# 验证归因成果
# 目前方式是将所有归因结论直接上传数数
# 最后在数数中进行验证，比如过滤拥有idfa的数据，然后计算实际归因于推测归因的比值。

# 不同的归因方式可验证的方案可能不同，比如目前主要用cv值进行归因的方案，就只能针对24小时内付费用户进行归因。

import sys
sys.path.append('/src')
from src.maxCompute import execSql

if __name__ == '__main__':
    sql = '''
        select *
        from topwar_ios_funplus02_adv_uid_mutidays_campaign
        where day > 0
        order by day desc
        limit 100
        ;
    '''
    df = execSql(sql)
    print(df)

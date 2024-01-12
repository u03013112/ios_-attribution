import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getMilestonesStartDate():
    sql = '''
        select 
            max(startday) as startday
        from 
            ads_application_milestones_v2
        where
            app_package_group = 'GLOBAL'
            and app_package_sys = 'IOS'
        ;
    '''
    result = execSql(sql)
    startDate = result['startday'][0]
    return startDate

if __name__ == '__main__':
    print(getMilestonesStartDate())
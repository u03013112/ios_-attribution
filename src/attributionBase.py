# 归因算法的base类
import os
import sys
sys.path.append('/src')
from src.ss import Data

class AttributionBase:
    def __init__(self,name='',version='0.0.1'):
        if name == '':
            # 直接用文件名做name
            filename = os.path.basename(__file__)
            if len(filename)>3 and filename[-3:]=='.py':
                self.name = filename[:-3]
        else:
            self.name = name
        self.version = version

    def attribution(self,since=None,until=None):
        ret = []
        # 从数数里获得需要的用户数据，以供后续处理
        data = Data(since=since,until=until).get24HPayUserInfo()
        
        for uid in data.keys():
            ret.append({
                'uid':uid,
                'installDate':data[uid]['installDate'],
                'media':None,
                'country':None,
                'campaign':None,
            })

        return ret

if __name__ == '__main__':
    base = AttributionBase()
    base.attribution(since='2022-08-01',until='2022-08-31')

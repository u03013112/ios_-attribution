# 归因算法的base类
import os
import sys
import time
sys.path.append('/src')
from src.ss import Data

class AttributionBase:
    def __init__(self,name='',version='0.0.1',since=None,until=None):
        if name == '':
            # 直接用文件名做name
            filename = os.path.basename(__file__)
            if len(filename)>3 and filename[-3:]=='.py':
                self.name = filename[:-3]
        else:
            self.name = name
        self.version = version
        if since is None:
            now = time.time()
            before1days = time.localtime(now - 3600 * 24)
            before30days = time.localtime(now - 3600 * 24 * 30)
            since = time.strftime('%Y-%m-%d',before30days)
            until = time.strftime('%Y-%m-%d',before1days)
        self.since = since
        self.until = until

    def attribution(self):
        ret = []
        # 从数数里获得需要的用户数据，以供后续处理
        data = Data(since=self.since,until=self.until).get24HPayUserInfo()
        
        for uid in data.keys():
            uidData = data[uid]
            # 应该在这里准备概率归因，base就直接都填None值了
            ret.append({
                'uid':uid,
                'installDate':uidData['installDate'],
                'media':uidData['media'],
                'country':uidData['country'],
                'campaign':uidData['campaign'],
                'pMedia':None,
                'pCampaign':None
            })

        return ret

if __name__ == '__main__':
    base = AttributionBase()
    base.attribution(since='2022-08-01',until='2022-08-31')

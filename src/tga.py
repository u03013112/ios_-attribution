from tgasdk.sdk import TGAnalytics, BatchConsumer
import datetime

import pandas as pd
import sys
sys.path.append('/src')
 
from src.tools import getFilename

class ThinkingData:
    def __init__(self):
        uri = 'https://tatracker.rivergame.net/'
        appid = 'cf7a0712b2e44e4882973fa137969fff'
        self.batchConsumer = BatchConsumer(server_uri=uri, appid=appid,compress=False)

    
    def uploadEvent(self,filename):
        self.ta = TGAnalytics(self.batchConsumer)

        # account_id = 'skan20221014'
        account_id = 3013112
        event_name = 'skanNullFill'
        df = pd.read_csv(getFilename(filename))

        for i in range(len(df)):
            
            time = datetime.datetime.strptime(df['install_date'].get(i),'%Y-%m-%d')
            skanUsd = df['skanUsd'].get(i)
            organicUsd = df['organicUsd'].get(i)
            nullUsd = df['nullUsd'].get(i)
            afUsd = df['afUsd'].get(i)
            properties = {
                "#time":time,
                "skanUsd":skanUsd,
                "organicUsd":organicUsd,
                "nullUsd":nullUsd,
                "afUsd":afUsd,
                "filename":filename
            }
            try:
                self.ta.track(account_id = account_id, event_name = event_name, properties = properties)
                # print(properties)
            except Exception as e:
                print(e)    
        self.ta.flush()
        print('发送事件成功:',len(df))
        self.ta.close()


    def uploadEventNullByMedia(self,filename):
        self.ta = TGAnalytics(self.batchConsumer)

        account_id = 3013112
        event_name = 'skanNullFillByMedia'
        df = pd.read_csv(getFilename(filename))

        for i in range(len(df)):
            
            time = datetime.datetime.strptime(df['install_date'].get(i),'%Y-%m-%d')
            media = df['media'].get(i)
            revenueUsd = df['revenueUsd'].get(i)
            nullUsd = df['nullUsd'].get(i)
            properties = {
                "#time":time,
                "revenueUsd":revenueUsd,
                "nullUsd":nullUsd,
                "media":media,
                "filename":filename
            }
            try:
                self.ta.track(account_id = account_id, event_name = event_name, properties = properties)
                # print(properties)
            except Exception as e:
                print(e)    
        self.ta.flush()
        print('发送事件成功:',len(df))
        self.ta.close()

    
if __name__ == '__main__':
    ss = ThinkingData()
    # ss.uploadEvent('log20220601_20220930_28_sample_predict')
    ss.uploadEventNullByMedia('log20220601_20220930_28_sample_media_byMedia')
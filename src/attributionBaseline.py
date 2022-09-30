# 归因算法基础线
import datetime
import pandas as pd
import sys
sys.path.append('/src')
from src.attributionBase import AttributionBase
from src.ss import Data
from src.smartCompute import SmartCompute
class AttributionBaseline( AttributionBase):
    def getSkanDataFromSmartCompute(self):
        sinceTime = datetime.datetime.strptime(self.since,'%Y-%m-%d')
        unitlTime = datetime.datetime.strptime(self.until,'%Y-%m-%d')
        # 根据苹果文档，付费用户会在用户激活后24小时到48小时之间进行skan上报
        sinceTime += datetime.timedelta(days=1)
        # 但是根据af数据的规律来看，往后多延长3天，确保数据足够
        unitlTime += datetime.timedelta(days=3)
        sinceTimeStr = sinceTime.strftime("%Y%m%d")
        unitlTimeStr = unitlTime.strftime("%Y%m%d")
        sql='''
            select
                *
            from ods_platform_appsflyer_skad_details
            where
                skad_conversion_value>0
                and day>=%s and day <=%s
        '''%(sinceTimeStr,unitlTimeStr)
        # print(sql)
        smartCompute = SmartCompute()
        pd_df = smartCompute.execSql(sql)
        smartCompute.writeCsv(pd_df,'/src/data/baseline20220801.csv')
        return pd_df
    def getSkanDataFromCsv(self,csvFilename):
        smartCompute = SmartCompute()
        pd_df = smartCompute.readCsv(csvFilename)
        return pd_df
        
    def attribution(self):
        ret = []
        # 从数数里获得需要的用户数据，以供后续处理
        data = Data(since=self.since,until=self.until).get24HPayUserInfo()
        
        # skanDataFrame = self.getSkanDataFromSmartCompute()
        skanDataFrame = self.getSkanDataFromCsv('/src/data/baseline20220801.csv')
        # skan数据添加一列，是否已经分配，0为未分配，1是已分配
        skanDataFrame['used'] = 0
        for uid in data.keys():
            uidData = data[uid]

            # 尝试分配
            # 找到符合要求的所有行，
            # 要求1：时间匹配的，这里直接匹配install_date
            # 要求2：cv匹配
            # 要求3：还未分配

            # 2022-08-07 08:35:20.000 => 2022-07-31
            installDate = uidData['installDate'].split()[0]

            usd = uidData['usd']
            afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
            
            # if usd > afCvMapDataFrame.max_event_revenue[len(afCvMapDataFrame.max_event_revenue)-1]:
            #     usd = afCvMapDataFrame.max_event_revenue[len(afCvMapDataFrame.max_event_revenue)-1]
            # 默认值是最大值，这里要求usd要大于0，这是个偷懒的写法
            cv = len(afCvMapDataFrame.max_event_revenue)-1
            # 暂时不考虑开闭区间问题，卡在区间边缘的数据并不常见
            cvDataFrame = afCvMapDataFrame[(afCvMapDataFrame.min_event_revenue<=usd) & (afCvMapDataFrame.max_event_revenue>usd)]
            if len(cvDataFrame) == 1:
                # 这里索引值就是cv值
                cv = cvDataFrame.conversion_value.index[0]
            else:
                print("付费金额%f找不到对应的cv值"%(usd))

            readyDataFrames = skanDataFrame[(skanDataFrame.skad_conversion_value == cv ) & (skanDataFrame.install_date == installDate) & (skanDataFrame.used == 0)]
            if len(readyDataFrames) <= 0:
                # 一个匹配的都没有，直接填None吧先
                ret.append({
                    'uid':uid,
                    'installDate':uidData['installDate'],
                    'media':uidData['media'],
                    'country':uidData['country'],
                    'campaign':uidData['campaign'],
                    'pMedia':None,
                    'pCampaign':None
                })
            else:
                # 随机选择一个作为归因
                chooseDataFrame = readyDataFrames.sample(1)
                index = chooseDataFrame.index[0]
                # 标记已使用
                chooseDataFrame.used = 1
                pMedia = chooseDataFrame.media_source[index]
                pCampaign = chooseDataFrame.ad_network_campaign_name[index]

                ret.append({
                    'uid':uid,
                    'installDate':uidData['installDate'],
                    'media':uidData['media'],
                    'country':uidData['country'],
                    'campaign':uidData['campaign'],
                    'pMedia':pMedia,
                    'pCampaign':pCampaign
                })

        return ret

if __name__ == '__main__':
    baseline = AttributionBaseline(since='2022-08-01',until='2022-08-03')
    # print(baseline.getSkanDataFromSmartCompute())
    baseline.attribution()
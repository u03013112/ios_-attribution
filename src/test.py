# 验证归因成果
# 目前方式是将所有归因结论直接上传数数
# 最后在数数中进行验证，比如过滤拥有idfa的数据，然后计算实际归因于推测归因的比值。

# 不同的归因方式可验证的方案可能不同，比如目前主要用cv值进行归因的方案，就只能针对24小时内付费用户进行归因。

import sys
sys.path.append('/src')

from src.tools import getFilename
# 更换不同的归因算法，就改这一行
# from src.attributionBase import AttributionBase as Attribution

# attribution = Attribution()
# retList = attribution.attribution(since='2022-08-01',until='2022-08-03')

# # 上传数数
# from tgasdk.sdk import TGAnalytics, BatchConsumer

# # 事件名暂定
# event_name = 'skan_attribution'

# uri = 'https://tatracker.rivergame.net/'
# appid = 'cf7a0712b2e44e4882973fa137969fff'
# batchConsumer = BatchConsumer(server_uri=uri, appid=appid,compress=False)

# ta = TGAnalytics(batchConsumer)

# # 先将所有的归因结论按照事件上传数数，算是留个记录
# # try:
# #     for ret in retList:
# #         account_id = ret['uid']

# #         properties = {
# #             "#time":ret['installDate'],
# #             "name":attribution.name,
# #             "version":attribution.version,
# #             "media":ret['media'],
# #             "country":ret['country'],
# #             "campaign":ret['campaign'],
# #         }
# #         ta.track(account_id = account_id, event_name = event_name, properties = properties)
# #     ta.flush()
# #     print('发送事件成功:',len(retList))
# # except Exception as e:
# #     print(e)

# # 然后更新用户属性，用于数数上快速的查看准确率
# # 用户属性会有后缀，用于保存多套结论
# # 暂时只针对media和campaign进行匹配测试，国家部分暂时没有想到价值
# try:
#     for ret in retList:
#         account_id = ret['uid']
#         propertiesSuffix = attribution.name + attribution.version
#         pCampaignMatch = None
#         if 'campaign' in ret :
#             if 'pCampaign' in ret and ret['campaign'] == ret['pCampaign']:
#                 pCampaignMatch = 1
#             else:
#                 pCampaignMatch = 0
#         if 'media' in ret :
#             if 'pMedia' in ret and ret['media'] == ret['pMedia']:
#                 pMediaMatch = 1
#             else:
#                 pMediaMatch = 0
        
#         properties = {
#             'pMedia'+propertiesSuffix:ret['pMedia'],
#             'pCampaign'+propertiesSuffix:ret['pCampaign'],
#             'pMediaMatch'+propertiesSuffix:pMediaMatch,
#             'pCampaignMatch'+propertiesSuffix:pCampaignMatch,
#         }
#         ta.user_set(account_id = account_id,properties = properties)
#     ta.flush()
#     print('发送用户属性成功:',len(retList))
# except Exception as e:
#     print(e)

from sklearn import metrics
import numpy as np
import pandas as pd

def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100
def smapeFunc(a, f):
    return 1/len(a) * np.sum(2 * np.abs(f-a) / (np.abs(a) + np.abs(f))*100)

def test(logFileName):
    df = pd.read_csv(getFilename(logFileName))
    y_true = df['afUsd']
    y_pred = df['skanUsd']+df['organicUsd']+df['nullUsd']
    r2 = metrics.r2_score(y_true,y_pred)
    mape = mapeFunc(y_true,y_pred)
    print('filename:%s,r2:%.2f,mape:%.2f%%'%(logFileName,r2,mape))

# 用差值来做指标
def test2(logFileName,n=3):
    df = pd.read_csv(getFilename(logFileName))
    # print(df)
    # df.loc[(df.afUsd<df.skanUsd),'afUsd']=df.skanUsd
    df = df[['skanUsd','organicUsd','nullUsd','afUsd']]
    
    dfSum = df.groupby(df.index//n).sum()
    # print(df)

    y_true = dfSum['afUsd'] - dfSum['skanUsd']
    y_pred = dfSum['organicUsd'] + dfSum['nullUsd']
    # print(y_true,y_pred)

    r2 = metrics.r2_score(y_true,y_pred)
    mape = mapeFunc(y_true,y_pred)
    print('filename:%s,n:%d,r2:%.2f,mape:%.2f%%'%(logFileName,n,r2,mape))
if __name__ == '__main__':
    # test('log20220601_20220930_7_sample_predict')
    # test('log20220601_20220930_14_sample_predict')
    # test('log20220601_20220930_28_sample_predict')

    # test2('log20220601_20220930_7_sample_predict')
    # test2('log20220601_20220930_14_sample_predict')
    test2('log20220601_20220930_28_sample_predict')
# 验证归因成果
# 目前方式是将所有归因结论直接上传数数
# 最后在数数中进行验证，比如过滤拥有idfa的数据，然后计算实际归因于推测归因的比值。

# 不同的归因方式可验证的方案可能不同，比如目前主要用cv值进行归因的方案，就只能针对24小时内付费用户进行归因。

import sys
sys.path.append('/src')

# 更换不同的归因算法，就改这一行
from src.attributionBase import AttributionBase as Attribution

attribution = Attribution()
retList = attribution.attribution(since='2022-08-01',until='2022-08-03')

# 上传数数
from tgasdk.sdk import TGAnalytics, BatchConsumer

# 事件名暂定
event_name = 'skan_attribution'

uri = 'https://tatracker.rivergame.net/'
appid = 'cf7a0712b2e44e4882973fa137969fff'
batchConsumer = BatchConsumer(server_uri=uri, appid=appid,compress=False)

ta = TGAnalytics(batchConsumer)

try:
    for ret in retList:
        account_id = ret['uid']

        properties = {
            "#time":ret['installDate'],
            "name":attribution.name,
            "version":attribution.version,
            "media":ret['media'],
            "country":ret['country'],
            "campaign":ret['campaign'],
        }
        ta.track(account_id = account_id, event_name = event_name, properties = properties)
    ta.flush()
    print('发送事件成功:',len(retList))
except Exception as e:
    print(e)
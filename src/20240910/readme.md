# 融合归因 改进方案

改为彻底采用平均值进行预估。

即找到真实用户的7日付费平均值，然后预估SKAN中对应的


Facebook api 可以获得
时间（天），campaign id，skad network id，spend，installs

得到skan 原始报告

skad network，skad network id，cv，installs，postback时间

怎么对应起来？
skan 找到对应的 激活时间范围，2~3天。

找到所有的skad network id可以对应的campaign id。

如果有多个campaign id，怎么处理？
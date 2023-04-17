# 目前有数据Df,每个用户一行
# 列名为：uid,r1usd,r7usd,installDate,f1,f2……
# 其中uid是用户唯一索引，r1usd是用户首日充值金额，r7usd是用户7日充值金额
# installDate是用户安装日期。
# f1,f2……都是用户行为，即特征。
# r1usd也可以是特征。
# 希望用特征（不包含r7usd)将用户分为64类，记作cv，取值为0，1,2,3……63
# 最终的调整标准是 将64类中的每一类的用户，找到r7usdCvN，N是0~63，用户加入一列r7usdp，r7usdp的值等于该用户对应分类的r7usdCvN。
# 按installDate对用户数据汇总，r7usd与r7usdp进行汇总求和，计算汇总值的MAPE与R2，作为最终的评价指标。
# 要求：MAPE尽量小。R2尽量大。

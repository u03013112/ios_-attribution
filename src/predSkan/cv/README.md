# 尝试进行cv改进，用于获得更准确的大盘预测结果

## 步骤
1、将目前纯首日回收cv进行简化，简化方案未定。将不同的简化方案进行关联性测试，R1与R7都要测试。    
预计之中cv简化会带来关联性下降，下降的幅度问题而已。    
2、将想到的几个维度进行cv化，即档位化，尝试从10个档到30个档。可以先做关联性测试，然后再做分档后的关联性测试。
> 想到的维度：
>> 首日在线时间（秒）
>> 首日等级（或者升级次数）
>> 首日打开app次数
>> 常见礼包
>> 其他事件，这个比较麻烦


## 其他想法
对于cv的分开预测：比如将不付费的用户的63位用于其他信息存储。
这种情况如何做一体化训练？每个用户都有128位cv，付费的用户只拥有付费的部分，不付费的只拥有不付费的。这是一个可能得情况？将不付费的用户进行再细分。
这种方案可能要针对首日没有付费的用户的行为与第二日之后付费的关联。

## 
总体上还是要找到更加有效的付费关联属性
算关联性的话数据只能直接用数数的数据
之后如果要和AF数据或者BI数据做融合的时候，需要先用uid做join

# 还是绕不开iOS归因

## 目前有的归因方案

### 撞库

> 将首日（24H）付费金额进行分桶，暂定就是按照cv分成63个桶。然后将不同的桶进行分析。
应该先排除idfa用户？

>> 总数是否可以对得上。即 大盘 >= SUM（媒体），大于的部分为自然量。自然量是否是一个较为平稳的值。存在的潜在问题，我们是按照uid来做的唯一，iOS是按照设备来做的唯一，所以应该存在一些误差。正好AF今天来讲SKAN，这个可以和AF再确认一下。
>> 需要确定一个评判撞库准确性的指标。如果确定是正确分配没有异议的部分应该是最高分，然后就是有双重可能性的，三重可能性的……然后将上述按照人数、金额（首日、7日）进行划分。
或许可以用安卓来做金额验证，撞库之后的收入金额与7日金额分别算MAPE和R2

>> 撞库的方案应该是基于 安装日期 + cv

额外的，有一些属性可能可以加入到可用区分属性上
country_code

是否应该只看did_win == true的归因结论，目前看到除了true就是null，没有false的情况
fidelity_type 是触点类型，有0,1和null

skad_version 2.0 2.1 2.2 3.0 和 null，3.0和null最多，其他都很少

### 多目标训练

> 尝试搭建一个非全连接神经网络，让机器去揣测skan与最终大盘的收入金额的关系。

>> 大致思路是对每个媒体单独做一个7日付费增长率，然后尝试将首日收入金额乘进去再求和， 获得一个总和，再和最终大盘的总收入金额进行mse loss，如果这个方案行得通，将中间得到的媒体倍率拿出来就是希望得到的比率。

这个需要去查询一下文档，是否支持。

### AF方案

在相同的范围内进行相似用户抽取，然后用相似用户的近期表现进行skan部分预测

在android上进行测试：
1、将android数据中80%的用户的uid忽略，暂时认为是无法追踪用户。
2、将所有android数据按媒体进行cv化。
3、找到待预测媒体的cv分布。
4、在一直的用户中，相同范围内，一定的时间内（比如7~14天内），进行用户抽取，获得与待预测类似cv分布。
5、多次重复步骤4并进行7日付费增长率的预测。
6、进行验算，计算MAPE。

其中咱是想明白的点：
1、iOS中有一些用户是SKAN中没有的，但是却可以在overview中被归因，这部分用户有多少？
2、cv分布的相似程度，可能控制在千分之一的四舍五入？这个可以尝试设置几个档位，百分之一，千分之一，万分之一。精度越高，对范围要求越高。
3、重复步骤4的次数，这个可以先来个100次，将每次的mape和累计算术平均的mape都做出来。


# AF与SKAN对数
AF文档 https://support.appsflyer.com/hc/zh-cn/articles/360011307357#display-by-1 中提到：
激活日期是基于回传接收日期推算的，具体方法如下：回传接收日期 - 36小时 - [末次互动范围平均小时数]。默认[末次互动范围平均小时数]为12小时，但如果转化值为0，则末次互动范围平均小时数也为0。

苹果文档 https://developer.apple.com/documentation/storekit/skadnetwork/receiving_ad_attributions_and_postbacks 中提到：
For ads signed with version 3 or earlier, the device sends install-validation postbacks 0–24 hours after a 24-hour timer expires following the final call to update the conversion value. The total delay from the final conversion update to receiving the postback is 24–48 hours.

没有明确的说明付费用户会比较提前。

苹果文档 https://developer.apple.com/documentation/storekit/skadnetwork/3919928-updatepostbackconversionvalue 中提到：
The 24-hour timer restarts each time the app calls this method with a valid conversionValue that’s greater than the previous value.
所以付费用户的安装时间需要额外再往前追溯一天。

所以可以用SKAN报告中的timestamp计算出他可能的真实安装时间范围。

然后用真是安装时间范围，映射到2个自然日内，并按照具体时间将这个用户进行概率分割，比如他应该50% 2月1日，50% 2月2日，这样是否比直接将他定位到某日更加的准确？

如果这样的话，就可以将之前统计的CV count变成小数，这样在人数较多的时候回更加的准确？

另外，从AF处获得想法：利用SSOT将IDFA用户从SKAN中排除出去。
排除掉的可能不只是IDFA。但是这种方案确实使得撞库的范围更准了。

既然iOS不能有效的校验安装日期，那么就尝试用安卓数据来做测试。
将安卓的媒体数据按照安装时间 + 24~48小时随机时间来计算误差。

误差可以用安卓来准确率或者变差率计算，主要指标应该是每日的首日付费金额，7日付费金额 MAPE 与 线性相关性。

这个方案的问题可能还是不能校验。可能还是需要用之前的校验方式来做测试。

或者尝试多对几天进行汇总，降低误差。

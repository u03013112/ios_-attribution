# 针对applovin的定性分析

## 1、数据

获得指定项目（比如lastwar），分媒体+分国家 的 花费 与 回收 数据（1日、7日，暂时不需要更长久）。

找出applivin数据中比较特别的时间段，比如一段时间的applovin花费占比极低区间，或者一段时间的applovin花费占比极高区间。

这些特殊时间段是主要的分析对象。当然，还啊需要对照组。所以，可能需要按照applovin的花费占比，将数据分为几个组，然后对照组进行分析。可以做一定的时间维度汇总，暂定按周。

## 2、分析

主要需要定性的内容：

1、applovin是否抢夺了别的媒体的用户
2、applovin是被高估了还是被低估了
3、applovin的roi是否稳定

可以从以下几个方面进行分析：

1、针对是否抢夺媒体用户，可以找到applivin花费占比不同时，其他媒体表现（roi）是否稳定来分析。需要找到applivin占比变化剧烈的周期，和这个周期附近的周期。获得主要媒体（花费占比较高的媒体）的roi数据，然后对比这两个周期的roi数据，看是否有明显的变化。

可能存在问题：主要媒体的花费金额最好也比较稳定，否则他们的roi变化可能是由于花费变化导致的。

2、针对是否被高估，或者低估，可以找到只有applovin花费占比高的周期和周围周期。甚至找到只有applovin一种媒体在投放广告的周期，这样所有该国家周期内收入都可以算作applovin的收入。

3、相同媒体花费占比的时候，媒体收入与大盘收入的相关性对比。以及相同花费占比的时候的媒体roi与大盘roi的稳定性对比。

可能不仅要参考媒体花费占比，还需要媒体回收占比作为参考。

或者 各媒体的花费或花费占比与ROI占比进行线性相关性分析。比如，Google的花费占比与Facebook的ROI占比 呈 负相关，那么就可能是抢量关系。

## 3、方案

1、按照时间段，找到每个媒体花费占比不同的周期。另外也将媒体回收占比作为参考。

其中占比不同可以分为：小于等于1%，1~5%，5~10%，10~20%，20~50%，大于等于50%。

2、将占比类似的周期进行合并，找到applovin比较大段的个比例周期。并找到该周期附近的周期的其他媒体花费占比，回收占比。

3、最好能找到比较好的对照组，即只有applovin有明显的花费占比变化，其他媒体花费占比变化不大的周期。

4、在此周期进行 数据分析、画图 等方式，从数据层面和直观层面进行分析。

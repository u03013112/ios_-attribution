# 素材数据

## 对数

### 素材数据可靠性

目前有两个表 dws_material_overseas_data_public，dws_material_overseas_data_inventory_public 。
分别计算汇总之后与大盘进行对比。其中包括维度上包括花费与回收，其中只有安卓可以比对回收。
分平台，国家，媒体，日期（天，周，月）进行对比。

## 基础分析

### 畅销素材分析

找到不同维度的畅销素材。大盘、平台、国家、媒体、日期（天，周，月）。
不同维度的畅销素材是否相似。
畅销素材的生命周期是否类似，是否可以找出规律。

1、畅销素材，在不同媒体、不同平台、在不同国家是否相似。
2、畅销素材，是否不同国家畅销的是不同的语言。比如在日本畅销的是日语，而在美国畅销的是英语。
3、畅销素材生命周期是否相似，即每个畅销素材的花费变化是相似的。
4、畅销素材是否有明显的接班现象，即新的畅销素材出现，是否加速了旧的畅销素材的下降。

## 素材内容分析

### 素材内容解析，并将解析的内容进行汇总，找到内容与畅销素材的关系

## 时间相关分析

素材都是有时效性的，即一个畅销素材在一段时间后会变得不畅销。

所以尝试找到畅销素材的时间规律。

1、畅销素材 开始畅销 时间与 素材开始投放时间的关系。是否所有的素材都是刚开始投放没多久就开始畅销。
2、畅销素材 畅销时间 长度是否类似。即一般的一个畅销素材会畅销多久。
3、畅销素材的生命周期是否都是类似的，即从开始投放，到不再畅销，甚至到停止投放，花费变化趋势是否是类似的，或线性相关的。

畅销周期可能影响最近分析周期选择，这个要先做，然后只分析最近一个周期内的数据。或者按照这个周期作为时间分段，分析每个周期内的数据。

## TODO

数据加入Google数据，国家改用国家分组。

1、计算畅销周期，各种不同维度的畅销周期是否相似。比如平台、国家、媒体、创意类型等。
2、在畅销素周期进行分析，重新分析之前的数据（分国家、分平台、分媒体），看看是否有不同。着重分析最近1~2个周期。统计维度。暂时想到的只有分创意类型统计。

3、制作 按照 国家、平台、媒体、周期时间 的属性 与 畅销 的相关性分析。素材维度。目前能用到的维度，只有创意标签，和原始素材名，可能还有投放时间。
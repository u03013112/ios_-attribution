# tiktok ios 预测

用10小时、14小时数据，预测3日收入。

## 数据

要获
得10小时、14小时数据。
付费分布、国家分布。这里没有想明白用付费用户还是用所有用户。
3天数据。

## 验证

一方面是尽可能的预测准确，比如mse，mae等。
另一方面是可能考虑采用分类，计算正确率。比如将ROI分为3类（偏高、中等、偏低），然后计算正确率。

## 模型

### 1、线性回归

回归之后再用分类验算

### 2、分类

直接用分类模型，并保存分类模型中效果最好的模型。
# 自制模型MMM

## 1.媒体数据

    分媒体进行数据整理
    获得了线性相关性相当高的数据，3日融合归因结论
    最好不使用具体的CV值，因为iOS的CV频繁地在变化
    比如付费率，或者超过某金额的付费比率，这个要重新计算相关性
    可以考虑用均线来做，波动性小很多，直接用均线做训练

    ```
        media: googleadwords_int
                payRate   r1usd10   r1usd20   r1usd50  r1usd100  r1usdSum  r7usdSum
        payRate   1.000000  0.886685  0.822489  0.654496  0.446481  0.211137  0.216469
        r1usd10   0.886685  1.000000  0.933466  0.725474  0.514347  0.330815  0.329885
        r1usd20   0.822489  0.933466  1.000000  0.789856  0.547443  0.329754  0.321401
        r1usd50   0.654496  0.725474  0.789856  1.000000  0.714691  0.308905  0.274302
        r1usd100  0.446481  0.514347  0.547443  0.714691  1.000000  0.313960  0.259582
        r1usdSum  0.211137  0.330815  0.329754  0.308905  0.313960  1.000000  0.943494
        r7usdSum  0.216469  0.329885  0.321401  0.274302  0.259582  0.943494  1.000000
        media: Facebook Ads
                payRate   r1usd10   r1usd20   r1usd50  r1usd100  r1usdSum  r7usdSum
        payRate   1.000000  0.782378  0.700681  0.508856  0.318899  0.362745  0.322418
        r1usd10   0.782378  1.000000  0.854594  0.621257  0.418403  0.488641  0.425802
        r1usd20   0.700681  0.854594  1.000000  0.685232  0.464108  0.493156  0.430316
        r1usd50   0.508856  0.621257  0.685232  1.000000  0.588712  0.507803  0.467121
        r1usd100  0.318899  0.418403  0.464108  0.588712  1.000000  0.477768  0.405100
        r1usdSum  0.362745  0.488641  0.493156  0.507803  0.477768  1.000000  0.910406
        r7usdSum  0.322418  0.425802  0.430316  0.467121  0.405100  0.910406  1.000000
        media: bytedanceglobal_int
                payRate   r1usd10   r1usd20   r1usd50  r1usd100  r1usdSum  r7usdSum
        payRate   1.000000  0.746581  0.639063  0.452659  0.309299 -0.160496 -0.165274
        r1usd10   0.746581  1.000000  0.837315  0.572141  0.396686 -0.079673 -0.092188
        r1usd20   0.639063  0.837315  1.000000  0.701721  0.552506 -0.042391 -0.051554
        r1usd50   0.452659  0.572141  0.701721  1.000000  0.750509  0.010148 -0.013829
        r1usd100  0.309299  0.396686  0.552506  0.750509  1.000000  0.020194  0.000775
        r1usdSum -0.160496 -0.079673 -0.042391  0.010148  0.020194  1.000000  0.921140
        r7usdSum -0.165274 -0.092188 -0.051554 -0.013829  0.000775  0.921140  1.000000
        media: snapchat_int
                payRate   r1usd10   r1usd20   r1usd50  r1usd100  r1usdSum  r7usdSum
        payRate   1.000000  0.748850  0.558269  0.283772  0.195382  0.101325  0.057396
        r1usd10   0.748850  1.000000  0.717269  0.386433  0.296983  0.167387  0.105131
        r1usd20   0.558269  0.717269  1.000000  0.547152  0.299310  0.211623  0.149159
        r1usd50   0.283772  0.386433  0.547152  1.000000  0.660275  0.213574  0.151412
        r1usd100  0.195382  0.296983  0.299310  0.660275  1.000000  0.201570  0.144051
        r1usdSum  0.101325  0.167387  0.211623  0.213574  0.201570  1.000000  0.886671
        r7usdSum  0.057396  0.105131  0.149159  0.151412  0.144051  0.886671  1.000000
    ```

## 2.广告相关情况

    尝试将广告花费、展示数、点击数、安装数等数据进行整理
    对这些数据与7日回收金额或者 7日收入/1日收入 进行相关性分析
    如果可能，可以尝试用一些进阶数据进行分析，比如CPM，CPC，CPI等

    ```
        media: googleadwords_int
            r1usd     r7usd  impressions    clicks  installs      cost
        r1usd        1.000000  0.943494     0.733200  0.543218  0.549205  0.857496
        r7usd        0.943494  1.000000     0.755040  0.517973  0.531943  0.870956
        impressions  0.733200  0.755040     1.000000  0.748437  0.719237  0.871857
        clicks       0.543218  0.517973     0.748437  1.000000  0.956767  0.618988
        installs     0.549205  0.531943     0.719237  0.956767  1.000000  0.621059
        cost         0.857496  0.870956     0.871857  0.618988  0.621059  1.000000
        media: Facebook Ads
            r1usd     r7usd  impressions    clicks  installs      cost
        r1usd        1.000000  0.910406     0.649941  0.605041  0.602340  0.752992
        r7usd        0.910406  1.000000     0.639645  0.571680  0.564637  0.708130
        impressions  0.649941  0.639645     1.000000  0.950795  0.913607  0.864524
        clicks       0.605041  0.571680     0.950795  1.000000  0.974617  0.786683
        installs     0.602340  0.564637     0.913607  0.974617  1.000000  0.752486
        cost         0.752992  0.708130     0.864524  0.786683  0.752486  1.000000
        media: bytedanceglobal_int
            r1usd     r7usd  impressions    clicks  installs      cost
        r1usd        1.000000  0.921140     0.822509  0.840400  0.827269  0.874527
        r7usd        0.921140  1.000000     0.797420  0.801685  0.789222  0.808268
        impressions  0.822509  0.797420     1.000000  0.965360  0.967336  0.848788
        clicks       0.840400  0.801685     0.965360  1.000000  0.960101  0.881503
        installs     0.827269  0.789222     0.967336  0.960101  1.000000  0.823745
        cost         0.874527  0.808268     0.848788  0.881503  0.823745  1.000000
        media: snapchat_int
            r1usd     r7usd  impressions  clicks  installs  cost
        r1usd        1.000000  0.886671          NaN     NaN       NaN   NaN
        r7usd        0.886671  1.000000          NaN     NaN       NaN   NaN
        impressions       NaN       NaN          NaN     NaN       NaN   NaN
        clicks            NaN       NaN          NaN     NaN       NaN   NaN
        installs          NaN       NaN          NaN     NaN       NaN   NaN
        cost              NaN       NaN          NaN     NaN       NaN   NaN
    ```


## 3.其他数据

    比如 星期几 ，暂时没有想到其他数据
    先尝试看看是否有相关性

## 4.模型

    由于找到相关性较大的数据，所以考虑使用类似于线性回归的模型
    还是 用 媒体1 + 媒体2 + 媒体3 + 媒体4 = 总量 这样的模型
    再用总量 与 预测总量的 mse 进行loss计算

    每个媒体的输入都应该是 媒体数据 + 广告数据 + 其他数据
    自然量的输入应该是 媒体数据 + 其他数据

## 5.对于之前的模型矫正

    之前只用CV计算倍率，导致部分媒体（比如：Facebook）的偏差太大。
    所以考虑用其他数据来进行矫正，比如 广告信息，其他数据等。

## 6.其他方案

    只能做一些尝试
    尝试一：将模型分为多个，先将第一媒体和其他做拟合，再将第二媒体和其他做拟合，以此类推。
    尝试二：采用更复杂的模型，不再强行追求线性关系，输入还是那些，另外再把线性相关性高的数据也当做输入，然后也是用总量来做loss。
    尝试三：继续尝试二，更进一步，甚至考虑直接用全连接神经网络，输入是所有数据，输出是总量，然后用mse做loss。
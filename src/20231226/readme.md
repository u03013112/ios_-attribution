# af 对于 pc 游戏归因的问题整理

我理解的pc借助af归因流程：
1、在AF账号中设置并开通PC应用，可能还需要进行一些相关的设置。
2、pc应用中接入af sdk，类似app sdk，主要是各种打点，比如first app open之类。
3、落地页改造，落地页原有utm参数规范，在落地页加入af sdk，以便将utm参数传递给af。

是否大体流程就是这样，有没有遗漏的地方？
针对上面流程，有几个问题：
1、针对第2步，看到文档《应用对接（通过S2S或SDK）》提到device_ids好像需要我们自己处理，这个接入是类似app，我们在客户端接入sdk就可以了，还是要额外的在针对类似device_ids做特殊处理？
2、针对第3步，对于PC原生应用是不是建议使用落地页而不是直接归因链接。
3、sdk 与 s2s 是否可以同时使用？目前towpar的app是类似这个情况，类似first open之类的事件是sdk上报的，但是付费事件是s2s上报的，这种情况是否可以同时使用？
4、最终的归因数据，是不是和app的归因数据一样，可以通过api获取，我们bi直接获取af归因结果来完成我们的bi数据统计。
5、对媒体打点，是否是类似app，我们对接af sdk后就可以通过设置af来实现对媒体的转化数据传输？还是需要我们自己给媒体打点？如果我们投放的是落地页，比如果google只能投放vac广告，vac广告只能事件归因，发给google的转化数据能否做到和app类似逻辑，即按用户归因，而且只将用户早期的付费转化数据传给google。
6、这个产品的文档我看到比较新，是新产品吗，有什么应用案例吗，最好是规模比较大一点的，游戏类的，最好和我们情况类似的。


可以投电视广告，展示转化，通过IP归因
web有智能脚本，可以减少开发量，IP归因
暂时只有meta支持协助回传
原生pc会比较多

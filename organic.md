# 推测ios自然量回收

流程思路

```blockdiag
blockdiag {
    "找到指定日的af events安装数" -> "差值就是自然量，暂时这么认为"
    "找到对应日的skan安装数" -> "差值就是自然量，暂时这么认为" 
    -> "预测当日cv分布并进行记录"
    "找到前n日含有idfa的自然量cv分布" -> "预测当日cv分布并进行记录"
    -> "利用此方案预测一段时间 获得cv汇总" -> "+"
    "获得一段时间的skan cv对应金额" -> "+"
    -> 比较
    "获得一段时间的af events cv对应金额" -> 比较

    "找到指定日的af events安装数" [color = "greenyellow"]
    "找到对应日的skan安装数" [color = "greenyellow"]
    "差值就是自然量，暂时这么认为" [color = "greenyellow"]
    "找到前n日含有idfa的自然量cv分布" [color = "greenyellow"]
    "预测当日cv分布并进行记录" [color = "orange"];
    "利用此方案预测一段时间 获得cv汇总" [color = "orange"];
    "获得一段时间的skan cv对应金额" [color = "greenyellow"]
    "获得一段时间的af events cv对应金额" [color = "greenyellow"]
    "+" [color = "greenyellow"]
    "比较" [color = "pink"]
}
```
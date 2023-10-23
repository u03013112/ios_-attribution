# 国家分组

# 所有的getXXXGeoGroup函数都返回一个dict，key为国家名，value为国家分组名
# 类似于 {'Asia': ['China', 'Japan', 'Korea'], 'Europe': ['France', 'Germany', 'UK'], 'Other': ['Canada', 'Australia']}

# 获得海外iOS国家分组
# 先做一个简单的
def getIOSGeoGroup01():
    # 这是获得10月数据中排名比较靠前的国家
    # JP之后的国家用户数占比低于3%，统一归为Other
    ret = {
        'TW': ['TW'],
        'KR': ['KR'],
        'US': ['US'],
        'JP': ['JP'],
    }
    return ret
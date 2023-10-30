# 媒体分组

# 所有的getXXXMediaGroup函数返一个dict，key为媒体名，value为媒体分组名
# 类似于 {'Group1': ['Media1', 'Media2', 'Media3'], 'Group2': ['Media4', 'Media5', 'Media6'], 'Other': ['Media7', 'Media8']}

def getIOSMediaGroup01():
    # 其他媒体统一归为Other
    ret = [
        {'name':'facebook','codeList': ['Facebook Ads']},
        {'name':'google','codeList': ['googleadwords_int']},
        {'name':'bytedanceglobal','codeList': ['bytedanceglobal_int']},
    ]
    return ret

K2 = 5.986 
M2 = 3.416 
O2 = 2.594 
Q2 = 1.480 
S2 = 1.178 
U2 = 0.672 
W2 = 5.986 
Y2 = 2.594 
AA2 = 1.178 

FList = []

AList = [0.94191 ,0.89936 ,0.85035 ,0.79512 ,0.74696 ,0.70077 ,0.64543 ,0.59909 ,0.54929 ,0.49943 ,0.44926 ,0.39496 ,0.35065 ,0.29532 ,0.24864 ,0.20202 ,0.14645 ,0.09997 ,0.05307 ,0.00000]
BList = [0.960 ,0.980 ,1.000 ,1.065 ,1.125 ,1.185 ,1.245 ,1.310 ,1.380 ,1.445 ,1.505 ,1.565 ,1.610 ,1.665 ,1.705 ,1.745 ,1.775 ,1.800 ,1.825 ,1.845 ]
EList = [0.0000177778 ,0.0000400000 ,0.0000560000 ,0.0000653333 ,0.0000706667 ,0.0000751111 ,0.0000782222 ,0.0000808889 ,0.0000826667 ,0.0000840000 ,0.0000848889 ,0.0000852000 ,0.0000852444 ,0.0000852889 ,0.0000852889 ,0.0000853333 ,0.0000853333 ,0.0000856000 ,0.0000855556 ,0.0000855556]


def orientation(p, q, r):
    """返回三元组 (p, q, r) 的方向。
    0 -> p, q 和 r 共线
    1 -> 顺时针
    2 -> 逆时针
    """
    val = (q[1] - p[1]) * (r[0] - q[0]) - (q[0] - p[0]) * (r[1] - q[1])
    if val == 0:
        return 0
    elif val > 0:
        return 1
    else:
        return 2

def on_segment(p, q, r):
    """检查点 q 是否在线段 pr 上。"""
    if q[0] <= max(p[0], r[0]) and q[0] >= min(p[0], r[0]) and q[1] <= max(p[1], r[1]) and q[1] >= min(p[1], r[1]):
        return True
    return False

def segments_intersect(p1, q1, p2, q2):
    """检查线段 'p1q1' 和 'p2q2' 是否相交。"""
    o1 = orientation(p1, q1, p2)
    o2 = orientation(p1, q1, q2)
    o3 = orientation(p2, q2, p1)
    o4 = orientation(p2, q2, q1)

    # 一般情况
    if o1 != o2 and o3 != o4:
        return True

    # 特殊情况
    # p1, q1 和 p2 共线且 p2 在线段 p1q1 上
    if o1 == 0 and on_segment(p1, p2, q1):
        return True

    # p1, q1 和 q2 共线且 q2 在线段 p1q1 上
    if o2 == 0 and on_segment(p1, q2, q1):
        return True

    # p2, q2 和 p1 共线且 p1 在线段 p2q2 上
    if o3 == 0 and on_segment(p2, p1, q2):
        return True

    # p2, q2 和 q1 共线且 q1 在线段 p2q2 上
    if o4 == 0 and on_segment(p2, q1, q2):
        return True

    return False

def intersection_point(p1, q1, p2, q2):
    """计算两条线段的交点（如果相交）。"""
    def line(p1, p2):
        A = p1[1] - p2[1]
        B = p2[0] - p1[0]
        C = p1[0] * p2[1] - p2[0] * p1[1]
        return A, B, -C

    L1 = line(p1, q1)
    L2 = line(p2, q2)

    D = L1[0] * L2[1] - L1[1] * L2[0]
    Dx = L1[2] * L2[1] - L1[1] * L2[2]
    Dy = L1[0] * L2[2] - L1[2] * L2[0]

    if D != 0:
        x = Dx / D
        y = Dy / D
        return x, y
    else:
        return None


for F in range(800, 2550, 50):
    FList.append(F)
    

for f in range(len(FList)):
    F = FList[f]

    GList = []
    HList = []
    IList = []
    JList = []
    KList = []
    LList = []
    MList = []
    NList = []
    OList = []
    PList = []
    QList = []
    RList = []
    SList = []
    TList = []
    UList = []
    VList = []
    WList = []
    XList = []
    YList = []
    ZList = []
    AAList = []

    for i in range(len(AList)):
        GList.append(F * F * EList[i])
        HList.append(GList[i] * BList[i])
        IList.append(F * AList[i])

        JList.append(IList[i]/K2)
        KList.append(HList[i]*K2)

        LList.append(IList[i]/M2)
        MList.append(HList[i]*M2)

        NList.append(IList[i]/O2)
        OList.append(HList[i]*O2)

        PList.append(IList[i]/Q2)
        QList.append(HList[i]*Q2)

        RList.append(IList[i]/S2)
        SList.append(HList[i]*S2)

        TList.append(IList[i]/U2)
        UList.append(HList[i]*U2)

        VList.append(IList[i]/W2)
        WList.append(HList[i]*W2)

        XList.append(IList[i]/Y2)
        YList.append(HList[i]*Y2)

        ZList.append(IList[i]/AA2)
        AAList.append(HList[i]*AA2)

    print(f">>F = {F}")

    # [J,K],[L,M],[N,O],[P,Q],[R,S],[T,U] 相邻的两组之间求交点
    missions = [
        {'name': '[J列, K列]', 'data': [JList, KList]},
        {'name': '[L列, M列]', 'data': [LList, MList]},
        {'name': '[N列, O列]', 'data': [NList, OList]},
        {'name': '[P列, Q列]', 'data': [PList, QList]},
        {'name': '[R列, S列]', 'data': [RList, SList]},
        {'name': '[T列, U列]', 'data': [TList, UList]}
    ]

    # 双重遍历 missions 中的每一对线段
    for i in range(len(missions) - 1):
        groupA = missions[i]
        groupB = missions[i + 1]

        for j in range(len(groupA['data'][0]) - 1):
            p1 = (groupA['data'][0][j], groupA['data'][1][j])
            q1 = (groupA['data'][0][j + 1], groupA['data'][1][j + 1])
            
            for k in range(len(groupB['data'][0]) - 1):
                p2 = (groupB['data'][0][k], groupB['data'][1][k])
                q2 = (groupB['data'][0][k + 1], groupB['data'][1][k + 1])
                
                if segments_intersect(p1, q1, p2, q2):
                    intersect = intersection_point(p1, q1, p2, q2)
                    if intersect:
                        print(f"{groupA['name']} 与 {groupB['name']} 的交点在 {intersect}")
        
    # [V,W],[X,Y],[Z,AA] 相邻的两组之间求交点

    # 假设 missions2 已经定义好
    missions2 = [
        {'name': '[V列, W列]', 'data': [VList, WList]},
        {'name': '[X列, Y列]', 'data': [XList, YList]},
        {'name': '[Z列, AA列]', 'data': [ZList, AAList]}
    ]

    # 双重遍历 missions2 中的每一对相邻线段
    for i in range(len(missions2) - 1):
        groupA = missions2[i]
        groupB = missions2[i + 1]

        for j in range(len(groupA['data'][0]) - 1):
            p1 = (groupA['data'][0][j], groupA['data'][1][j])
            q1 = (groupA['data'][0][j + 1], groupA['data'][1][j + 1])
            
            for k in range(len(groupB['data'][0]) - 1):
                p2 = (groupB['data'][0][k], groupB['data'][1][k])
                q2 = (groupB['data'][0][k + 1], groupB['data'][1][k + 1])
                
                if segments_intersect(p1, q1, p2, q2):
                    intersect = intersection_point(p1, q1, p2, q2)
                    if intersect:
                        print(f"{groupA['name']} 与 {groupB['name']} 的交点在 {intersect}")


    print('---------------------------------')

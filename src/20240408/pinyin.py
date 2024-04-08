import sys
from pypinyin import pinyin, lazy_pinyin, Style

if __name__ == '__main__':
    args = sys.argv

    if len(args) != 2:
        print('请输入中文')
        sys.exit(1)

    zh = args[1]

    # 将文件名转为拼音
    py = lazy_pinyin(zh, style=Style.NORMAL)
    py = ''.join(py)
    
    print(py)




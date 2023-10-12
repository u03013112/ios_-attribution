# 读取两个文件
# inputFile1, inputFile2
# 其中inputFile1中包含inputFile2的全部内容
# 将inputFile1中的inputFile2的内容删除
# 剩余部分写入outputFile
# 按行处理
def diff(inputFile1, inputFile2, outputFile):
    with open(inputFile1, 'r') as f1:
        with open(inputFile2, 'r') as f2:
            with open(outputFile, 'w') as f3:
                lines1 = f1.readlines()
                lines2 = f2.readlines()
                for line1 in lines1:
                    if line1 not in lines2:
                        f3.write(line1)


diff('/Users/u03013112/Downloads/index.nsfw.m3u','/Users/u03013112/Downloads/index.m3u','/Users/u03013112/Downloads/nsfw.m3u')
    
from collections import Counter

# 读取文件内容
with open("1.txt", "r", encoding="utf-8") as file:
    text = file.read()

# 按空格分割词语（自动过滤空字符串）
words = text.split()

# 统计词频并降序排序
word_counts = Counter(words)
sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)

N = 50

# 输出前20个高频词
print("【词频统计结果（前N名）】")
for rank, (word, count) in enumerate(sorted_words[:N], 1):
    print(f"{rank}. {word}: {count}次")
import csv
from collections import Counter

# 定义IP名称映射
ip_mapping = {
    'One Piece': '海贼王',
    'Naruto': '火影忍者',
    'Bleach': '死神',
    'Pokémon': '宝可梦',
    'Solo Leveling': '我独自升级',
    'Fairy Tail': '妖精的尾巴',
    'Sword Art Online': '刀剑神域',
    'World of Warcraft': '魔兽世界',
    'Berserk': '剑风传奇',
    'My Hero Academia': '我的英雄学院',
    'Overlord': '不死者之王',
    'Fullmetal Alchemist': '钢之炼金术师',
    'Gundam': '高达',
    'Black Clover': '黑色四叶草',
    'Attack on Titan': '进击的巨人',
    'Death Note': '死亡笔记',
    'Dr. Stone': '石纪元',
    'Frieren': '葬送的芙莉莲',
    'Zelda': '塞尔达传说',
    'Diablo': '暗黑破坏神',
    'Lord of the Rings': '指环王',
    'Saint Seiya': '圣斗士星矢',
    'Marvel': '漫威',
    'Star Wars': '星球大战',
    'Dark Souls': '黑暗之魂',
    'One Punch Man': '一拳超人'
}

def read_and_count_ips(csv_file):
    ip_counter = Counter()
    
    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            for item in row:
                # 分割包含逗号的条目
                ips = [ip.strip() for ip in item.split(',')]
                # 统一IP名称
                unified_ips = [ip_mapping.get(ip, ip) for ip in ips]
                ip_counter.update(unified_ips)
    
    return ip_counter

def write_counts_to_csv(ip_counter, output_file):
    with open(output_file, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['IP', 'count'])
        for ip, count in ip_counter.most_common():
            writer.writerow([ip, count])

def main():
    input_csv_file = 'mohan1.csv'  # 请将此处替换为你的输入CSV文件路径
    output_csv_file = '/src/data/mohan.csv'  # 请将此处替换为你的输出CSV文件路径
    
    ip_counter = read_and_count_ips(input_csv_file)
    
    # 将统计结果写入新的CSV文件
    write_counts_to_csv(ip_counter, output_csv_file)

if __name__ == '__main__':
    main()

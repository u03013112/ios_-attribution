import re
import os
import shutil

def split_file(file_path):
    # 获取文件名（不带扩展名）
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    # 创建文件夹
    output_dir = file_name
    if os.path.exists(output_dir):
        # 清空文件夹
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    
    current_file = None
    chapter_number = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            # 检查是否是章节标题
            chapter_match = re.match(r'(第\d+[章节][^\n]*)', line)
            if chapter_match:
                # 关闭当前文件（如果有）
                if current_file:
                    current_file.close()
                
                # 提取章节号
                chapter_number_match = re.search(r'\d+', chapter_match.group(1))
                if chapter_number_match:
                    chapter_number = chapter_number_match.group()
                
                # 创建新文件
                current_file = open(os.path.join(output_dir, f'{file_name}_第{chapter_number}章.txt'), 'w', encoding='utf-8')
                # 写入章节标题行
                current_file.write(line)
            elif current_file:
                # 写入章节内容
                current_file.write(line)
        
        # 关闭最后一个文件
        if current_file:
            current_file.close()

file_path = '我不是戏神.txt'
split_file(file_path)

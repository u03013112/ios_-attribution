import cv2

# 读取图像
image = cv2.imread('deadAhead01.png')

# 定义每个块的大小和间隔
block_size = 72
spacing = 1

# 获取图像的高度和宽度
height, width, _ = image.shape

# 初始化块编号
block_number = 0

# 遍历图像并切割块
for y in range(0, height, block_size + spacing):
    for x in range(0, width, block_size + spacing):
        # 提取块
        block = image[y:y + block_size, x:x + block_size]
        
        # 检查块的大小是否正确
        if block.shape[0] == block_size and block.shape[1] == block_size:
            # 保存块
            filename = f"{block_number}.png"
            cv2.imwrite(filename, block)
            block_number += 1

print("图像切割并保存完成。")

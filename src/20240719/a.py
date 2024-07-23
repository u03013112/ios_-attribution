import os
from PIL import Image

def catPicFromDir(dirPath, saveFilename, w=5, h=0):
    # 获取文件夹中的所有图片文件
    image_files = [f for f in os.listdir(dirPath) if f.endswith(('png', 'jpg', 'jpeg'))]
    # 按字母顺序排序文件名
    image_files.sort()

    # 打开所有图片并存储在列表中
    images = [Image.open(os.path.join(dirPath, img)) for img in image_files]

    # 假设所有图片的尺寸相同，获取单张图片的宽和高
    if images:
        img_width, img_height = images[0].size
    else:
        print("No images found in the directory.")
        return

    # 计算拼接图片的宽和高
    num_images = len(images)
    num_rows = (num_images + w - 1) // w  # 每行w张图片
    if h > 0:
        num_rows = min(num_rows, h)  # 限制行数

    total_width = img_width * w
    total_height = img_height * num_rows

    # 创建一个新的空白图片用于拼接
    new_image = Image.new('RGB', (total_width, total_height), (255, 255, 255))

    # 将图片逐一粘贴到新图片上
    for index, img in enumerate(images):
        if h > 0 and index >= w * h:
            break  # 超过最大行数限制
        x_offset = (index % w) * img_width
        y_offset = (index // w) * img_height
        new_image.paste(img, (x_offset, y_offset))

    # 保存拼接后的图片
    new_image.save(saveFilename)
    print(f"Image saved as {saveFilename}")

# 示例调用
catPicFromDir('/Users/u03013112/Downloads/characterWalkingAndRunning_betterCrops/512by512crops/girlRunningBones/L', 'output_image.png',w=3,h=6)

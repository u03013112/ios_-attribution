import os
import subprocess

ffmpeg = '/opt/homebrew/bin/ffmpeg'
ffprobe = '/opt/homebrew/bin/ffprobe'
inputDir = '/Users/u03013112/Downloads/input20250311'
outputDir = '/Users/u03013112/Downloads/output20250311'

# 比特率建议
bitrate_map = {
    (1920, 1080): 4000,
    (1080, 1080): 3000,
    (1080, 1350): 3500,
    (1080, 1920): 4000,
    (720, 1280): 1800,
}

def get_resolution(file_path):
    """使用 ffprobe 获取视频分辨率"""
    command = [
        ffprobe,
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height',
        '-of', 'csv=s=x:p=0',
        file_path
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    resolution = result.stdout.decode().strip()
    if resolution:
        width, height = map(int, resolution.split('x'))
        return width, height
    return None

def get_resolution2(file_name):
    """从文件名中提取分辨率"""
    parts = file_name.split('_')
    if len(parts) >= 3:
        resolution_str = parts[-3]
        try:
            width, height = map(int, resolution_str.split('X'))
            return width, height
        except ValueError:
            pass
    return None

def convert_video(input_file, output_file, bitrate):
    """使用 ffmpeg 转换视频比特率"""
    command = [
        ffmpeg,
        '-i', input_file,
        '-b:v', f'{bitrate}k',
        '-c:a', 'copy',
        output_file
    ]
    subprocess.run(command)

def convert_video2(input_file, output_file, bitrate):
    """将分辨率转换为 720x1280 并调整比特率"""
    command = [
        ffmpeg,
        '-i', input_file,
        '-vf', 'scale=720:-1',
        '-b:v', f'{bitrate}k',
        '-c:a', 'copy',
        output_file
    ]
    subprocess.run(command)

def main():
    # 确保输出目录存在
    os.makedirs(outputDir, exist_ok=True)

    # 获取 inputDir 目录下的所有 mp4 文件
    for file_name in os.listdir(inputDir):
        if file_name.endswith('.mp4'):
            input_file = os.path.join(inputDir, file_name)
            resolution = get_resolution2(file_name)

            if resolution in bitrate_map:
                bitrate = bitrate_map[resolution]
                base_name = os.path.splitext(file_name)[0]
                output_file = os.path.join(outputDir, f'{base_name}_{bitrate}kbps.mp4')
                convert_video(input_file, output_file, bitrate)
                print(f'Converted {file_name} to {bitrate} kbps')

                # 如果分辨率是 1080x1920，额外调用 convert_video2
                if resolution == (1080, 1920):
                    # base_name 进行分割，将分辨率部分替换为 720x1280
                    base_name_parts = base_name.split('_')
                    base_name_parts[-3] = '720x1280'
                    base_name_new = '_'.join(base_name_parts)
                    output_file_720 = os.path.join(outputDir, f'{base_name_new}_{bitrate_map[(720, 1280)]}kbps.mp4')
                    convert_video2(input_file, output_file_720, bitrate_map[(720, 1280)])
                    print(f'Converted {file_name} to 720x1280 at {bitrate_map[(720, 1280)]} kbps')

if __name__ == '__main__':
    main()

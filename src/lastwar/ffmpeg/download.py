import pandas as pd

import os
import subprocess


def downloadFromCsv(csvfilename, outputDir='videos'):
    df = pd.read_csv(csvfilename)
    
    df = df[['Title','Video link']]
    
    # 下载视频并按照Title重命名
    for i in range(len(df)):
        title = df.iloc[i]['Title']
        url = df.iloc[i]['Video link']
        filename = f'{title}'
        if not os.path.exists(filename):
            subprocess.run(['wget','-O',outputDir+filename,url])
            print(f'download {filename}')
        else:
            print(f'file {filename} exists')

# 获得视频的分辨率，比特率和文件大小
def getResolutionBitrateSize(filename):
    # 获取分辨率和比特率
    command = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height,bit_rate',
        '-of', 'csv=s=x:p=0',
        filename
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE)
    result = result.stdout.decode('utf-8').strip()
    
    # 获取文件大小
    file_size = os.path.getsize(filename)
    
    # 解析结果
    width, height, bit_rate = result.split('x')
    resolution = f"{width}x{height}"
    
    return resolution, bit_rate, file_size

def main():
    # downloadFromCsv('/Users/u03013112/Documents/git/ios_-attribution/src/lastwar/ffmpeg/native_video_creatives_20250314_1438.csv', '/Users/u03013112/Downloads/20250314/native_videos/')
    # downloadFromCsv('/Users/u03013112/Documents/git/ios_-attribution/src/lastwar/ffmpeg/video_creatives_20250314_1438.csv', '/Users/u03013112/Downloads/20250314/videos/')

    # 遍历当前文件夹中的所有 .mp4 文件
    video_data = []

    # dirs = ['/Users/u03013112/Downloads/20250314/native_videos/', '/Users/u03013112/Downloads/20250314/videos/']

    dirs = ['/Users/u03013112/Downloads/20250314/native_videos/']

    for dir in dirs:
        for filename in os.listdir(dir):
            if filename.endswith('.mp4'):
                resolution, bit_rate, file_size = getResolutionBitrateSize(dir+filename)
                video_data.append({
                    'Filename': filename,
                    'Resolution': resolution,
                    'Bitrate': bit_rate,
                    'File Size': file_size
                })

    # 保存到 DataFrame
    df = pd.DataFrame(video_data)

    # 保存到 CSV
    df.to_csv('video_info.csv', index=False)
    print("Video information saved to video_info.csv")


if __name__ == '__main__':
    main()
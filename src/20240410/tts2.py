import os
import subprocess
import pandas as pd

def tts(chn, duration, speaker, outfileName):
    # 初始化语速
    r = 160
    temp_file = outfileName
    print('内容:', chn)
    print('需求时长:', duration)

    while True:
        # 调用say命令生成音频
        cmd = ["say", "-v", speaker, chn, "-r", str(r), "-o", temp_file]
        subprocess.run(cmd)

        # 获取音频长度
        cmd = ["soxi", "-D", temp_file]
        original_duration = float(subprocess.check_output(cmd).strip())
        # print(f'rate:{r},original_duration:{original_duration}')

        # 判断音频长度是否满足需求
        if original_duration <= duration:
            break
        else:
            r += 10
    print('最终语速:', r)
    print('实际时长:', original_duration)
    return original_duration

def generate_silence(duration, output_file):
    print(f"生成{duration}秒的静音音频{output_file}")
    cmd = ["sox", "-n", "-r", "22050", "-c", "1", output_file, "trim", "0.0", str(duration)]
    subprocess.run(cmd)

def ttsFromCsv(csvFileName, audioDirPath, speaker):
    # 读取CSV文件
    df = pd.read_csv(csvFileName)

    # 由于配音最后总是有一点点空白，所以将结束时间延长一点
    df['结束时间'] = df['结束时间'] + 0.2

    # 生成音频文件
    audio_files = []
    for index, row in df.iterrows():
        chn = row['chn']
        start_time = row['开始时间']
        end_time = row['结束时间']
        duration = end_time - start_time

        outfileName = os.path.join(audioDirPath, f"{index}.aiff")
        actual_duration = tts(chn, duration, speaker, outfileName)

        audio_files.append(outfileName)

        # 如果不是最后一行，则添加空白音频
        if index < len(df) - 1:
            next_start_time = df.loc[index + 1, '开始时间']
            silence_duration = next_start_time - end_time + (duration - actual_duration)
            silence_file = os.path.join(audioDirPath, f"silence_{index}.aiff")
            generate_silence(silence_duration, silence_file)
            audio_files.append(silence_file)

    # 合并音频文件
    ret = os.path.join(audioDirPath, "ret.aiff")
    cmd = ["sox"] + audio_files + [ret]
    subprocess.run(cmd)

# 示例用法
ttsFromCsv("jap1.csv", "audios", "Tingting")

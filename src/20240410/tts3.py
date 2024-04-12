import os
import subprocess
import pandas as pd
from msTTS import msTTS

def tts(chn, duration, speaker, outfileName):
    temp_file = outfileName
    print('内容:', chn)
    print('需求时长:', duration)

    # 使用speed=1生成音频，计算实际时长
    msTTS(chn, temp_file, speechSynthesisVoiceName=speaker, speed=1.0)
    cmd = ["soxi", "-D", temp_file]
    original_duration = float(subprocess.check_output(cmd).strip())
    print('原始时长:', original_duration)

    # 对于比较短可以容忍
    if original_duration < duration and duration - original_duration < 0.5:
        print('时长短不足0.5秒,无需调整')
        return original_duration
    # 对于比较长，比较严格
    if original_duration > duration and original_duration - duration < 0.2:
        print('时长长不足0.2秒,无需调整')
        return original_duration

    # 计算所需速度
    speed = original_duration / duration
    print('最终语速:', speed)

    # 使用计算出的速度生成音频
    msTTS(chn, temp_file, speechSynthesisVoiceName=speaker, speed=speed)

    # 再次获取音频长度以进行调试
    cmd = ["soxi", "-D", temp_file]
    final_duration = float(subprocess.check_output(cmd).strip())
    print('实际时长:', final_duration)

    return final_duration


def generate_silence(duration, output_file):
    print(f"生成{duration}秒的静音音频{output_file}")
    cmd = ["sox", "-n", "-r", "16000", "-c", "1", output_file, "trim", "0.0", str(duration)]
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

        needTTS = row['需要重新配音']
        if needTTS == 'Y' or not os.path.exists(outfileName):
            actual_duration = tts(chn, duration, speaker, outfileName)
        else:
            cmd = ["soxi", "-D", outfileName]
            actual_duration = float(subprocess.check_output(cmd).strip())
            print('读取旧有音频，实际时长:', actual_duration)

        audio_files.append(outfileName)

        # 如果不是最后一行，则添加空白音频
        if index < len(df) - 1:
            next_start_time = df.loc[index + 1, '开始时间']
            silence_duration = next_start_time - end_time + (duration - actual_duration)
            if silence_duration < 0:
                print(f"第{index}行音频过长，无法添加静音")
                continue
            silence_file = os.path.join(audioDirPath, f"silence_{index}.aiff")
            generate_silence(silence_duration, silence_file)
            audio_files.append(silence_file)

    # 合并音频文件
    ret = os.path.join(audioDirPath, "ret.aiff")
    cmd = ["sox"] + audio_files + [ret]
    subprocess.run(cmd)

# 示例用法
ttsFromCsv("jap1.csv", "audios", "zh-CN-XiaoxiaoNeural")

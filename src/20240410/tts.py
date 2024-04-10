# 配音

# 读取csv文件，其中chn列是中文内容，用这个内容生成音频文件
# 开始时间,结束时间 为这句话的开始时间与结束时间
# 生成的语音长度要通过调整语速来匹配这个时间
# 生成语音部分采用shell调用 macos的 say 命令
# cmd = ["say", "-v", speaker, text, "-o", output_file, "--data-format=LEI16"]
# 临时生成的每一句，保存在audioDirPath中，用在csv中的行号作为文件名
# 最后再将这个音频文件合并到一个音频文件中，记得填充空白音频，使得整个音频长度与csv文件中的时间一致

import os
import subprocess
import pandas as pd

def tts(chn, duration, speaker, outfileName):
    # 调用say命令生成音频
    temp_file = "temp.aiff"
    r = "180"
    cmd = ["say", "-v", speaker, chn, "-r", r,"-o", temp_file]
    subprocess.run(cmd)

    # 获取原始音频长度
    cmd = ["soxi", "-D", temp_file]
    original_duration = float(subprocess.check_output(cmd).strip())

    # 计算拉伸因子
    stretch_factor = duration / original_duration

    # 使用sox命令校准音频长度
    cmd = ["sox", temp_file, outfileName, "stretch", str(stretch_factor)]
    subprocess.run(cmd)

    # 删除临时文件
    os.remove(temp_file)

def generate_silence(duration, output_file):
    cmd = ["sox", "-n", "-r", "22050", "-c", "1", output_file, "trim", "0.0", str(duration)]
    subprocess.run(cmd)

def ttsFromCsv(csvFileName, audioDirPath, speaker):
    # 读取CSV文件
    df = pd.read_csv(csvFileName)

    # 生成音频文件
    audio_files = []
    for index, row in df.iterrows():
        chn = row['chn']
        start_time = row['开始时间']
        end_time = row['结束时间']
        duration = end_time - start_time

        outfileName = os.path.join(audioDirPath, f"{index}.aiff")
        tts(chn, duration, speaker, outfileName)

        audio_files.append(outfileName)

        # 如果不是最后一行，则添加空白音频
        if index < len(df) - 1:
            next_start_time = df.loc[index + 1, '开始时间']
            silence_duration = next_start_time - end_time
            silence_file = os.path.join(audioDirPath, f"silence_{index}.aiff")
            generate_silence(silence_duration, silence_file)
            audio_files.append(silence_file)

    # 合并音频文件
    ret = os.path.join(audioDirPath, "ret.aiff")
    cmd = ["sox"] + audio_files + [ret]
    subprocess.run(cmd)

    # # 删除临时音频文件
    # for audio_file in audio_files:
    #     os.remove(audio_file)

# 示例用法
ttsFromCsv("jap1.csv", "audios", "Tingting")
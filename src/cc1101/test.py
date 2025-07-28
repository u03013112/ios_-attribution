import numpy as np
import matplotlib.pyplot as plt

# 读取.cu8文件
def read_cu8(filename):
    data = np.fromfile(filename, dtype=np.uint8)
    # 转换为复数 (I+jQ)
    iq = data[0::2] + 1j * data[1::2]
    # 转换为浮点数 (-1 to 1)
    iq = (iq - 127.5) / 127.5
    return iq

# 分析频谱
iq_data = read_cu8('/Users/u03013112/Downloads/1/recording250000Hz.cu8')
fft_data = np.fft.fft(iq_data)  # 取前1024个样本做FFT
plt.plot(np.abs(fft_data))
plt.show()
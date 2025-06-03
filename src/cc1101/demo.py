import spidev

# 初始化 SPI
spi = spidev.SpiDev()
spi.open(0, 0)  # SPI0, CS0
spi.max_speed_hz = 500000

# 读取寄存器的示例函数
def read_register(address):
    response = spi.xfer2([address | 0x80, 0x00])
    return response[1]

# 写入寄存器的示例函数
def write_register(address, value):
    spi.xfer2([address, value])

# 读取 FIFO 数据
def read_fifo():
    # 读取 FIFO 的数据长度
    length = read_register(0x7F)  # FIFO寄存器地址
    if length > 0:
        # 读取 FIFO 中的所有数据
        data = spi.xfer2([0x7F | 0x80] + [0x00] * length)
        return data[1:]
    return []

# 设置频率（具体寄存器值需要根据 CC1101 数据手册设定）
def set_frequency():
    write_register(0x0D, 0x0C)  # FREQ2
    write_register(0x0E, 0x1A)  # FREQ1
    write_register(0x0F, 0x92)  # FREQ0

# 初始化模块
set_frequency()

# 主循环监听信号
while True:
    # 读取信号强度
    signal_strength = read_register(0x34)  # RSSI 寄存器地址
    print(f"Signal Strength: {signal_strength}")

    # 读取数据
    data = read_fifo()
    if data:
        print(f"Received Data: {data}")

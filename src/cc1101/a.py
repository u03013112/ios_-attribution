import sys
import cc1101
import logging
import time

def getTxData(data, bit_rate=10000,isdebug = False):
    BIT_RATE = bit_rate  # 比特率为10kbps
    BIT_DURATION_US = 1_000_000 / BIT_RATE  # 每个比特的持续时间（微秒）

    HIGH_0_BITS = int(400 / BIT_DURATION_US)  # 0.4ms 高电平
    LOW_0_BITS = int(1100 / BIT_DURATION_US)  # 1.1ms 低电平
    HIGH_1_BITS = int(1100 / BIT_DURATION_US) # 1.1ms 高电平
    LOW_1_BITS = int(400 / BIT_DURATION_US)   # 0.4ms 低电平

    if isdebug:
        print(f"BIT_RATE: {BIT_RATE} bps")
        print(f"BIT_DURATION_US: {BIT_DURATION_US} us")
        print(f"HIGH_0_BITS: {HIGH_0_BITS}, LOW_0_BITS: {LOW_0_BITS}")
        print(f"HIGH_1_BITS: {HIGH_1_BITS}, LOW_1_BITS: {LOW_1_BITS}")

    bit_stream = []

    for byte in data:
        # byte ^= 0xFF  # 数据取反
        for bit_pos in range(7, -1, -1):  # 从最高位开始
            bit = (byte >> bit_pos) & 0x01
            if bit == 0:
                bit_stream += [1] * HIGH_0_BITS + [0] * LOW_0_BITS
            else:
                bit_stream += [1] * HIGH_1_BITS + [0] * LOW_1_BITS

    # 不知道为啥，最后需要多加一个0
    bit_stream += [1] * HIGH_0_BITS + [0] * LOW_0_BITS
    # 为了凑齐8的倍数，最后再补充一些0
    while len(bit_stream) % 8 != 0:
        bit_stream.append(0)

    # 为了稳定，最后持多发送8个低电平
    bit_stream += [0] * 8


    # # 对bit_stream取反
    # bit_stream = [1 - bit for bit in bit_stream]

    if isdebug:
        print(f"Bit stream length: {len(bit_stream)} bits")
        print(f"Bit stream (first 64 bits): {bit_stream[:64]}")
        print(f"Bit stream (last 64 bits): {bit_stream[-64:]}")

    # 将bit_stream转为bytearray
    tx_bytes = bytearray()
    for i in range(0, len(bit_stream), 8):
        byte = 0
        bits = bit_stream[i:i+8]
        for bit in bits:
            byte = (byte << 1) | bit
        if len(bits) < 8:
            byte <<= (8 - len(bits))  # 补齐最后不足8位的情况
        tx_bytes.append(byte)

    return tx_bytes

# 新增的发送函数
def transmit_data(tx_bytes, frequency=433.92e6, symbol_rate=10000, power=(0, 0xC0)):
    logging.basicConfig(level=logging.INFO)
    with cc1101.CC1101() as transceiver:
        transceiver.set_base_frequency_hertz(frequency)
        transceiver.set_symbol_rate_baud(symbol_rate)
        transceiver.set_sync_mode(cc1101.SyncMode.NO_PREAMBLE_AND_SYNC_WORD)
        transceiver.set_packet_length_mode(cc1101.PacketLengthMode.FIXED)
        transceiver.set_packet_length_bytes(len(tx_bytes))
        transceiver.disable_checksum()
        transceiver.set_output_power(power)  # OOK modulation: (off, on)
        logging.info(f"Transceiver configuration:\n{transceiver}")
        transceiver.transmit(tx_bytes)
        logging.info("Data transmitted successfully.")

def test():
    hex_string = '4bb108'
    data = bytes.fromhex(hex_string)
    tx_data = getTxData(data, isdebug=False)

    transmit_data(tx_data, frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))
    time.sleep(0.02)
    transmit_data(tx_data, frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))
    time.sleep(0.02)
    transmit_data(tx_data, frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))

def hack():
    # 先遍历一小段，
    # tx data 从 4000开始 到4fff结束
    # 后面固定追加 0x01，发送

    for i in range(0x4000, 0x5000):
        hex_string = f"{i:04x}" + "01"  # 后面固定追加'01'
        if hex_string == '4bb10801':
            continue
        data = bytes.fromhex(hex_string)
        tx_data = getTxData(data, isdebug=False)
        print(f"Sending: {hex_string}")

        transmit_data(tx_data, frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))
        time.sleep(0.01)
        transmit_data(tx_data, frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))




if __name__ == "__main__":
    # hex_string = '4bb108'
    # # 检查输入合法性
    # if len(hex_string) % 2 != 0:
    #     sys.stderr.write("Error: hex_string length must be even.\n")
    #     sys.exit(1)
    # try:
    #     data = bytes.fromhex(hex_string)
    # except ValueError:
    #     sys.stderr.write("Error: Invalid hex string.\n")
    #     sys.exit(1)
    # # 获取待发送的数据
    # tx_data = getTxData(data, isdebug=False)
    # # 调用封装好的发送函数进行发送
    # transmit_data(tx_data, frequency=433.92e6, symbol_rate=10122, power=(0, 0xC0))


    test()
    # hack()
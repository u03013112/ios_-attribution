import cc1101
import datetime

with cc1101.CC1101() as transceiver:
    transceiver.set_base_frequency_hertz(315.00e6)  # 或433.92e6
    transceiver.set_modulation_format(cc1101.ModulationFormat.ASK_OOK)  # ✅ ASK/OOK
    
    # 波特率尝试常见值（遥控器典型范围1.2k-10kbps）
    transceiver.set_symbol_rate_baud(4800)  # ✅ 尝试4.8kbps
    # 或测试多组值： [1200, 2400, 4800, 9600]

    # 数据包模式设为原始流
    transceiver.set_packet_length_mode(cc1101.PacketLengthMode.INFINITE)  # ✅
    
    # # 关键寄存器配置
    # transceiver.write_config(0x11, 0x30)   # MDMCFG2: ASK/OOK
    # transceiver.write_config(0x08, 0x00)   # PKTCTRL0: 禁用CRC
    # transceiver.write_config(0x03, 0x07)   # FIFOTHR: 1字节触发
    # transceiver.write_config(0x17, 0xB0)   # AGCCTRL2: 高灵敏度
    
    # 好像没有write_config 方法
    # 可以参考 https://github.com/fphammerle/python-cc1101/blob/1623a48e96496f84fa26b2b29b2def955a5ac1d0/cc1101/__init__.py#L132
    # self._write_burst(
    #     start_register=ConfigurationRegisterAddress.MDMCFG4, values=[mdmcfg4]
    # )

    print(transceiver)  # 打印配置
    
    # 延长接收超时并改用标准API
    packet = transceiver.receive(timeout=datetime.timedelta(seconds=30))
    
    if not packet:
        print("No packet received within 30 seconds.")
    else:
        print(f"Received RAW data: {packet}")
        # 进一步解析原始数据（通常为PWM/PPM编码）
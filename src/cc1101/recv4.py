import cc1101
import datetime

# python3 -m venv ~/cc1101-env
# source ~/cc1101-env/bin/activate
# pip install --upgrade cc1101


with cc1101.CC1101() as transceiver:
    transceiver.set_base_frequency_hertz(433.92e6)
    transceiver._set_modulation_format(cc1101.ModulationFormat.ASK_OOK)  # ✅ ASK/OOK
    
    transceiver.set_symbol_rate_baud(1200)  # ✅ 尝试4.8kbps

    # 数据包模式设为原始流
    transceiver.set_packet_length_mode(cc1101.PacketLengthMode.FIXED)
    transceiver.set_packet_length_bytes(8)

    transceiver.set_sync_mode(cc1101.SyncMode.NO_PREAMBLE_AND_SYNC_WORD)

    print(transceiver)  # 打印配置
    while True:
        packet = transceiver._wait_for_packet(
            timeout=datetime.timedelta(seconds=10),
            gdo0_gpio_line_name = b"GPIO25"
        )
        
        if not packet:
            print("No packet received within 30 seconds.")
        else:
            if packet.rssi_dbm > -80:
                print(f"Received RAW data: {packet}")
                
                print("✅ 成功接收到数据包:")
                print("  数据内容 (hex)：", packet.payload.hex())
                print("  RSSI信号强度 (dBm)：", packet.rssi_dbm)
                print("  校验和有效性：", packet.checksum_valid)
                print("  链路质量指示器 (LQI)：", packet.link_quality_indicator)
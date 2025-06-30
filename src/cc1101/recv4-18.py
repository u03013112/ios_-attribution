import cc1101
import datetime

# python3 -m venv ~/cc1101-env
# source ~/cc1101-env/bin/activate
# pip install --upgrade cc1101


with cc1101.CC1101() as transceiver:
    transceiver.set_base_frequency_hertz(433.92e6)
    transceiver._set_modulation_format(cc1101.ModulationFormat.ASK_OOK)  # ✅ ASK/OOK
    
    transceiver.set_symbol_rate_baud(1200)

    # 配置CC1101为异步原始模式
    # 正确配置异步原始模式（基于寄存器说明表）
    # transceiver._write_register(0x08, 0b00111100)  
    transceiver._write_burst(
        start_register=0x08, values=[0b00111100]
    )
    # 二进制分解：
    # bit6=0(禁用白化)
    # bit5:4=11(异步模式)
    # bit2=0(禁用CRC)
    # bit1:0=10(无限包长)
    transceiver._write_burst(
        start_register=0x00, values=[0x0D]
    )

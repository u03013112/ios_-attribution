import spidev
import time

class CC1101Transparent:
    def __init__(self, spi_bus=0, spi_device=0):
        self.spi = spidev.SpiDev()
        self.spi.open(spi_bus, spi_device)
        self.spi.max_speed_hz = 500000
        self.spi.mode = 0b00
        self.reset()
        self.setup_transparent_mode()

    def reset(self):
        self.spi.xfer2([0x30])  # SRES command
        time.sleep(0.1)

    def write_reg(self, addr, value):
        self.spi.xfer2([addr, value])

    def strobe(self, command):
        self.spi.xfer2([command])

    def setup_transparent_mode(self):
        # 设置频率为433.92MHz
        self.write_reg(0x0D, 0x10)  # FREQ2
        self.write_reg(0x0E, 0xB0)  # FREQ1
        self.write_reg(0x0F, 0x71)  # FREQ0

        # 设置ASK/OOK调制，无同步字检测，异步透明模式
        self.write_reg(0x12, 0x30)  # MDMCFG2: ASK/OOK, no sync word detection
        self.write_reg(0x08, 0x32)  # PKTCTRL0: Asynchronous serial mode
        self.write_reg(0x00, 0x0D)  # IOCFG2: Asynchronous Serial Data Output

        # ⚠️关键：设置符号速率为1200 baud
        # Symbol rate = (256 + DRATE_M) × 2^(DRATE_E) × (f_XOSC / 2^28)
        # f_XOSC = 26 MHz
        # 1200 baud时，推荐寄存器配置为：
        self.write_reg(0x10, 0x27)  # MDMCFG4 (DRATE_E=7, CHANBW_E=2, CHANBW_M=0)
        self.write_reg(0x11, 0x56)  # MDMCFG3 (DRATE_M=86)

        # 设置适合ASK/OOK的滤波器带宽
        self.write_reg(0x10, 0x67)  # MDMCFG4: DRATE_E=6, CHANBW_E=2, CHANBW_M=0 (带宽约200kHz)
        # 如果上面你已经设置MDMCFG4寄存器，这一步可以省略（取决于你的带宽需求）

        # 进入接收模式
        self.strobe(0x34)  # SRX command

    def close(self):
        self.spi.close()

# 初始化CC1101
cc1101 = CC1101Transparent()
print("CC1101 已进入透明模式，GDO2输出原始数据流。")

# 保持运行，直到用户中断
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    cc1101.close()
    print("退出CC1101配置脚本。")

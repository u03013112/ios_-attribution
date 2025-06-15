import spidev
import RPi.GPIO as GPIO
import time

# CC1101寄存器地址（部分）
CC1101_IOCFG0 = 0x02
CC1101_PKTCTRL0 = 0x08
CC1101_FSCTRL1 = 0x0B
CC1101_FREQ2 = 0x0D
CC1101_FREQ1 = 0x0E
CC1101_FREQ0 = 0x0F
CC1101_MDMCFG4 = 0x10
CC1101_MDMCFG3 = 0x11
CC1101_MDMCFG2 = 0x12
CC1101_MDMCFG1 = 0x13
CC1101_MDMCFG0 = 0x14
CC1101_MCSM0 = 0x18
CC1101_FOCCFG = 0x19
CC1101_AGCCTRL2 = 0x1B
CC1101_AGCCTRL1 = 0x1C
CC1101_AGCCTRL0 = 0x1D
CC1101_FREND1 = 0x21
CC1101_FSCAL3 = 0x23
CC1101_FSCAL2 = 0x24
CC1101_FSCAL1 = 0x25
CC1101_FSCAL0 = 0x26
CC1101_TEST2 = 0x2C
CC1101_TEST1 = 0x2D
CC1101_TEST0 = 0x2E

CC1101_SRES = 0x30
CC1101_SRX = 0x34
CC1101_SIDLE = 0x36
CC1101_FIFO = 0x3F | 0x80  # FIFO read burst

GDO0_PIN = 25  # GPIO25

class CC1101:
    def __init__(self, bus=0, device=0):
        self.spi = spidev.SpiDev()
        self.spi.open(bus, device)
        self.spi.max_speed_hz = 5000000
        self.spi.mode = 0

        GPIO.setmode(GPIO.BCM)
        GPIO.setup(GDO0_PIN, GPIO.IN)

        self.reset()
        self.configure()

    def reset(self):
        self.spi.xfer([CC1101_SRES])
        time.sleep(0.1)

    def write_reg(self, addr, value):
        self.spi.xfer2([addr, value])

    def read_reg(self, addr):
        return self.spi.xfer2([addr | 0x80, 0])[1]

    def configure(self):
        # 设置433.92MHz频率
        freq = 433920000
        freq_regs = int((freq / (26e6 / 2**16)))
        self.write_reg(CC1101_FREQ2, (freq_regs >> 16) & 0xFF)
        self.write_reg(CC1101_FREQ1, (freq_regs >> 8) & 0xFF)
        self.write_reg(CC1101_FREQ0, freq_regs & 0xFF)

        # ASK/OOK modulation
        self.write_reg(CC1101_MDMCFG2, 0x30)  # ASK/OOK, no Manchester encoding, 2-FSK disabled

        # 设置波特率1200bps
        self.write_reg(CC1101_MDMCFG4, 0xC7)
        self.write_reg(CC1101_MDMCFG3, 0x93)

        # 固定长度模式，长度1字节
        self.write_reg(CC1101_PKTCTRL0, 0x00)  # 固定长度模式
        self.write_reg(0x06, 0x01)  # PKTLEN = 1字节

        # GDO0配置为数据包接收完成信号
        self.write_reg(CC1101_IOCFG0, 0x06)  # assert when sync word sent/received, de-assert on end of packet

        # 其他推荐的寄存器配置 (来自官方推荐)
        self.write_reg(CC1101_FSCTRL1, 0x06)
        self.write_reg(CC1101_MCSM0, 0x18)
        self.write_reg(CC1101_FOCCFG, 0x16)
        self.write_reg(CC1101_AGCCTRL2, 0x43)
        self.write_reg(CC1101_AGCCTRL1, 0x40)
        self.write_reg(CC1101_AGCCTRL0, 0x91)
        self.write_reg(CC1101_FREND1, 0x56)
        self.write_reg(CC1101_FSCAL3, 0xE9)
        self.write_reg(CC1101_FSCAL2, 0x2A)
        self.write_reg(CC1101_FSCAL1, 0x00)
        self.write_reg(CC1101_FSCAL0, 0x1F)
        self.write_reg(CC1101_TEST2, 0x81)
        self.write_reg(CC1101_TEST1, 0x35)
        self.write_reg(CC1101_TEST0, 0x09)

    def receive_packet(self, timeout=10):
        self.spi.xfer([CC1101_SRX])  # 进入接收模式
        start_time = time.time()
        while time.time() - start_time < timeout:
            if GPIO.input(GDO0_PIN):
                time.sleep(0.01)  # 等待数据完全进入FIFO
                length = 1  # 固定长度模式为1字节
                data = self.spi.xfer2([CC1101_FIFO] + [0]*length)[1:]
                rssi = self.read_reg(0x34)
                lqi = self.read_reg(0x33) & 0x7F
                return data, rssi, lqi
            time.sleep(0.01)
        return None, None, None

    def close(self):
        GPIO.cleanup()
        self.spi.close()

if __name__ == "__main__":
    cc1101 = CC1101()
    print("开始接收数据，请按遥控器按键...")
    try:
        while True:
            data, rssi, lqi = cc1101.receive_packet(timeout=10)
            if data:
                print(f"✅ 接收到数据: {bytes(data).hex()}")
                print(f"   RSSI: {- (rssi/2)} dBm")
                print(f"   LQI: {lqi}")
            else:
                print("⚠️ 未收到数据")
    except KeyboardInterrupt:
        print("结束程序")
    finally:
        cc1101.close()

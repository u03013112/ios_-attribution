import spidev
import time

class CC1101:
    def __init__(self, spi_bus=0, spi_device=0):
        self.spi = spidev.SpiDev()
        self.spi.open(spi_bus, spi_device)
        self.spi.max_speed_hz = 500000  # 设置SPI时钟频率为500kHz

    def write_reg(self, addr, value):
        """写入寄存器"""
        self.spi.xfer([addr & 0x3F, value])  # 地址掩码0x3F，BIT7=0表示写操作

    def read_reg(self, addr):
        """读取寄存器"""
        return self.spi.xfer([addr | 0x80, 0x00])[1]  # BIT7=1表示读操作

    def init_315mhz(self):
        """初始化CC1101为315MHz接收模式"""
        # 进入空闲模式
        self.write_reg(0x08, 0x36)  # 发送SIDLE命令（参考CC1101手册）
        time.sleep(0.1)

        # 配置频率寄存器（315MHz）
        self.write_reg(0x0D, 0x10)  # FREQ2
        self.write_reg(0x0E, 0xA7)  # FREQ1
        self.write_reg(0x0F, 0x62)  # FREQ0（具体值需根据晶振频率计算）

        # 设置调制方式为2-FSK（默认）
        self.write_reg(0x12, 0x10)  # MDMCFG4（带宽=325kHz）
        self.write_reg(0x13, 0x93)  # MDMCFG3（数据速率=38.4kbps）

        # 启用接收模式
        self.write_reg(0x08, 0x34)  # 发送SRX命令

    def receive_data(self):
        """轮询接收数据"""
        while True:
            rx_bytes = self.read_reg(0xFB)  # 读取RX FIFO中的字节数
            if rx_bytes > 0:
                data = []
                for _ in range(rx_bytes):
                    data.append(self.read_reg(0xFF))  # 读取FIFO数据
                print(f"Received: {bytes(data).hex()}")
            time.sleep(0.1)

if __name__ == "__main__":
    cc1101 = CC1101()
    cc1101.init_315mhz()
    print("Listening on 315MHz...")
    cc1101.receive_data()
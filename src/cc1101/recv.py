import time
import spidev
import RPi.GPIO as GPIO
import sys

class CC1101PollingReceiver:
    def __init__(self, frequency=433.92):
        # 仅保留必要引脚
        self.SPI_CSN = 8    # BCM 8 (物理引脚24)
        self.SPI_SCK = 11   # BCM 11 (物理引脚23)
        self.SPI_MOSI = 10  # BCM 10 (物理引脚19)
        self.SPI_MISO = 9   # BCM 9 (物理引脚21)
        
        # 频率设置
        self.frequency = frequency
        self.frequency_str = f"{self.frequency}MHz"
        
        # SPI初始化
        self.spi = spidev.SpiDev()
        self.spi.open(0, 0)  # SPI0, CE0
        self.spi.mode = 0b00
        self.spi.max_speed_hz = 100000
        
        # 硬件复位（通过SPI命令替代硬件复位线）
        self._strobe_command(0x30)  # SRES
        time.sleep(0.1)
        
        # 配置接收参数
        self._configure_receiver()

        # 清空 RX FIFO
        self._strobe_command(0x3A)  # SFRX
        print(f"[轮询模式] 频率设置为{self.frequency_str}")

    def _calculate_frequency_registers(self, target_frequency_mhz, crystal_frequency_mhz=26):
        """计算频率寄存器值"""
        target_frequency_hz = target_frequency_mhz * 1_000_000
        crystal_frequency_hz = crystal_frequency_mhz * 1_000_000
        
        freq = int((target_frequency_hz / crystal_frequency_hz) * (2**16))
        
        freq2 = (freq >> 16) & 0xFF
        freq1 = (freq >> 8) & 0xFF
        freq0 = freq & 0xFF
        
        print(f"[频率计算] FREQ2: {freq2:#04x}, FREQ1: {freq1:#04x}, FREQ0: {freq0:#04x}")

        return freq2, freq1, freq0

    def _configure_receiver(self):
        """精简版接收配置"""
        # 计算频率寄存器值
        freq2, freq1, freq0 = self._calculate_frequency_registers(self.frequency)

        # 设置频率寄存器
        self._write_reg(0x0D, freq2)  # FREQ2
        self._write_reg(0x0E, freq1)  # FREQ1
        self._write_reg(0x0F, freq0)  # FREQ0

        # 关键寄存器配置
        # self._write_reg(0x07, 0x0F)  # 无限包长度
        

        # self._write_reg(0x17, 0xD3)  # RX结束返回IDLE
        self._strobe_command(0x34)   # 进入接收模式

        # 简化
        self._write_reg(0x07, 0x04)
        self._write_reg(0x17, 0x00)


    def poll_data(self):
        """轮询接收数据"""
        while True:
            # print("[轮询模式] 等待数据...")
            # 检查RX FIFO状态（替代GDO0中断）
            rx_bytes = self._read_reg(0xFB) & 0x7F
            if rx_bytes > 0:
                data = self.spi.xfer2([0xFF] + [0]*rx_bytes)[1:]
                print(f"收到数据: {bytes(data).hex()}")
            time.sleep(0.01)  # 轮询间隔

    # 保留原有SPI操作方法
    def _strobe_command(self, command):
        self.spi.xfer2([command])
        time.sleep(0.01)

    def _write_reg(self, addr, value):
        self.spi.xfer2([addr & 0x3F, value])

    def _read_reg(self, addr):
        return self.spi.xfer2([addr | 0x80, 0])[1]

if __name__ == "__main__":
    freq = float(sys.argv[1]) if len(sys.argv) > 1 else 433.92
    receiver = CC1101PollingReceiver(frequency=freq)
    try:
        receiver.poll_data()
    except KeyboardInterrupt:
        receiver.spi.close()
        print("\n[退出] 接收器已关闭")

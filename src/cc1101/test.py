import time
import spidev

class CC1101Tester:
    def __init__(self):
        self.spi = spidev.SpiDev()
        try:
            self.spi.open(0, 0)  # 确保使用SPI0 CE0 (引脚24)，如果硬件连接为CE1请保持原样
            self.spi.mode = 0b00
            self.spi.max_speed_hz = 500000
            print("[初始化] SPI接口已打开")
        except Exception as e:
            print(f"[错误] SPI初始化失败: {str(e)}")
            exit(1)

    def write_reg(self, addr, value):
        """写入CC1101寄存器"""
        self.spi.xfer2([addr & 0x3F, value])  # 地址掩码0x3F表示写操作

    def read_reg(self, addr):
        """读取CC1101寄存器"""
        result = self.spi.xfer2([addr | 0x80, 0x00])
        return result[1]  # 地址最高位1表示读操作

    def test_spi_connection(self):
        """测试SPI通信是否正常"""
        try:
            partnum = self.read_reg(0xF0)  # 应读取PARTNUM寄存器<sup><span title="cc1101.pdf assistant-2fSrbyHH9gsVSyBsGyCP57"><strong>1</strong></span></sup>
            if partnum in [0x00, 0x14, 0x80]:
                print(f"[SPI测试] 成功 (PARTNUM=0x{partnum:02X})")
                return True
            else:
                print(f"[SPI测试] 异常返回值: 0x{partnum:02X}")
                return False
        except Exception as e:
            print(f"[SPI测试] 失败: {str(e)}")
            return False

    def test_register_rw(self):
        """测试寄存器读写功能"""
        test_addr, test_value = 0x0D, 0xAA  # 使用FREQ2寄存器测试<sup><span title="cc1101.pdf assistant-2fSrbyHH9gsVSyBsGyCP57"><strong>2</strong></span></sup>
        try:
            self.write_reg(test_addr, test_value)
            read_val = self.read_reg(test_addr)
            if read_val == test_value:
                print(f"[寄存器测试] 成功 (写入0x{test_value:02X}, 读取0x{read_val:02X})")
                return True
            else:
                print(f"[寄存器测试] 失败 (期望0x{test_value:02X}, 实际0x{read_val:02X})")
                return False
        except Exception as e:
            print(f"[寄存器测试] 异常: {str(e)}")
            return False

    def test_state_machine(self):
        """测试状态机切换"""
        try:
            self.spi.xfer2([0x36])  # 使用SIDLE命令来切换到IDLE状态<sup><span title="cc1101.pdf assistant-2fSrbyHH9gsVSyBsGyCP57"><strong>3</strong></span></sup>
            time.sleep(0.1)
            state = self.read_reg(0x35) & 0xF0  # MARCSTATE寄存器反映芯片状态<sup><span title="cc1101.pdf assistant-2fSrbyHH9gsVSyBsGyCP57"><strong>4</strong></span></sup>
            if state == 0x00:  # IDLE状态应为0x00
                print("[状态机测试] IDLE状态切换成功")
                return True
            else:
                print(f"[状态机测试] 状态异常 (当前状态: 0x{state:02X})")
                return False
        except Exception as e:
            print(f"[状态机测试] 异常: {str(e)}")
            return False

    def run_all_tests(self):
        """执行所有测试并汇总结果"""
        tests = [
            ("SPI通信", self.test_spi_connection),
            ("寄存器读写", self.test_register_rw),
            ("状态机", self.test_state_machine)
        ]
        
        print("\n=== 开始CC1101硬件测试 ===")
        results = []
        for name, test_func in tests:
            print(f"\n执行测试: {name}")
            results.append(test_func())
        
        print("\n=== 测试结果汇总 ===")
        for i, (name, _) in enumerate(tests):
            status = "通过" if results[i] else "失败"
            print(f"{name.ljust(10)}: {status}")
        
        return all(results)

if __name__ == "__main__":
    tester = CC1101Tester()
    final_result = tester.run_all_tests()
    print(f"\n整体测试结果: {'通过' if final_result else '失败'}")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTL433 信号监控脚本 - 小时级模式控制版
功能：
1. 启动rtl_433进程进行信号录制
2. 定期检查录制目录的新文件
3. 统计文件数量和大小
4. 发送通知到飞书webhook
5. 基于小时的精确模式控制（关闭/高功率/低功率/混合）
6. 忽略模式切换后1分钟内的信号
7. 自动偏置电源管理
"""

import os
import time
import subprocess
import threading
import requests
import json
from datetime import datetime, time as dt_time
from pathlib import Path
import signal
import sys
import atexit

class RTL433Monitor:
    def __init__(self, work_dir="/home/u03013112/rtl433_monitor", webhook_url=None):
        self.work_dir = Path(work_dir)
        self.webhook_url = webhook_url
        self.rtl433_process = None
        self.known_files = set()  # 已知的文件集合
        self.total_files = 0
        self.total_size = 0
        self.running = True
        
        # 模式控制
        self.current_mode = "off"  # off, high_power, low_power, mixed
        self.current_sub_mode = None  # 在mixed模式下的当前子模式
        self.mode_start_time = time.time()
        self.mixed_switch_interval = 10 * 60  # 混合模式下10分钟切换一次
        self.ignore_signals_duration = 60  # 切换后忽略信号的时间（秒）
        self.last_mode_switch_time = 0
        
        # 24小时模式配置 (0-23小时)
        # 'off': 关闭, 'high': 纯高功率, 'low': 纯低功率, 'mixed': 混合模式
        self.hourly_schedule = {
            0: 'off',      # 00:00-01:00 关闭
            1: 'off',      # 01:00-02:00 关闭
            2: 'off',      # 02:00-03:00 关闭
            3: 'off',      # 03:00-04:00 关闭
            4: 'off',      # 04:00-05:00 关闭
            5: 'off',      # 05:00-06:00 关闭
            6: 'high',     # 06:00-07:00 纯高功率
            7: 'high',     # 07:00-08:00 纯高功率
            8: 'mixed',    # 08:00-09:00 混合模式
            9: 'mixed',    # 09:00-10:00 混合模式
            10: 'high',    # 10:00-11:00 纯高功率
            11: 'high',    # 11:00-12:00 纯高功率
            12: 'mixed',   # 12:00-13:00 混合模式
            13: 'mixed',   # 13:00-14:00 混合模式
            14: 'high',    # 14:00-15:00 纯高功率
            15: 'high',    # 15:00-16:00 纯高功率
            16: 'mixed',   # 16:00-17:00 混合模式
            17: 'mixed',   # 17:00-18:00 混合模式
            18: 'high',    # 18:00-19:00 纯高功率
            19: 'high',    # 19:00-20:00 纯高功率
            20: 'mixed',   # 20:00-21:00 混合模式
            21: 'low',     # 21:00-22:00 纯低功率
            22: 'low',     # 22:00-23:00 纯低功率
            23: 'low',     # 23:00-24:00 纯低功率
        }
        
        # 创建工作目录
        self.work_dir.mkdir(exist_ok=True)
        
        # 初始化已知文件
        self._scan_existing_files()
        
        # 注册退出时的清理函数
        atexit.register(self.cleanup_bias_tee)
        
    def cleanup_bias_tee(self):
        """程序退出时关闭偏置电源"""
        try:
            print(f"[{datetime.now()}] 正在关闭偏置电源...")
            result = subprocess.run(['rtl_biast', '-b', '0'], 
                                  capture_output=True, timeout=5)
            if result.returncode == 0:
                print(f"[{datetime.now()}] 偏置电源已关闭")
            else:
                print(f"[{datetime.now()}] 偏置电源关闭命令执行，返回码: {result.returncode}")
        except Exception as e:
            print(f"[{datetime.now()}] 关闭偏置电源失败: {e}")
    
    def set_bias_tee(self, enable):
        """设置偏置电源状态"""
        try:
            state = '1' if enable else '0'
            result = subprocess.run(['rtl_biast', '-b', state], 
                                  capture_output=True, timeout=5)
            action = "开启" if enable else "关闭"
            if result.returncode == 0:
                print(f"[{datetime.now()}] 偏置电源已{action}")
                return True
            else:
                print(f"[{datetime.now()}] 偏置电源{action}失败，返回码: {result.returncode}")
                return False
        except Exception as e:
            print(f"[{datetime.now()}] 设置偏置电源失败: {e}")
            return False

    def get_current_schedule_mode(self):
        """获取当前小时应该使用的模式"""
        current_hour = datetime.now().hour
        return self.hourly_schedule.get(current_hour, 'off')
    
    def should_ignore_signal(self):
        """检查是否应该忽略当前信号（刚切换模式后1分钟内）"""
        if self.last_mode_switch_time == 0:
            return False
        
        time_since_switch = time.time() - self.last_mode_switch_time
        return time_since_switch < self.ignore_signals_duration

    def sendMessageToWebhook2(self, title, text, aText="", aUrl="", webhook=None):
        """发送消息到飞书webhook"""
        if not webhook:
            webhook = self.webhook_url
            
        if not webhook:
            print(f"[{datetime.now()}] Webhook URL not set, skipping notification")
            return
            
        # 在消息中添加当前模式信息
        schedule_mode = self.get_current_schedule_mode()
        if schedule_mode == 'off':
            mode_info = "\n⚙️ 当前模式: 关闭模式 ⏹️"
        elif schedule_mode == 'high':
            mode_info = "\n⚙️ 当前模式: 纯高功率模式 🚀"
        elif schedule_mode == 'low':
            mode_info = "\n⚙️ 当前模式: 纯低功率模式 🧊"
        elif schedule_mode == 'mixed':
            sub_mode = "高功率" if self.current_sub_mode == "high_power" else "低功率"
            mode_info = f"\n⚙️ 当前模式: 混合模式 ({sub_mode}) 🔄"
        else:
            mode_info = f"\n⚙️ 当前模式: {schedule_mode}"
        
        # 添加当前时间信息
        current_hour = datetime.now().hour
        time_info = f"\n⏰ 当前时间: {datetime.now().strftime('%H:%M')} (第{current_hour}小时)"
        
        text_with_info = text + mode_info + time_info
            
        url = webhook
        headers = {'Content-Type': 'application/json'}
        
        content = [{
            "tag": "text",
            "text": text_with_info
        }]
        
        if aText and aUrl:
            content.append({
                "tag": "a", 
                "text": aText,
                "href": aUrl
            })
            
        data = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": [content]
                    }
                }
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=data, timeout=10)
            if response.status_code != 200:
                print(f"[{datetime.now()}] Webhook Error: {response.status_code}, {response.text}")
            else:
                print(f"[{datetime.now()}] Notification sent successfully")
        except Exception as e:
            print(f"[{datetime.now()}] Webhook Exception: {e}")
    
    def _scan_existing_files(self):
        """扫描现有文件，初始化统计"""
        cu8_files = list(self.work_dir.glob("g*.cu8"))
        for file_path in cu8_files:
            self.known_files.add(file_path.name)
            self.total_size += file_path.stat().st_size
        
        self.total_files = len(cu8_files)
        print(f"[{datetime.now()}] 初始化完成: {self.total_files} 个文件, "
              f"总大小: {self._format_size(self.total_size)}")
    
    def _format_size(self, size_bytes):
        """格式化文件大小"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.2f} {size_names[i]}"
    
    def get_rtl433_command(self, power_mode="high_power"):
        """根据功率模式获取rtl433命令"""
        base_cmd = [
            "rtl_433",
            "-f", "433920000",
            "-s", "250000",  # 固定250kHz采样率
            "-S", "all",
            "-M", "time",
            "-M", "level"
        ]
        
        if power_mode == "high_power":
            # 高功率模式：启用偏置电源
            cmd = base_cmd + [
                "-t", "biastee=1,offset_tune=1",
                "-g", "49.6",
                "-Y", "level=-25"
            ]
        else:  # low_power mode
            # 低功率模式：关闭偏置电源
            cmd = base_cmd + [
                "-t", "offset_tune=1",
                "-g", "49.6",
                "-Y", "level=-25"
            ]
        
        return cmd
    
    def start_rtl433(self, power_mode="high_power"):
        """启动rtl_433进程"""
        cmd = self.get_rtl433_command(power_mode)
        
        try:
            # 切换到工作目录
            os.chdir(self.work_dir)
            
            power_text = "高功率" if power_mode == "high_power" else "低功率"
            print(f"[{datetime.now()}] 启动RTL433监听 ({power_text}模式)...")
            print(f"[{datetime.now()}] 工作目录: {self.work_dir}")
            print(f"[{datetime.now()}] 命令: {' '.join(cmd)}")
            
            # 设置偏置电源
            if power_mode == "high_power":
                self.set_bias_tee(True)
            else:
                self.set_bias_tee(False)
            
            # 启动进程，重定向输出到日志文件
            log_file = self.work_dir / f"rtl433_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{power_mode}.log"
            with open(log_file, 'w') as f:
                self.rtl433_process = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    cwd=self.work_dir
                )
            
            self.current_sub_mode = power_mode
            self.mode_start_time = time.time()
            self.last_mode_switch_time = time.time()
            
            # 发送启动通知
            bias_status = "开启" if power_mode == "high_power" else "关闭"
            
            self.sendMessageToWebhook2(
                f"🎯 RTL433监听已启动 ({power_text}模式)",
                f"监听频率: 433.92MHz\n"
                f"采样率: 250kHz\n"
                f"工作目录: {self.work_dir}\n"
                f"日志文件: {log_file.name}\n"
                f"偏置电源: {bias_status}\n"
                f"注意: 切换后1分钟内的信号将被忽略",
                "查看详情", 
                f"file://{log_file}"
            )
            
            return True
            
        except Exception as e:
            print(f"[{datetime.now()}] 启动RTL433失败: {e}")
            self.sendMessageToWebhook2(
                "❌ RTL433启动失败",
                f"错误信息: {str(e)}"
            )
            return False
    
    def stop_rtl433(self):
        """停止rtl433进程"""
        if self.rtl433_process:
            print(f"[{datetime.now()}] 停止RTL433进程...")
            self.rtl433_process.terminate()
            try:
                self.rtl433_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.rtl433_process.kill()
            self.rtl433_process = None
        
        # 关闭偏置电源
        self.set_bias_tee(False)
    
    def should_switch_in_mixed_mode(self):
        """在混合模式下检查是否应该切换子模式"""
        if self.get_current_schedule_mode() != 'mixed':
            return False
        
        running_time = time.time() - self.mode_start_time
        return running_time >= self.mixed_switch_interval
    
    def switch_sub_mode(self):
        """在混合模式下切换子模式"""
        if not self.rtl433_process:
            return
        
        old_sub_mode = self.current_sub_mode
        new_sub_mode = "low_power" if old_sub_mode == "high_power" else "high_power"
        
        running_time = time.time() - self.mode_start_time
        running_minutes = int(running_time / 60)
        
        print(f"[{datetime.now()}] 混合模式切换: {old_sub_mode} -> {new_sub_mode}, 运行时间: {running_minutes}分钟")
        
        # 停止当前进程
        self.stop_rtl433()
        time.sleep(1)  # 等待1秒
        
        # 启动新的子模式
        if self.start_rtl433(new_sub_mode):
            old_text = "高功率" if old_sub_mode == "high_power" else "低功率"
            new_text = "高功率" if new_sub_mode == "high_power" else "低功率"
            
            self.sendMessageToWebhook2(
                f"🔄 混合模式切换: {old_text} → {new_text}",
                f"上一子模式运行时间: {running_minutes}分钟\n"
                f"新子模式将运行: 10分钟\n"
                f"切换时间: {datetime.now().strftime('%H:%M:%S')}\n"
                f"注意: 切换后1分钟内的信号将被忽略"
            )
    
    def check_schedule_and_control(self):
        """检查时间表并控制进程"""
        required_mode = self.get_current_schedule_mode()
        
        if required_mode == 'off':
            # 需要关闭
            if self.rtl433_process:
                self.stop_rtl433()
                self.sendMessageToWebhook2(
                    "⏹️ 进入关闭模式",
                    f"当前时间: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"根据时间表，当前小时应该关闭\n"
                    f"偏置电源已关闭"
                )
                self.current_mode = 'off'
        
        elif required_mode in ['high', 'low']:
            # 需要纯模式
            power_mode = "high_power" if required_mode == 'high' else "low_power"
            
            if not self.rtl433_process or self.current_mode != required_mode:
                if self.rtl433_process:
                    self.stop_rtl433()
                
                self.start_rtl433(power_mode)
                self.current_mode = required_mode
                
                mode_text = "纯高功率" if required_mode == 'high' else "纯低功率"
                self.sendMessageToWebhook2(
                    f"⚡ 切换到{mode_text}模式",
                    f"当前时间: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"根据时间表，当前小时使用{mode_text}模式"
                )
        
        elif required_mode == 'mixed':
            # 需要混合模式
            if self.current_mode != 'mixed':
                if self.rtl433_process:
                    self.stop_rtl433()
                
                # 混合模式默认从高功率开始
                self.start_rtl433("high_power")
                self.current_mode = 'mixed'
                
                self.sendMessageToWebhook2(
                    "🔄 切换到混合模式",
                    f"当前时间: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"根据时间表，当前小时使用混合模式\n"
                    f"开始子模式: 高功率 (10分钟后切换到低功率)"
                )
            else:
                # 已经在混合模式，检查是否需要切换子模式
                if self.should_switch_in_mixed_mode():
                    self.switch_sub_mode()
    
    def check_new_files(self):
        """检查新文件"""
        try:
            current_files = set()
            current_total_size = 0
            
            # 扫描当前所有cu8文件
            cu8_files = list(self.work_dir.glob("g*.cu8"))
            
            for file_path in cu8_files:
                current_files.add(file_path.name)
                current_total_size += file_path.stat().st_size
            
            # 找出新文件
            new_files = current_files - self.known_files
            
            if new_files:
                # 检查是否应该忽略信号
                if self.should_ignore_signal():
                    print(f"[{datetime.now()}] 忽略 {len(new_files)} 个新文件（刚切换模式）")
                    # 更新已知文件但不发送通知
                    self.known_files.update(new_files)
                    self.total_files = len(current_files)
                    self.total_size = current_total_size
                    return
                
                new_files_info = []
                new_files_size = 0
                
                for filename in sorted(new_files):
                    file_path = self.work_dir / filename
                    file_size = file_path.stat().st_size
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    new_files_info.append({
                        'name': filename,
                        'size': file_size,
                        'time': file_time
                    })
                    new_files_size += file_size
                
                # 更新统计
                self.known_files.update(new_files)
                old_total_files = self.total_files
                old_total_size = self.total_size
                
                self.total_files = len(current_files)
                self.total_size = current_total_size
                
                # 构建通知消息
                files_text = "\n".join([
                    f"📁 {info['name']} ({self._format_size(info['size'])}) - {info['time'].strftime('%H:%M:%S')}"
                    for info in new_files_info
                ])
                
                message = (f"🆕 发现 {len(new_files)} 个新信号文件:\n\n"
                          f"{files_text}\n\n"
                          f"📊 统计信息:\n"
                          f"• 新增文件: {len(new_files)} 个\n"
                          f"• 新增大小: {self._format_size(new_files_size)}\n"
                          f"• 总文件数: {old_total_files} → {self.total_files}\n"
                          f"• 总大小: {self._format_size(old_total_size)} → {self._format_size(self.total_size)}")
                
                print(f"[{datetime.now()}] {message}")
                
                # 发送通知
                self.sendMessageToWebhook2(
                    "📡 新信号检测",
                    message,
                    "查看目录",
                    f"file://{self.work_dir}"
                )
                
        except Exception as e:
            print(f"[{datetime.now()}] 检查文件时出错: {e}")
    
    def send_status_report(self):
        """发送状态报告"""
        # 检查rtl433进程状态
        if self.rtl433_process:
            if self.rtl433_process.poll() is None:
                process_status = "🟢 运行中"
            else:
                process_status = f"🔴 已停止 (退出码: {self.rtl433_process.returncode})"
        else:
            process_status = "⏹️ 已关闭"
        
        # 磁盘使用情况
        disk_usage = os.statvfs(self.work_dir)
        free_space = disk_usage.f_frsize * disk_usage.f_available
        
        # 当前模式信息
        schedule_mode = self.get_current_schedule_mode()
        current_hour = datetime.now().hour
        
        if schedule_mode == 'off':
            mode_info = "关闭模式"
        elif schedule_mode == 'high':
            mode_info = "纯高功率模式"
        elif schedule_mode == 'low':
            mode_info = "纯低功率模式"
        elif schedule_mode == 'mixed':
            if self.rtl433_process:
                running_time = time.time() - self.mode_start_time
                remaining_time = max(0, self.mixed_switch_interval - running_time)
                remaining_minutes = int(remaining_time / 60)
                sub_mode = "高功率" if self.current_sub_mode == "high_power" else "低功率"
                mode_info = f"混合模式 ({sub_mode}, 剩余{remaining_minutes}分钟)"
            else:
                mode_info = "混合模式 (未运行)"
        else:
            mode_info = f"未知模式: {schedule_mode}"
        
        message = (f"📈 RTL433监控状态报告\n\n"
                  f"🔧 进程状态: {process_status}\n"
                  f"⏰ 当前时间: {datetime.now().strftime('%H:%M')} (第{current_hour}小时)\n"
                  f"⚙️ 当前模式: {mode_info}\n"
                  f"📁 工作目录: {self.work_dir}\n"
                  f"📊 已捕获: {self.total_files} 个信号文件\n"
                  f"💾 占用空间: {self._format_size(self.total_size)}\n"
                  f"🗄️ 剩余空间: {self._format_size(free_space)}\n"
                  f"⏰ 报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.sendMessageToWebhook2(
            "📊 RTL433状态报告",
            message
        )
        
        print(f"[{datetime.now()}] 状态报告已发送")
    
    def monitor_loop(self):
        """主监控循环"""
        last_status_time = time.time()
        status_interval = 3600  # 每小时发送一次状态报告
        
        # 初始检查时间表
        self.check_schedule_and_control()
        
        while self.running:
            try:
                # 检查时间表和模式控制
                self.check_schedule_and_control()
                
                # 只在有进程运行时检查文件
                if self.rtl433_process:
                    # 检查新文件
                    self.check_new_files()
                    
                    # 检查rtl433进程是否还在运行
                    if self.rtl433_process.poll() is not None:
                        print(f"[{datetime.now()}] RTL433进程意外退出，尝试重启...")
                        self.sendMessageToWebhook2(
                            "⚠️ RTL433进程异常",
                            f"进程意外退出，退出码: {self.rtl433_process.returncode}\n正在尝试重启..."
                        )
                        self.rtl433_process = None
                        time.sleep(5)
                        # 重新检查时间表来决定是否重启
                        self.check_schedule_and_control()
                
                # 检查是否需要发送状态报告
                current_time = time.time()
                if current_time - last_status_time >= status_interval:
                    self.send_status_report()
                    last_status_time = current_time
                
                # 等待30秒
                time.sleep(30)
                
            except KeyboardInterrupt:
                print(f"\n[{datetime.now()}] 收到中断信号，正在停止...")
                break
            except Exception as e:
                print(f"[{datetime.now()}] 监控循环出错: {e}")
                time.sleep(10)
    
    def stop(self):
        """停止监控"""
        self.running = False
        
        # 停止rtl433进程
        self.stop_rtl433()
        
        # 发送停止通知
        self.sendMessageToWebhook2(
            "🛑 RTL433监控已停止",
            f"监控结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"总共捕获: {self.total_files} 个信号文件\n"
            f"总大小: {self._format_size(self.total_size)}\n"
            f"偏置电源已关闭"
        )
        
        print(f"[{datetime.now()}] 监控已停止")

def signal_handler(signum, frame):
    """处理系统信号"""
    print(f"\n[{datetime.now()}] 收到信号 {signum}，正在停止监控...")
    if 'monitor' in globals():
        monitor.stop()
    
    # 确保偏置电源关闭
    try:
        subprocess.run(['rtl_biast', '-b', '0'], capture_output=True, timeout=5)
        print("偏置电源已关闭")
    except:
        print("关闭偏置电源失败，请手动执行: rtl_biast -b 0")
    
    sys.exit(0)

def main():
    global monitor
    
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Webhook URL
    testWebhookUrl = 'https://open.feishu.cn/open-apis/bot/v2/hook/acceb43c-5da3-47a2-987f-fc7228449a9c'
    
    # 创建监控实例
    monitor = RTL433Monitor(
        work_dir="/home/u03013112/rtl433_monitor",
        webhook_url=testWebhookUrl
    )
    
    # 发送启动通知，包含时间表信息
    schedule_info = "📅 24小时时间表:\n"
    for hour in range(24):
        mode = monitor.hourly_schedule[hour]
        mode_text = {
            'off': '关闭',
            'high': '高功率',
            'low': '低功率',
            'mixed': '混合'
        }.get(mode, mode)
        schedule_info += f"{hour:02d}:00-{(hour+1)%24:02d}:00 {mode_text}\n"
    
    monitor.sendMessageToWebhook2(
        "🚀 RTL433监控系统启动",
        f"系统已启动，开始按时间表运行\n\n{schedule_info}"
    )
    
    try:
        # 开始监控循环
        monitor.monitor_loop()
            
    except Exception as e:
        print(f"程序异常: {e}")
        monitor.sendMessageToWebhook2(
            "❌ RTL433监控程序异常",
            f"异常信息: {str(e)}"
        )
    finally:
        monitor.stop()

if __name__ == "__main__":
    main()
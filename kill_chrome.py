#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理残留的Chrome进程工具
当程序意外退出时，可以手动运行此脚本清理残留进程
"""
import psutil
import sys

def kill_chrome_processes():
    """杀死所有Chrome相关进程"""
    print("正在查找Chrome进程...")
    
    chrome_processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromium' in proc.info['name'].lower()):
                # 检查是否是自动化测试相关的进程
                if proc.info['cmdline'] and any('--remote-debugging-port' in arg for arg in proc.info['cmdline']):
                    chrome_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    if not chrome_processes:
        print("未找到Chrome自动化进程")
        return
    
    print(f"找到 {len(chrome_processes)} 个Chrome自动化进程:")
    for proc in chrome_processes:
        print(f"  PID: {proc.pid}, 名称: {proc.name()}")
    
    # 询问是否要终止
    confirm = input("是否要终止这些进程? (y/N): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("已取消操作")
        return
    
    # 终止进程
    killed_count = 0
    for proc in chrome_processes:
        try:
            proc.terminate()
            proc.wait(timeout=3)
            print(f"已终止进程 {proc.pid}")
            killed_count += 1
        except (psutil.NoSuchProcess, psutil.TimeoutExpired):
            try:
                proc.kill()
                print(f"已强制杀死进程 {proc.pid}")
                killed_count += 1
            except:
                print(f"无法终止进程 {proc.pid}")
        except Exception as e:
            print(f"终止进程 {proc.pid} 时出错: {e}")
    
    print(f"共终止了 {killed_count} 个进程")

if __name__ == "__main__":
    try:
        kill_chrome_processes()
    except KeyboardInterrupt:
        print("\n操作已被用户取消")
    except Exception as e:
        print(f"出现错误: {e}")
        sys.exit(1)
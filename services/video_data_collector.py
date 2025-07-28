#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
视频数据采集服务
集成video_processor_clean.py的功能，支持实时进度推送
"""

from DrissionPage import Chromium
import time
import pymongo
from datetime import datetime
from collections import defaultdict
import gc
import threading
import queue
import requests
import os
import signal
import atexit
import psutil

class VideoDataCollector:
    def __init__(self, progress_callback=None, filter_start_date=None, filter_end_date=None):
        self.progress_callback = progress_callback
        self.filter_start_date = filter_start_date
        self.filter_end_date = filter_end_date
        self.init_config()
        self.init_mongodb()
        self.is_running = False
        self.browser_pid = None
        # 注册退出清理函数
        atexit.register(self.force_cleanup)
        
        # 如果设置了日期过滤，记录到日志
        if self.filter_start_date and self.filter_end_date:
            self.send_progress(f"🗓️ 已设置日期过滤: {self.filter_start_date} 到 {self.filter_end_date}", "info")
        
    def init_config(self):
        self.login_page = 'http://10.0.0.5:31611/login'
        self.browser = None
        self.tab = None
        self.is_connected = False
        self.username_ele = 'xpath:/html/body/div[1]/div/div[2]/div[2]/div[1]/div[2]/input'
        self.password_ele = 'xpath:/html/body/div[1]/div/div[2]/div[2]/div[2]/div[2]/input'
        self.login_ele = 'xpath:/html/body/div[1]/div/div[2]/div[3]'
        self.backserver_token = None
    
    def init_mongodb(self):
        try:
            self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
            self.mydb = self.myclient["留存"]
            self.mycol_raw = self.mydb["原始数据"]
            self.mycol_retention = self.mydb["用户日活跃"]
            self.send_progress("✅ MongoDB连接成功", "info")
        except Exception as e:
            self.send_progress(f"❌ MongoDB连接失败: {e}", "error")
            self.myclient = None

    def send_progress(self, message, level="info"):
        """发送进度信息"""
        if self.progress_callback:
            self.progress_callback({
                'message': message,
                'level': level,
                'timestamp': datetime.now().strftime('%H:%M:%S')
            })

    def connect(self, use_existing=False):
        try:
            self.send_progress("🔗 正在启动浏览器...", "info")
            self.browser = Chromium()
            
            # 获取浏览器进程ID
            if self.browser and hasattr(self.browser, 'driver') and hasattr(self.browser.driver, 'service') and hasattr(self.browser.driver.service, 'process'):
                self.browser_pid = self.browser.driver.service.process.pid
                self.send_progress(f"🔗 浏览器进程ID: {self.browser_pid}", "info")
            
            if use_existing:
                self.tab = self.browser.latest_tab
            else:
                self.tab = self.browser.get_tab()
                self.tab.set.window.max()
            self.is_connected = True
            self.send_progress("✅ 浏览器连接成功", "success")
            return True
        except Exception as e:
            self.send_progress(f"❌ 浏览器连接失败: {e}", "error")
            return False
            
    def login(self, username='admin', password='admin@liandanxia'):
        try:
            self.send_progress("🔐 正在登录系统...", "info")
            self.tab.get(self.login_page)
            self.tab.wait(0.5)
            self.tab.ele(self.username_ele).input(username)
            self.tab.wait(0.5)
            self.tab.ele(self.password_ele).input(password)
            self.tab.wait(0.5)
            self.tab.ele(self.login_ele).click()
            self.tab.wait(2)
            self.backserver_token = self.get_backserver_token()
            
            if self.backserver_token:
                self.send_progress("✅ 登录成功，已获取token", "success")
                return True
            else:
                self.send_progress("❌ 登录失败，未获取到token", "error")
                return False
        except Exception as e:
            self.send_progress(f"❌ 登录失败: {e}", "error")
            return False
        
    def get_backserver_token(self):
        try:
            cookies = self.tab.cookies()
            for cookie in cookies:
                if 'backserver-token' in cookie.get('name', '').lower() or 'token' in cookie.get('name', '').lower():
                    return cookie['value']
            
            try:
                local_storage_token = self.tab.run_js('return localStorage.getItem("backserver-token") || localStorage.getItem("token") || localStorage.getItem("authToken")')
                if local_storage_token:
                    return local_storage_token
            except:
                pass
            
            try:
                all_local_storage = self.tab.run_js('''
                    var items = {};
                    for (var i = 0; i < localStorage.length; i++) {
                        var key = localStorage.key(i);
                        items[key] = localStorage.getItem(key);
                    }
                    return items;
                ''')
                
                for key, value in all_local_storage.items():
                    if 'token' in key.lower():
                        return value
                        
            except:
                pass
            
            return None
            
        except:
            return None
    
    def get_video_list(self, page_number=1, page_size=100, status=-1, username=""):
        if not self.backserver_token:
            self.send_progress("❌ 未获取到token", "error")
            return None
        
        url = "https://tu.liandanxia.com/api/cms/task/video_list"
        params = {
            'page_number': page_number,
            'page_size': page_size,
            'status': status,
            'username': username
        }
        
        headers = {
            'Authorization': f'Bearer {self.backserver_token}',
            'backserver-token': self.backserver_token,
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                self.send_progress(f"❌ 请求失败，状态码: {response.status_code}", "error")
                return None
        except Exception as e:
            self.send_progress(f"❌ 请求失败: {e}", "error")
            return None

    def extract_video_data(self, video_list_response):
        if not video_list_response or 'data' not in video_list_response:
            return []
        
        data = video_list_response['data']
        video_list = data.get('list', [])
        extracted_data = []
        
        for video in video_list:
            basic_info = {
                'id': video.get('id'),
                'prompt': video.get('prompt', '').strip(),
                'nickname': video.get('nickname'),
                'status': video.get('status'),
                'gen_start_time': video.get('gen_start_time'),
                'submit_time': video.get('submit_time'),
                'finish_time': video.get('finish_time'),
                'deleted': video.get('deleted'),
                'endpoint': video.get('endpoint')
            }
            
            param_info = {}
            if 'param' in video and isinstance(video['param'], dict):
                param = video['param']
                
                if 'param' in param and 'model' in param['param']:
                    model = param['param']['model']
                    param_info.update({
                        'model_name': model.get('model_name'),
                        'model_id': model.get('id'),
                        'model_type': model.get('type'),
                        'model_describe': model.get('describe')
                    })
                
                if 'param' in param:
                    inner_param = param['param']
                    param_info.update({
                        'gen_time': inner_param.get('gen_time'),
                        'input_image': inner_param.get('input_image'),
                        'file_name': inner_param.get('fileName'),
                        'picture_scale': inner_param.get('pictureScale')
                    })
                    
                    if 'scale' in inner_param:
                        scale = inner_param['scale']
                        param_info.update({
                            'scale_ratio': scale.get('scale'),
                            'scale_width': scale.get('scaleWidth'),
                            'scale_height': scale.get('scaleHeight')
                        })
                
                param_info['tools_type'] = video.get('tools_type')
                param_info['kind'] = video.get('kind')
            
            video_data = {**basic_info, **param_info}
            extracted_data.append(video_data)
        
        return extracted_data

    def aggregate_user_daily_data(self, raw_records):
        user_daily_groups = defaultdict(list)
        
        for record in raw_records:
            nickname = record.get('nickname')
            submit_time = record.get('submit_time')
            
            if not nickname or not submit_time:
                continue
            
            try:
                submit_date = datetime.strptime(submit_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except:
                continue
            
            record['submit_date'] = submit_date
            group_key = f"{nickname}_{submit_date}"
            user_daily_groups[group_key].append(record)
        
        user_summaries = []
        for group_key, records in user_daily_groups.items():
            nickname = records[0]['nickname']
            date = records[0]['submit_date']
            
            usage_count = len(records)
            success_count = len([r for r in records if r.get('status') == '已完成'])
            fail_count = usage_count - success_count
            
            submit_times = [r['submit_time'] for r in records if r.get('submit_time')]
            first_usage_time = min(submit_times) if submit_times else None
            last_usage_time = max(submit_times) if submit_times else None
            
            video_ids = [r.get('id') for r in records if r.get('id')]
            models_used = list(set([r.get('model_name') for r in records if r.get('model_name')]))
            total_prompt_length = sum([len(r.get('prompt', '')) for r in records])
            
            processing_times = []
            for record in records:
                if record.get('submit_time') and record.get('finish_time') and record.get('status') == '已完成':
                    try:
                        submit_dt = datetime.strptime(record['submit_time'], '%Y-%m-%d %H:%M:%S')
                        finish_dt = datetime.strptime(record['finish_time'], '%Y-%m-%d %H:%M:%S')
                        processing_minutes = (finish_dt - submit_dt).total_seconds() / 60
                        processing_times.append(processing_minutes)
                    except:
                        pass
            
            avg_processing_minutes = sum(processing_times) / len(processing_times) if processing_times else 0
            
            summary = {
                "unique_key": group_key,
                "nickname": nickname,
                "date": date,
                "usage_count": usage_count,
                "success_count": success_count,
                "fail_count": fail_count,
                "first_usage_time": first_usage_time,
                "last_usage_time": last_usage_time,
                "video_ids": video_ids,
                "models_used": models_used,
                "avg_processing_minutes": round(avg_processing_minutes, 2),
                "total_prompt_length": total_prompt_length,
                "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            user_summaries.append(summary)
        
        return user_summaries

    def save_batch_to_mongodb(self, raw_records, user_summaries):
        if not self.myclient:
            self.send_progress("❌ MongoDB未连接", "error")
            return {"raw_saved": 0, "summary_saved": 0}

        result = {"raw_saved": 0, "summary_saved": 0, "raw_skipped": 0, "summary_skipped": 0, 
                 "raw_filtered": 0, "summary_filtered": 0}

        try:
            # 处理原始数据
            if raw_records:
                # 先应用日期过滤 - 原始数据使用 submit_time 字段
                filtered_raw_records = self._filter_records_by_date(raw_records, "submit_time")
                result["raw_filtered"] = len(raw_records) - len(filtered_raw_records)
                
                # 再过滤已存在的记录
                existing_ids = set(self.mycol_raw.distinct("id"))
                new_raw_records = [r for r in filtered_raw_records if r.get("id") not in existing_ids]
                result["raw_skipped"] = len(filtered_raw_records) - len(new_raw_records)

                if new_raw_records:
                    insert_result = self.mycol_raw.insert_many(new_raw_records, ordered=False)
                    result["raw_saved"] = len(insert_result.inserted_ids)
                
                # 显示详细统计信息
                if result["raw_filtered"] > 0:
                    self.send_progress(f"✅ 原始数据: 新增 {result['raw_saved']} 条，重复跳过 {result['raw_skipped']} 条，日期过滤 {result['raw_filtered']} 条", "success")
                else:
                    self.send_progress(f"✅ 原始数据: 新增 {result['raw_saved']} 条，跳过 {result['raw_skipped']} 条", "success")

            # 处理用户汇总数据
            if user_summaries:
                # 先应用日期过滤
                filtered_summaries = self._filter_records_by_date(user_summaries, "date")
                result["summary_filtered"] = len(user_summaries) - len(filtered_summaries)
                
                # 再过滤已存在的记录
                existing_keys = set(self.mycol_retention.distinct("unique_key"))
                new_summaries = [s for s in filtered_summaries if s.get("unique_key") not in existing_keys]
                result["summary_skipped"] = len(filtered_summaries) - len(new_summaries)

                if new_summaries:
                    insert_result = self.mycol_retention.insert_many(new_summaries, ordered=False)
                    result["summary_saved"] = len(insert_result.inserted_ids)
                
                # 显示详细统计信息
                if result["summary_filtered"] > 0:
                    self.send_progress(f"✅ 用户日活跃: 新增 {result['summary_saved']} 条，重复跳过 {result['summary_skipped']} 条，日期过滤 {result['summary_filtered']} 条", "success")
                else:
                    self.send_progress(f"✅ 用户日活跃: 新增 {result['summary_saved']} 条，跳过 {result['summary_skipped']} 条", "success")

            return result

        except Exception as e:
            self.send_progress(f"❌ 保存到MongoDB失败: {e}", "error")
            return result
    
    def _filter_records_by_date(self, records, date_field):
        """根据日期范围过滤记录"""
        if not self.filter_start_date or not self.filter_end_date or not records:
            return records
        
        try:
            filtered_records = []
            
            # 调试信息：记录过滤设置
            self.send_progress(f"🔍 开始日期过滤: 范围 {self.filter_start_date} 到 {self.filter_end_date} (包含边界)", "info")
            
            for i, record in enumerate(records):
                record_date = record.get(date_field)
                if record_date:
                    # 根据字段类型处理日期
                    if date_field == "submit_time":
                        # submit_time 是完整时间戳，需要提取日期部分
                        try:
                            if isinstance(record_date, str) and len(record_date) >= 10:
                                record_date_str = record_date[:10]  # YYYY-MM-DD
                            else:
                                record_date_str = str(record_date)[:10]
                        except:
                            # 如果提取失败，跳过这条记录
                            if i < 3:
                                self.send_progress(f"🔍 记录 {i+1} 日期格式错误: '{record_date}'", "warning")
                            continue
                    else:
                        # date 字段已经是日期格式
                        if isinstance(record_date, str):
                            record_date_str = record_date[:10]  # 取前10位 YYYY-MM-DD
                        else:
                            record_date_str = str(record_date)[:10]
                    
                    # 调试信息：显示前几条记录的日期格式
                    if i < 3:
                        self.send_progress(f"🔍 记录 {i+1} 日期: 字段='{date_field}', 原始='{record_date}', 处理后='{record_date_str}'", "info")
                    
                    # 判断是否在日期范围内 (包含边界)
                    if self.filter_start_date <= record_date_str <= self.filter_end_date:
                        filtered_records.append(record)
                        if i < 3:
                            self.send_progress(f"🔍 记录 {i+1} 通过过滤: '{record_date_str}' 在范围内", "info")
                    elif i < 3:
                        # 显示为什么被过滤的原因
                        self.send_progress(f"🔍 记录 {i+1} 被过滤: '{record_date_str}' 不在 [{self.filter_start_date}, {self.filter_end_date}] 范围内", "info")
                else:
                    # 没有日期字段的记录
                    if i < 3:
                        self.send_progress(f"🔍 记录 {i+1} 被过滤: 缺少日期字段 '{date_field}'", "warning")
            
            # 调试信息：过滤结果统计
            self.send_progress(f"🔍 过滤结果: 总数 {len(records)}, 保留 {len(filtered_records)}, 过滤 {len(records) - len(filtered_records)}", "info")
            
            return filtered_records
            
        except Exception as e:
            self.send_progress(f"⚠️ 日期过滤失败: {e}，将保存所有数据", "warning")
            return records

    def fetch_batch_pages(self, start_page, end_page):
        batch_raw_data = []

        for page in range(start_page, end_page + 1):
            # 在每个页面获取前检查停止状态
            if not self.is_running:
                self.send_progress("❌ 数据获取已停止", "warning")
                break
                
            try:
                self.send_progress(f"🔄 正在获取第 {page} 页数据...", "info")
                page_response = self.get_video_list(page_number=page, page_size=100)

                if page_response and 'data' in page_response:
                    page_extracted = self.extract_video_data(page_response)
                    batch_raw_data.extend(page_extracted)
                    self.send_progress(f"✅ 第 {page} 页: 获取 {len(page_extracted)} 条记录", "success")
                else:
                    self.send_progress(f"⚠️ 第 {page} 页: 无数据", "warning")
                
                # 在页面间添加短暂延时，让停止检查有机会生效
                time.sleep(0.1)

            except Exception as e:
                self.send_progress(f"❌ 第 {page} 页获取失败: {e}", "error")
                continue

        return batch_raw_data

    def start_data_collection(self, total_pages=500, batch_size=20):
        """启动数据采集过程"""
        self.is_running = True
        
        try:
            if not self.myclient:
                self.send_progress("❌ MongoDB未连接", "error")
                return False

            self.send_progress("🚀 开始批量处理视频数据", "info")
            self.send_progress(f"📊 总页数: {total_pages}, 批次大小: {batch_size}页/批", "info")
            self.send_progress(f"⏱️ 预计处理时间: {(total_pages * 2 / 60):.1f} 分钟", "info")

            start_time = time.time()
            total_batches = (total_pages + batch_size - 1) // batch_size

            total_stats = {
                "total_raw_saved": 0,
                "total_summary_saved": 0,
                "total_raw_skipped": 0,
                "total_summary_skipped": 0,
                "total_raw_filtered": 0,
                "total_summary_filtered": 0,
                "processed_pages": 0,
                "failed_pages": 0
            }

            for batch_num in range(1, total_batches + 1):
                if not self.is_running:
                    self.send_progress("❌ 数据获取已停止", "warning")
                    break
                    
                batch_start_page = (batch_num - 1) * batch_size + 1
                batch_end_page = min(batch_num * batch_size, total_pages)

                self.send_progress(f"📦 处理批次 {batch_num}/{total_batches}", "info")
                self.send_progress(f"📄 页面范围: {batch_start_page} - {batch_end_page}", "info")

                try:
                    batch_raw_data = self.fetch_batch_pages(batch_start_page, batch_end_page)

                    if not batch_raw_data:
                        self.send_progress("⚠️ 当前批次无数据", "warning")
                        continue

                    self.send_progress(f"📊 批次原始数据: {len(batch_raw_data)} 条记录", "info")

                    user_summaries = self.aggregate_user_daily_data(batch_raw_data)
                    self.send_progress(f"📊 用户日活跃数据: {len(user_summaries)} 条记录", "info")

                    save_result = self.save_batch_to_mongodb(batch_raw_data, user_summaries)

                    total_stats["total_raw_saved"] += save_result["raw_saved"]
                    total_stats["total_summary_saved"] += save_result["summary_saved"]
                    total_stats["total_raw_skipped"] += save_result["raw_skipped"]
                    total_stats["total_summary_skipped"] += save_result["summary_skipped"]
                    total_stats["total_raw_filtered"] += save_result.get("raw_filtered", 0)
                    total_stats["total_summary_filtered"] += save_result.get("summary_filtered", 0)
                    total_stats["processed_pages"] += (batch_end_page - batch_start_page + 1)

                    self.show_progress(batch_num, total_batches, start_time, total_stats)

                    del batch_raw_data, user_summaries
                    gc.collect()

                except Exception as e:
                    self.send_progress(f"❌ 批次 {batch_num} 处理失败: {e}", "error")
                    total_stats["failed_pages"] += (batch_end_page - batch_start_page + 1)
                    continue

            self.show_final_stats(total_stats, start_time)
            self.is_running = False
            return True
            
        except Exception as e:
            self.send_progress(f"❌ 数据采集失败: {e}", "error")
            self.is_running = False
            return False

    def show_progress(self, current_batch, total_batches, start_time, stats):
        progress = (current_batch / total_batches) * 100
        elapsed_minutes = (time.time() - start_time) / 60

        if current_batch > 0:
            estimated_total_minutes = elapsed_minutes / current_batch * total_batches
            remaining_minutes = estimated_total_minutes - elapsed_minutes
        else:
            remaining_minutes = 0

        self.send_progress(f"📈 完成进度: {progress:.1f}% ({current_batch}/{total_batches}批)", "info")
        self.send_progress(f"⏱️ 已用时间: {elapsed_minutes:.1f} 分钟", "info")
        self.send_progress(f"⏳ 预计剩余: {remaining_minutes:.1f} 分钟", "info")
        self.send_progress(f"📊 累计保存: 原始数据 {stats['total_raw_saved']} 条, 用户日活跃 {stats['total_summary_saved']} 条", "info")

    def show_final_stats(self, stats, start_time):
        total_time_minutes = (time.time() - start_time) / 60

        self.send_progress("🎉 批量处理完成!", "success")
        self.send_progress(f"⏱️ 总用时: {total_time_minutes:.1f} 分钟", "success")
        self.send_progress(f"📄 处理页面: {stats['processed_pages']} 页", "success")
        self.send_progress(f"❌ 失败页面: {stats['failed_pages']} 页", "info")
        
        # 显示原始数据统计
        if stats.get('total_raw_filtered', 0) > 0:
            self.send_progress(f"📊 原始数据: 新增 {stats['total_raw_saved']} 条, 重复跳过 {stats['total_raw_skipped']} 条, 日期过滤 {stats['total_raw_filtered']} 条", "success")
        else:
            self.send_progress(f"📊 原始数据: 新增 {stats['total_raw_saved']} 条, 跳过 {stats['total_raw_skipped']} 条", "success")
        
        # 显示用户日活跃统计
        if stats.get('total_summary_filtered', 0) > 0:
            self.send_progress(f"📊 用户日活跃: 新增 {stats['total_summary_saved']} 条, 重复跳过 {stats['total_summary_skipped']} 条, 日期过滤 {stats['total_summary_filtered']} 条", "success")
        else:
            self.send_progress(f"📊 用户日活跃: 新增 {stats['total_summary_saved']} 条, 跳过 {stats['total_summary_skipped']} 条", "success")

    def stop_data_collection(self):
        """停止数据采集"""
        self.is_running = False
        self.send_progress("⏹️ 收到停止指令，正在停止数据采集...", "warning")
        self.send_progress("✅ 数据采集已停止", "info")

    def cleanup(self):
        """清理资源"""
        self.is_running = False
        if self.browser:
            try:
                self.browser.quit()
                self.send_progress("🔧 浏览器已关闭", "info")
            except:
                pass
        
        # 强制终止浏览器进程
        self._force_kill_browser_processes()
        
        if self.myclient:
            try:
                self.myclient.close()
                self.send_progress("🔧 MongoDB连接已关闭", "info")
            except:
                pass
    
    def force_cleanup(self):
        """强制清理所有资源（程序退出时调用）"""
        try:
            self.is_running = False
            print("程序退出，强制清理所有进程...")
            
            # 强制关闭浏览器
            if self.browser:
                try:
                    self.browser.quit()
                except:
                    pass
            
            # 强制终止所有Chrome相关进程
            self._force_kill_browser_processes()
            
            # 关闭数据库连接
            if self.myclient:
                try:
                    self.myclient.close()
                except:
                    pass
            
            print("强制清理完成")
        except Exception as e:
            print(f"强制清理异常: {e}")
    
    def _force_kill_browser_processes(self):
        """强制终止Chrome/Chromium相关进程"""
        try:
            # 先尝试终止记录的浏览器进程
            if self.browser_pid:
                try:
                    process = psutil.Process(self.browser_pid)
                    process.terminate()
                    process.wait(timeout=3)
                    if self.progress_callback:
                        self.send_progress(f"终止浏览器进程 {self.browser_pid}", "info")
                    else:
                        print(f"终止浏览器进程 {self.browser_pid}")
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    try:
                        process.kill()
                        if self.progress_callback:
                            self.send_progress(f"强制杀死浏览器进程 {self.browser_pid}", "info")
                        else:
                            print(f"强制杀死浏览器进程 {self.browser_pid}")
                    except:
                        pass
                except Exception as e:
                    if self.progress_callback:
                        self.send_progress(f"终止浏览器进程失败: {e}", "warning")
                    else:
                        print(f"终止浏览器进程失败: {e}")
            
            # 查找并终止所有Chrome/Chromium进程
            chrome_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromium' in proc.info['name'].lower()):
                        # 检查是否是我们启动的进程（通过命令行参数识别）
                        if proc.info['cmdline'] and any('--remote-debugging-port' in arg for arg in proc.info['cmdline']):
                            chrome_processes.append(proc)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # 终止找到的Chrome进程
            for proc in chrome_processes:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                    if self.progress_callback:
                        self.send_progress(f"终止Chrome进程 {proc.pid}", "info")
                    else:
                        print(f"终止Chrome进程 {proc.pid}")
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    try:
                        proc.kill()
                        if self.progress_callback:
                            self.send_progress(f"强制杀死Chrome进程 {proc.pid}", "info")
                        else:
                            print(f"强制杀死Chrome进程 {proc.pid}")
                    except:
                        pass
                except Exception as e:
                    if self.progress_callback:
                        self.send_progress(f"终止Chrome进程失败: {e}", "warning")
                    else:
                        print(f"终止Chrome进程失败: {e}")
                    
        except Exception as e:
            if self.progress_callback:
                self.send_progress(f"强制终止浏览器进程失败: {e}", "warning")
            else:
                print(f"强制终止浏览器进程失败: {e}")
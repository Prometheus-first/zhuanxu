from DrissionPage import Chromium
import time
import pymongo
from datetime import datetime
from collections import defaultdict
import gc

class Client:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.init_config()
        self.init_mongodb()
        
    def init_config(self):
        self.login_page = 'http://10.0.0.5:31611/login'
        self.browser = None
        self.tab = None
        self.is_connected = False
        self.username_ele = 'xpath:/html/body/div[1]/div/div[2]/div[2]/div[1]/div[2]/input'
        self.password_ele = 'xpath:/html/body/div[1]/div/div[2]/div[2]/div[2]/div[2]/input'
        self.login_ele = 'xpath:/html/body/div[1]/div/div[2]/div[3]'
    
    def init_mongodb(self):
        try:
            self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
            self.mydb = self.myclient["留存"]
            self.mycol_raw = self.mydb["原始数据"]
            self.mycol_retention = self.mydb["用户日活跃"]
            print("✅ MongoDB连接成功")
        except Exception as e:
            print(f"❌ MongoDB连接失败: {e}")
            self.myclient = None

    def connect(self, use_existing: bool = False) -> bool:
        try:
            self.browser = Chromium()
            if use_existing:
                self.tab = self.browser.latest_tab
            else:
                self.tab = self.browser.get_tab()
                self.tab.set.window.max()
            self.is_connected = True
            return True
        except:
            return False
            
    def login(self):
        self.tab.get(self.login_page)
        self.tab.wait(0.5)
        self.tab.ele(self.username_ele).input(self.username)
        self.tab.wait(0.5)
        self.tab.ele(self.password_ele).input(self.password)
        self.tab.wait(0.5)
        self.tab.ele(self.login_ele).click()
        self.tab.wait(2)
        self.backserver_token = self.get_backserver_token()
        
    def get_backserver_token(self):
        try:
            cookies = self.tab.cookies()
            for cookie in cookies:
                if 'backserver-token' in cookie.get('name', '').lower() or 'token' in cookie.get('name', '').lower():
                    print(f"从Cookie获取到token: {cookie['name']} = {cookie['value']}")
                    return cookie['value']
            
            try:
                local_storage_token = self.tab.run_js('return localStorage.getItem("backserver-token") || localStorage.getItem("token") || localStorage.getItem("authToken")')
                if local_storage_token:
                    print(f"从localStorage获取到token: {local_storage_token}")
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
                        print(f"找到token: {key} = {value}")
                        return value
                        
            except:
                pass
            
            print("未找到backserver-token")
            return None
            
        except:
            return None
    
    def get_token(self):
        return getattr(self, 'backserver_token', None)
    
    def get_video_list(self, page_number=1, page_size=100, status=-1, username=""):
        import requests
        
        token = self.get_token()
        if not token:
            print("❌ 未获取到token")
            return None
        
        url = "https://tu.liandanxia.com/api/cms/task/video_list"
        params = {
            'page_number': page_number,
            'page_size': page_size,
            'status': status,
            'username': username
        }
        
        headers = {
            'Authorization': f'Bearer {token}',
            'backserver-token': token,
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ 请求失败，状态码: {response.status_code}")
                return None
        except:
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
            print("❌ MongoDB未连接")
            return {"raw_saved": 0, "summary_saved": 0}

        result = {"raw_saved": 0, "summary_saved": 0, "raw_skipped": 0, "summary_skipped": 0}

        try:
            if raw_records:
                existing_ids = set(self.mycol_raw.distinct("id"))
                new_raw_records = [r for r in raw_records if r.get("id") not in existing_ids]
                result["raw_skipped"] = len(raw_records) - len(new_raw_records)

                if new_raw_records:
                    insert_result = self.mycol_raw.insert_many(new_raw_records, ordered=False)
                    result["raw_saved"] = len(insert_result.inserted_ids)
                    print(f"✅ 原始数据: 新增 {result['raw_saved']} 条，跳过 {result['raw_skipped']} 条")

            if user_summaries:
                existing_keys = set(self.mycol_retention.distinct("unique_key"))
                new_summaries = [s for s in user_summaries if s.get("unique_key") not in existing_keys]
                result["summary_skipped"] = len(user_summaries) - len(new_summaries)

                if new_summaries:
                    insert_result = self.mycol_retention.insert_many(new_summaries, ordered=False)
                    result["summary_saved"] = len(insert_result.inserted_ids)
                    print(f"✅ 用户日活跃: 新增 {result['summary_saved']} 条，跳过 {result['summary_skipped']} 条")

            return result

        except Exception as e:
            print(f"❌ 保存到MongoDB失败: {e}")
            return result

    def fetch_batch_pages(self, start_page, end_page):
        batch_raw_data = []

        for page in range(start_page, end_page + 1):
            try:
                print(f"🔄 正在获取第 {page} 页数据...")
                page_response = self.get_video_list(page_number=page, page_size=100)

                if page_response and 'data' in page_response:
                    page_extracted = self.extract_video_data(page_response)
                    batch_raw_data.extend(page_extracted)
                    print(f"✅ 第 {page} 页: 获取 {len(page_extracted)} 条记录")
                else:
                    print(f"⚠️ 第 {page} 页: 无数据")

                

            except Exception as e:
                print(f"❌ 第 {page} 页获取失败: {e}")
                continue

        return batch_raw_data

    def process_all_video_data(self, total_pages=500, batch_size=20):
        if not self.myclient:
            print("❌ MongoDB未连接")
            return

        print(f"\n🚀 开始批量处理视频数据")
        print(f"📊 总页数: {total_pages}, 批次大小: {batch_size}页/批")
        print(f"⏱️ 预计处理时间: {(total_pages * 2 / 60):.1f} 分钟")
        print("="*60)

        start_time = time.time()
        total_batches = (total_pages + batch_size - 1) // batch_size

        total_stats = {
            "total_raw_saved": 0,
            "total_summary_saved": 0,
            "total_raw_skipped": 0,
            "total_summary_skipped": 0,
            "processed_pages": 0,
            "failed_pages": 0
        }

        for batch_num in range(1, total_batches + 1):
            batch_start_page = (batch_num - 1) * batch_size + 1
            batch_end_page = min(batch_num * batch_size, total_pages)

            print(f"\n📦 处理批次 {batch_num}/{total_batches}")
            print(f"📄 页面范围: {batch_start_page} - {batch_end_page}")

            try:
                batch_raw_data = self.fetch_batch_pages(batch_start_page, batch_end_page)

                if not batch_raw_data:
                    print("⚠️ 当前批次无数据")
                    continue

                print(f"📊 批次原始数据: {len(batch_raw_data)} 条记录")

                user_summaries = self.aggregate_user_daily_data(batch_raw_data)
                print(f"📊 用户日活跃数据: {len(user_summaries)} 条记录")

                save_result = self.save_batch_to_mongodb(batch_raw_data, user_summaries)

                total_stats["total_raw_saved"] += save_result["raw_saved"]
                total_stats["total_summary_saved"] += save_result["summary_saved"]
                total_stats["total_raw_skipped"] += save_result["raw_skipped"]
                total_stats["total_summary_skipped"] += save_result["summary_skipped"]
                total_stats["processed_pages"] += (batch_end_page - batch_start_page + 1)

                self.show_progress(batch_num, total_batches, start_time, total_stats)

                del batch_raw_data, user_summaries
                gc.collect()

            except Exception as e:
                print(f"❌ 批次 {batch_num} 处理失败: {e}")
                total_stats["failed_pages"] += (batch_end_page - batch_start_page + 1)
                continue

        self.show_final_stats(total_stats, start_time)

    def show_progress(self, current_batch, total_batches, start_time, stats):
        progress = (current_batch / total_batches) * 100
        elapsed_minutes = (time.time() - start_time) / 60

        if current_batch > 0:
            estimated_total_minutes = elapsed_minutes / current_batch * total_batches
            remaining_minutes = estimated_total_minutes - elapsed_minutes
        else:
            remaining_minutes = 0

        print(f"\n📈 进度报告:")
        print(f"  完成进度: {progress:.1f}% ({current_batch}/{total_batches}批)")
        print(f"  已用时间: {elapsed_minutes:.1f} 分钟")
        print(f"  预计剩余: {remaining_minutes:.1f} 分钟")
        print(f"  已处理页面: {stats['processed_pages']}")
        print(f"  累计保存: 原始数据 {stats['total_raw_saved']} 条, 用户日活跃 {stats['total_summary_saved']} 条")

    def show_final_stats(self, stats, start_time):
        total_time_minutes = (time.time() - start_time) / 60

        print(f"\n🎉 批量处理完成!")
        print("="*60)
        print(f"⏱️ 总用时: {total_time_minutes:.1f} 分钟")
        print(f"📄 处理页面: {stats['processed_pages']} 页")
        print(f"❌ 失败页面: {stats['failed_pages']} 页")
        print(f"\n📊 数据保存统计:")
        print(f"  原始数据: 新增 {stats['total_raw_saved']} 条, 跳过 {stats['total_raw_skipped']} 条")
        print(f"  用户日活跃: 新增 {stats['total_summary_saved']} 条, 跳过 {stats['total_summary_skipped']} 条")
        print(f"\n✅ 数据已保存到MongoDB数据库: 留存.原始数据 和 留存.用户日活跃")

if __name__=="__main__":
    client = Client(username='admin',password='admin@liandanxia')
    client.connect(use_existing=False)
    client.login()  

    print("\n" + "="*60)
    print("🎯 视频数据处理系统")
    print("="*60)

    try:
        client.process_all_video_data(total_pages=500, batch_size=20)
    except KeyboardInterrupt:
        print("\n\n👋 用户取消操作")
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")

    print("\n" + "="*60)
    print("✅ 程序执行完成")
    print("="*60)

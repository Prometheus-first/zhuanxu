"""
留存分析服务 - 处理数据上传和留存分析功能
"""
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import os
from collections import defaultdict
from utils.database import db

class RetentionService:

    @staticmethod
    def _serialize_record(record):
        """序列化记录，处理不能JSON序列化的类型"""
        import json
        from bson import ObjectId

        serialized = {}
        for key, value in record.items():
            try:
                if isinstance(value, datetime):
                    serialized[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, ObjectId):
                    serialized[key] = str(value)
                elif value is None:
                    serialized[key] = None
                else:
                    # 测试是否可以JSON序列化
                    json.dumps(value)
                    serialized[key] = value
            except (TypeError, ValueError):
                # 如果无法序列化，转换为字符串
                serialized[key] = str(value) if value is not None else None
        return serialized

    @staticmethod
    def process_and_store_data(file_path, file_content=None, force_overwrite=False):
        """
        数据处理阶段：从Excel/CSV读取数据，处理合并后存入数据库
        :param file_path: 数据文件路径
        :param file_content: 文件内容（用于前端上传的文件）
        :param force_overwrite: 是否强制覆盖重复数据
        :return: 处理结果
        """
        try:
            # 根据文件扩展名选择读取方式
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_content:
                # 处理前端上传的文件内容
                if file_ext in ['.xlsx', '.xls']:
                    df1 = pd.read_excel(file_content)
                elif file_ext == '.csv':
                    df1 = pd.read_csv(file_content)
                else:
                    return {
                        'success': False,
                        'message': f'不支持的文件格式: {file_ext}'
                    }
            else:
                # 处理本地文件路径
                if not os.path.exists(file_path):
                    return {
                        'success': False,
                        'message': f'文件不存在: {file_path}'
                    }
                
                if file_ext in ['.xlsx', '.xls']:
                    df1 = pd.read_excel(file_path)
                elif file_ext == '.csv':
                    df1 = pd.read_csv(file_path)
                else:
                    return {
                        'success': False,
                        'message': f'不支持的文件格式: {file_ext}'
                    }
            
            original_count = len(df1)
            
            # 连接MongoDB - 使用留存数据库
            try:
                client = pymongo.MongoClient('mongodb://localhost:27017/')
                retention_db = client['留存']  # 留存分析专用数据库
                collection = retention_db['数据']  # 集合名称
            except Exception as e:
                return {
                    'success': False,
                    'message': f'连接数据库失败: {str(e)}'
                }
            
            # 将DataFrame转换为字典列表
            data_records = df1.to_dict('records')
            
            # 处理数据格式并合并相同用户的记录
            # 先处理时间格式和访问时长
            for record in data_records:
                # 转换访问时间
                if '访问时间' in record:
                    try:
                        record['访问时间'] = datetime.strptime(record['访问时间'], '%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                # 处理访问时长：将"未知"、空值、None转换为0
                if '访问时长' in record:
                    duration = record['访问时长']
                    if duration == '未知' or duration == '' or duration is None:
                        record['访问时长'] = 0
                    else:
                        try:
                            record['访问时长'] = int(duration)
                        except (ValueError, TypeError):
                            record['访问时长'] = 0
            
            # 按 IP + 地域 + 日期 分组合并数据
            merged_data = defaultdict(lambda: {
                '访问次数': 0,
                '总访问时长': 0,
                '最早访问时间': None,
                '最晚访问时间': None,
                '其他信息': {}
            })
            
            for record in data_records:
                if record.get('访问时间') and record.get('访问ip') and record.get('地域'):
                    # 提取日期作为分组键的一部分
                    visit_date = record['访问时间'].date()
                    group_key = (record['访问ip'], record['地域'], visit_date)
                    
                    # 累加访问次数和时长
                    merged_data[group_key]['访问次数'] += 1
                    merged_data[group_key]['总访问时长'] += record.get('访问时长', 0)
                    
                    # 记录最早和最晚访问时间
                    current_time = record['访问时间']
                    if merged_data[group_key]['最早访问时间'] is None or current_time < merged_data[group_key]['最早访问时间']:
                        merged_data[group_key]['最早访问时间'] = current_time
                    if merged_data[group_key]['最晚访问时间'] is None or current_time > merged_data[group_key]['最晚访问时间']:
                        merged_data[group_key]['最晚访问时间'] = current_time
                    
                    # 保存其他字段信息（使用第一条记录的信息）
                    if not merged_data[group_key]['其他信息']:
                        merged_data[group_key]['其他信息'] = {
                            '来源': record.get('来源', ''),
                            '关键词': record.get('关键词', ''),
                            '搜索词': record.get('搜索词', ''),
                            '入口界面': record.get('入口界面', ''),
                            '系统': record.get('系统', ''),
                            '浏览器': record.get('浏览器', ''),
                            '来源类型': record.get('来源类型', ''),
                            '网站': record.get('网站', ''),
                            '流量类型': record.get('流量类型', '')
                        }
            
            # 将合并后的数据转换为最终格式
            final_records = []
            for (ip, region, date), data in merged_data.items():
                final_record = {
                    '访问时间': data['最早访问时间'],
                    '地域': region,
                    '访问ip': ip,
                    '访问日期': datetime.combine(date, datetime.min.time()),
                    '访问次数': data['访问次数'],
                    '访问时长': data['总访问时长'],
                    '总访问时长': data['总访问时长'],
                    '最早访问时间': data['最早访问时间'],
                    '最晚访问时间': data['最晚访问时间'],
                    **data['其他信息']
                }
                final_records.append(final_record)
            
            merged_count = len(final_records)
            
            # 统计合并示例
            merge_examples = []
            for (ip, region, date), data in list(merged_data.items())[:3]:
                if data['访问次数'] > 1:
                    merge_examples.append({
                        'ip': ip,
                        'region': region,
                        'date': str(date),
                        'visit_count': data['访问次数'],
                        'total_duration': data['总访问时长'],
                        'time_range': f"{data['最早访问时间'].strftime('%H:%M:%S')} - {data['最晚访问时间'].strftime('%H:%M:%S')}"
                    })
            
            # 统计访问时长处理情况
            duration_stats = {'有效时长': 0, '未知转换': 0, '空值转换': 0, '总时长': 0}
            for record in data_records:
                original_duration = record.get('访问时长')
                if original_duration == 0:
                    duration_stats['未知转换'] += 1
                else:
                    duration_stats['有效时长'] += 1
                duration_stats['总时长'] += record.get('访问时长', 0)
            
            # 检查是否有重复数据
            duplicate_dates = set()
            for record in final_records:
                if '访问日期' in record:
                    visit_date = record['访问日期']
                    date_str = visit_date.strftime('%Y-%m-%d')

                    # 检查数据库中是否已存在该日期的数据
                    existing_count = collection.count_documents({
                        '访问日期': {
                            '$gte': datetime.combine(visit_date.date(), datetime.min.time()),
                            '$lt': datetime.combine(visit_date.date() + timedelta(days=1), datetime.min.time())
                        }
                    })

                    if existing_count > 0:
                        duplicate_dates.add(date_str)

            # 如果有重复日期且不强制覆盖，提供选项
            if duplicate_dates and not force_overwrite:
                duplicate_dates_list = sorted(list(duplicate_dates))

                # 关闭连接
                client.close()

                return {
                    'success': True,
                    'has_duplicates': True,
                    'message': f'检测到重复数据：{", ".join(duplicate_dates_list)} 日期的数据已存在',
                    'data': {
                        'original_count': original_count,
                        'merged_count': merged_count,
                        'merged_diff': original_count - merged_count,
                        'duplicate_dates': duplicate_dates_list,
                        'merge_examples': merge_examples,
                        'duration_stats': duration_stats
                    }
                }

            # 如果强制覆盖，先删除重复日期的数据
            if duplicate_dates and force_overwrite:
                deleted_count = 0
                for date_str in duplicate_dates:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    delete_result = collection.delete_many({
                        '访问日期': {
                            '$gte': datetime.combine(date_obj, datetime.min.time()),
                            '$lt': datetime.combine(date_obj + timedelta(days=1), datetime.min.time())
                        }
                    })
                    deleted_count += delete_result.deleted_count

            # 批量插入合并后的数据到MongoDB
            result = collection.insert_many(final_records)

            # 关闭连接
            client.close()

            # 准备返回消息
            message = '数据处理完成'
            if duplicate_dates and force_overwrite:
                message += f'，已覆盖 {len(duplicate_dates)} 个重复日期的数据'

            # 准备示例记录（序列化处理）
            sample_records = []
            for record in final_records[:3]:
                sample_records.append(RetentionService._serialize_record(record))

            return {
                'success': True,
                'has_duplicates': False,
                'message': message,
                'data': {
                    'original_count': original_count,
                    'merged_count': merged_count,
                    'merged_diff': original_count - merged_count,
                    'inserted_count': len(result.inserted_ids),
                    'deleted_count': deleted_count if duplicate_dates and force_overwrite else 0,
                    'overwritten_dates': sorted(list(duplicate_dates)) if duplicate_dates and force_overwrite else [],
                    'merge_examples': merge_examples,
                    'duration_stats': duration_stats,
                    'sample_records': sample_records
                }
            }
            
        except Exception as e:
            print(f"数据处理失败: {str(e)}")  # 添加日志
            return {
                'success': False,
                'message': f'数据处理失败: {str(e)}'
            }

    @staticmethod
    def analyze_retention(start_date=None, end_date=None):
        """
        分析用户留存情况 - 待开发
        :param start_date: 开始日期，格式：'2025-07-17' 或 datetime对象
        :param end_date: 结束日期，格式：'2025-07-27' 或 datetime对象
        :return: 留存分析结果
        """
        return {
            'success': False,
            'message': '留存分析功能正在开发中，敬请期待！',
            'data': {
                'status': 'under_development',
                'note': '新的留存分析逻辑正在开发中'
            }
        }

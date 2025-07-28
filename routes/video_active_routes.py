#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
视频工具活跃数据路由
整合 yisen 功能，专注视频工具数据分析
"""

from flask import Blueprint, request, jsonify, make_response, Response
from auth.middleware import login_required, require_roles
from services.video_active_service import VideoActiveService
from services.video_data_collector import VideoDataCollector
import io
import csv
import json
import time
import threading
import queue
from datetime import datetime

video_active_bp = Blueprint('video_active', __name__)

@video_active_bp.route('/data-summary', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def get_data_summary():
    """数据概览接口"""
    try:
        service = VideoActiveService()
        
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        summary_data = service.get_data_summary(start_date, end_date)
        
        return jsonify({
            'success': True,
            'data': summary_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取数据概览失败: {str(e)}'
        }), 500

@video_active_bp.route('/date-range', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def query_date_range():
    """日期范围查询接口"""
    try:
        service = VideoActiveService()
        
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'message': '请提供开始日期和结束日期'
            }), 400
        
        data = service.query_users_by_date_range(start_date, end_date)
        
        return jsonify({
            'success': True,
            'data': data,
            'total': len(data)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'日期范围查询失败: {str(e)}'
        }), 500

@video_active_bp.route('/single-date', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def query_single_date():
    """单日查询接口"""
    try:
        service = VideoActiveService()
        
        single_date = request.args.get('single_date')
        
        if not single_date:
            return jsonify({
                'success': False,
                'message': '请提供查询日期'
            }), 400
        
        data = service.query_users_by_single_date(single_date)
        
        return jsonify({
            'success': True,
            'data': data,
            'total': len(data)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'单日查询失败: {str(e)}'
        }), 500

@video_active_bp.route('/user-history', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def query_user_history():
    """用户历史查询接口"""
    try:
        service = VideoActiveService()
        
        nickname = request.args.get('nickname')
        
        if not nickname:
            return jsonify({
                'success': False,
                'message': '请提供用户昵称'
            }), 400
        
        data = service.query_user_history(nickname)
        
        return jsonify({
            'success': True,
            'data': data,
            'total': len(data)
        })
        
    except Exception as e:
        return jsonify({
            'success': False, 
            'message': f'用户历史查询失败: {str(e)}'
        }), 500

@video_active_bp.route('/active-summary', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def get_active_summary():
    """活跃用户汇总接口"""
    try:
        service = VideoActiveService()
        
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'message': '请提供开始日期和结束日期'
            }), 400
        
        summary_data = service.get_active_users_summary(start_date, end_date)
        
        return jsonify({
            'success': True,
            'data': summary_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'活跃用户汇总失败: {str(e)}'
        }), 500

@video_active_bp.route('/export-csv', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def export_to_csv():
    """导出数据到CSV - 浏览器直接下载"""
    try:
        service = VideoActiveService()
        
        # 获取查询参数
        query_type = request.args.get('query_type', 'date_range')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # 根据查询类型获取数据
        data = []
        filename = f"video_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if query_type == 'data_summary':
            if start_date and end_date:
                summary = service.get_data_summary(start_date, end_date)
                # 将概览数据转换为表格格式
                data = [{
                    'period': summary.get('period', ''),
                    'total_records': summary.get('total_records', 0),
                    'unique_users': summary.get('unique_users', 0),
                    'unique_dates': summary.get('unique_dates', 0),
                    'total_usage': summary.get('total_usage', 0),
                    'total_success': summary.get('total_success', 0),
                    'total_fail': summary.get('total_fail', 0),
                    'total_prompt_length': summary.get('total_prompt_length', 0),
                    'success_rate': summary.get('success_rate', '0%')
                }]
            filename = f"video_summary_{start_date}_to_{end_date}.csv"
            
        elif query_type == 'date_range':
            if not start_date or not end_date:
                return jsonify({
                    'success': False,
                    'message': '请提供开始日期和结束日期'
                }), 400
            data = service.query_users_by_date_range(start_date, end_date)
            filename = f"video_data_{start_date}_to_{end_date}.csv"
            
        elif query_type == 'single_date':
            single_date = request.args.get('single_date')
            if not single_date:
                return jsonify({
                    'success': False,
                    'message': '请提供查询日期'
                }), 400
            data = service.query_users_by_single_date(single_date)
            filename = f"video_data_{single_date}.csv"
            
        elif query_type == 'user_history':
            nickname = request.args.get('nickname')
            if not nickname:
                return jsonify({
                    'success': False,
                    'message': '请提供用户昵称'
                }), 400
            data = service.query_user_history(nickname)
            filename = f"video_user_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
        elif query_type == 'active_summary':
            if not start_date or not end_date:
                return jsonify({
                    'success': False,
                    'message': '请提供开始日期和结束日期'
                }), 400
            summary = service.get_active_users_summary(start_date, end_date)
            # 将汇总数据转换为表格格式
            if summary and 'user_stats' in summary:
                data = []
                for nickname, stats in summary['user_stats'].items():
                    data.append({
                        'username': nickname,
                        'active_days': stats['active_days'],
                        'total_usage': stats['total_usage'],
                        'success_count': stats['total_success'],
                        'fail_count': stats['total_fail'],
                        'total_prompt_length': stats['total_prompt_length'],
                        'avg_processing_minutes': stats['avg_processing_minutes'],
                        'success_rate': f"{(stats['total_success']/stats['total_usage']*100):.1f}%" if stats['total_usage'] > 0 else "0%"
                    })
            filename = f"video_active_summary_{start_date}_to_{end_date}.csv"
        
        # 检查数据
        if not data:
            print(f"导出失败: 查询类型={query_type}, 开始日期={start_date}, 结束日期={end_date}, 数据为空")
            return jsonify({
                'success': False,
                'message': f'没有数据可导出 (查询类型: {query_type})'
            }), 400
        
        # 创建CSV内容
        output = io.StringIO()
        if data:
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        # 添加BOM头确保Excel正确显示中文
        csv_content = '\ufeff' + output.getvalue()
        
        # 创建响应
        response = make_response(csv_content)
        
        # 使用安全的文件名，避免编码问题
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        response.headers['Content-Type'] = 'text/csv; charset=utf-8'
        
        return response
        
    except Exception as e:
        print(f"CSV导出异常: {str(e)}")
        print(f"查询参数: query_type={request.args.get('query_type')}, start_date={request.args.get('start_date')}, end_date={request.args.get('end_date')}")
        return jsonify({
            'success': False,
            'message': f'导出失败: {str(e)}'
        }), 500


# 全局变量管理数据采集实例
data_collector_instance = None
progress_queue = queue.Queue()
collection_thread = None

def cleanup_global_resources():
    """清理全局资源"""
    global data_collector_instance, collection_thread
    try:
        if data_collector_instance:
            data_collector_instance.force_cleanup()
            data_collector_instance = None
        
        if collection_thread and collection_thread.is_alive():
            collection_thread.join(timeout=5)
            if collection_thread.is_alive():
                print("⚠️ 数据采集线程未能正常退出")
        
        print("✅ 全局资源清理完成")
    except Exception as e:
        print(f"❌ 清理全局资源异常: {e}")

# 注册程序退出时的清理函数
import atexit
atexit.register(cleanup_global_resources)


@video_active_bp.route('/start-data-collection', methods=['POST'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def start_data_collection():
    """启动数据采集"""
    global data_collector_instance, collection_thread
    
    try:
        if data_collector_instance and data_collector_instance.is_running:
            return jsonify({
                'success': False,
                'message': '数据采集正在运行中'
            }), 400
        
        # 获取参数
        data = request.get_json() or {}
        total_pages = data.get('total_pages', 500)
        batch_size = data.get('batch_size', 20)
        username = data.get('username', 'admin')
        password = data.get('password', 'admin@liandanxia')
        filter_start_date = data.get('filter_start_date')
        filter_end_date = data.get('filter_end_date')
        
        # 清空进度队列
        while not progress_queue.empty():
            try:
                progress_queue.get_nowait()
            except queue.Empty:
                break
        
        # 创建数据采集实例
        def progress_callback(progress_info):
            progress_queue.put(progress_info)
        
        data_collector_instance = VideoDataCollector(
            progress_callback=progress_callback,
            filter_start_date=filter_start_date,
            filter_end_date=filter_end_date
        )
        
        # 在后台线程中启动数据采集
        def collection_worker():
            try:
                # 连接和登录
                progress_queue.put({
                    'message': '🔗 开始连接浏览器...',
                    'level': 'info',
                    'timestamp': datetime.now().strftime('%H:%M:%S')
                })
                
                if not data_collector_instance.connect():
                    progress_queue.put({
                        'message': '❌ 浏览器连接失败，任务结束',
                        'level': 'error',
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    })
                    return
                
                progress_queue.put({
                    'message': '🔐 开始登录系统...',
                    'level': 'info',
                    'timestamp': datetime.now().strftime('%H:%M:%S')
                })
                
                if not data_collector_instance.login(username=username, password=password):
                    progress_queue.put({
                        'message': '❌ 系统登录失败，任务结束',
                        'level': 'error',
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    })
                    return
                
                progress_queue.put({
                    'message': '✅ 连接和登录成功，开始数据采集',
                    'level': 'success',
                    'timestamp': datetime.now().strftime('%H:%M:%S')
                })
                
                # 开始数据采集
                data_collector_instance.start_data_collection(
                    total_pages=total_pages,
                    batch_size=batch_size
                )
                
            except Exception as e:
                progress_queue.put({
                    'message': f'数据采集异常: {str(e)}',
                    'level': 'error',
                    'timestamp': datetime.now().strftime('%H:%M:%S')
                })
            finally:
                # 确保采集结束后重置状态和清理资源
                if data_collector_instance:
                    data_collector_instance.is_running = False
                    data_collector_instance.cleanup()
                    progress_queue.put({
                        'message': '🔚 数据采集任务结束',
                        'level': 'info',
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    })
        
        collection_thread = threading.Thread(target=collection_worker)
        collection_thread.daemon = True  # 设置为守护线程，程序退出时会强制终止
        collection_thread.start()
        
        return jsonify({
            'success': True,
            'message': '数据采集已启动'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'启动数据采集失败: {str(e)}'
        }), 500


@video_active_bp.route('/stop-data-collection', methods=['POST'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def stop_data_collection():
    """停止数据采集"""
    global data_collector_instance
    
    try:
        if not data_collector_instance:
            return jsonify({
                'success': False,
                'message': '没有运行中的数据采集任务'
            }), 400
        
        data_collector_instance.stop_data_collection()
        
        return jsonify({
            'success': True,
            'message': '数据采集停止指令已发送'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'停止数据采集失败: {str(e)}'
        }), 500


@video_active_bp.route('/collection-progress', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def collection_progress():
    """SSE进度推送端点"""
    
    def generate_progress():
        try:
            while True:
                try:
                    # 从队列中获取进度信息，超时1秒
                    progress_info = progress_queue.get(timeout=1)
                    yield f"data: {json.dumps(progress_info)}\n\n"
                except queue.Empty:
                    # 发送心跳保持连接
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now().strftime('%H:%M:%S')})}\n\n"
                    continue
        except GeneratorExit:
            pass
    
    response = Response(
        generate_progress(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Cache-Control'
        }
    )
    
    return response

@video_active_bp.route('/collection-progress-poll', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def collection_progress_poll():
    """轮询方式获取进度消息"""
    messages = []
    
    # 获取队列中的所有消息（非阻塞）
    try:
        while True:
            try:
                progress_info = progress_queue.get_nowait()
                # 跳过心跳消息
                if progress_info.get('type') != 'heartbeat':
                    messages.append(progress_info)
            except queue.Empty:
                break
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取进度失败: {str(e)}'
        }), 500
    
    return jsonify({
        'success': True,
        'messages': messages,
        'count': len(messages),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })


@video_active_bp.route('/collection-status', methods=['GET'])
@login_required
@require_roles(['admin', 'leader', 'employee'])
def collection_status():
    """获取数据采集状态"""
    global data_collector_instance
    
    is_running = False
    has_instance = False
    
    if data_collector_instance:
        has_instance = True
        is_running = data_collector_instance.is_running
    
    return jsonify({
        'success': True,
        'is_running': is_running,
        'has_instance': has_instance,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@video_active_bp.route('/test-status', methods=['GET'])
def test_status():
    """测试状态接口（无需认证）"""
    global data_collector_instance
    
    is_running = False
    has_instance = False
    
    if data_collector_instance:
        has_instance = True
        is_running = data_collector_instance.is_running
    
    return jsonify({
        'success': True,
        'is_running': is_running,
        'has_instance': has_instance,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'message': 'Test endpoint working'
    })
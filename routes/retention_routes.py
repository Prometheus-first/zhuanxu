"""
留存分析路由 - 处理留存分析相关的API请求
"""
from flask import Blueprint, request, jsonify
from auth.middleware import login_required, require_permission
from services.retention_service import RetentionService
import io
import os

retention_bp = Blueprint('retention', __name__, url_prefix='/api/retention')

@retention_bp.route('/upload-data', methods=['POST'])
@login_required
@require_permission('data.process')
def upload_data():
    """上传数据文件并处理"""
    try:
        # 检查是否有文件上传
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'message': '没有上传文件'
            }), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'success': False,
                'message': '没有选择文件'
            }), 400
        
        # 检查文件格式
        allowed_extensions = {'.xlsx', '.xls', '.csv'}
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in allowed_extensions:
            return jsonify({
                'success': False,
                'message': f'不支持的文件格式: {file_ext}，支持的格式: {", ".join(allowed_extensions)}'
            }), 400
        
        # 读取文件内容
        file_content = io.BytesIO(file.read())

        # 获取强制覆盖参数
        force_overwrite = request.form.get('force_overwrite', 'false').lower() == 'true'

        # 调用服务处理数据
        result = RetentionService.process_and_store_data(
            file_path=file.filename,
            file_content=file_content,
            force_overwrite=force_overwrite
        )

        # 确保结果可以JSON序列化
        try:
            import json
            json.dumps(result)  # 测试序列化
        except (TypeError, ValueError) as e:
            print(f"结果序列化失败: {e}")
            return jsonify({
                'success': False,
                'message': '数据处理完成，但返回结果序列化失败'
            }), 500

        # 返回结果
        status_code = 200 if result['success'] else 500
        return jsonify(result), status_code
        
    except Exception as e:
        print(f"上传数据时出错: {str(e)}")  # 添加日志
        return jsonify({
            'success': False,
            'message': f'服务器错误: {str(e)}'
        }), 500

@retention_bp.route('/analyze', methods=['POST'])
@login_required
@require_permission('data.process')
def analyze_retention():
    """进行留存分析"""
    try:
        data = request.json or {}
        
        # 获取日期参数
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # 调用服务进行留存分析
        result = RetentionService.analyze_retention(
            start_date=start_date,
            end_date=end_date
        )
        
        # 返回结果
        status_code = 200 if result['success'] else 500
        return jsonify(result), status_code
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'服务器错误: {str(e)}'
        }), 500

@retention_bp.route('/data-summary', methods=['GET'])
@login_required
@require_permission('data.process')
def get_data_summary():
    """获取数据库中的数据概览"""
    try:
        import pymongo
        from datetime import datetime
        
        # 连接数据库
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        retention_db = client['留存']
        collection = retention_db['数据']
        
        # 获取基本统计信息
        total_records = collection.count_documents({})
        
        if total_records == 0:
            return jsonify({
                'success': True,
                'data': {
                    'total_records': 0,
                    'date_range': None,
                    'message': '数据库中暂无数据，请先上传数据文件'
                }
            })
        
        # 获取日期范围
        earliest_record = collection.find().sort("访问时间", 1).limit(1)
        latest_record = collection.find().sort("访问时间", -1).limit(1)
        
        earliest_date = None
        latest_date = None
        
        for record in earliest_record:
            if record.get('访问时间'):
                earliest_date = record['访问时间'].strftime('%Y-%m-%d')
        
        for record in latest_record:
            if record.get('访问时间'):
                latest_date = record['访问时间'].strftime('%Y-%m-%d')
        
        # 获取每日数据统计
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$访问时间"
                        }
                    },
                    "count": {"$sum": 1},
                    "unique_ips": {"$addToSet": "$访问ip"}
                }
            },
            {
                "$project": {
                    "date": "$_id",
                    "record_count": "$count",
                    "unique_user_count": {"$size": "$unique_ips"}
                }
            },
            {"$sort": {"date": 1}}
        ]
        
        daily_stats = list(collection.aggregate(pipeline))
        
        client.close()
        
        return jsonify({
            'success': True,
            'data': {
                'total_records': total_records,
                'date_range': {
                    'start': earliest_date,
                    'end': latest_date
                },
                'daily_stats': daily_stats
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取数据概览失败: {str(e)}'
        }), 500

"""
Excel相关路由 - 处理Excel文件匹配的HTTP请求
"""
from flask import Blueprint, request, jsonify
from services.excel_match_service import ExcelMatchService
from auth.middleware import login_required, require_permission

# 创建蓝图
excel_bp = Blueprint('excel', __name__, url_prefix='/api')

@excel_bp.route('/save-match-result', methods=['POST'])
@login_required
@require_permission('data.process')
def save_match_result():
    """保存匹配结果到MongoDB"""
    try:
        data = request.json
        
        # 获取数据
        date = data.get('date')
        matched_count = data.get('matchedCount')
        
        # 验证数据
        if not date or matched_count is None:
            return jsonify({
                'success': False,
                'message': '缺少必要参数'
            }), 400
        
        # 调用服务保存数据
        result = ExcelMatchService.save_match_result(date, matched_count)
        
        # 返回响应
        status_code = 200 if result['success'] else 500
        return jsonify(result), status_code
        
    except Exception as e:
        print(f"处理请求时出错: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'服务器错误: {str(e)}'
        }), 500

@excel_bp.route('/get-match-history', methods=['GET'])
@login_required
def get_match_history():
    """获取匹配历史记录"""
    try:
        # 调用服务获取历史记录
        result = ExcelMatchService.get_match_history()
        
        # 返回响应
        status_code = 200 if result['success'] else 500
        return jsonify(result), status_code
        
    except Exception as e:
        print(f"处理请求时出错: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'服务器错误: {str(e)}'
        }), 500 
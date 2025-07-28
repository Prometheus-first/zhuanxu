"""
认证相关路由
"""
from flask import Blueprint, request, jsonify
from auth.services import AuthService
from auth.middleware import login_required

# 创建认证蓝图
auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

@auth_bp.route('/login', methods=['POST'])
def login():
    """用户登录"""
    try:
        data = request.json
        username = data.get('username')
        password = data.get('password')
        
        # 验证参数
        if not username or not password:
            return jsonify({
                'success': False,
                'message': '用户名和密码不能为空'
            }), 400
        
        # 验证用户
        auth_result = AuthService.authenticate_user(username, password)
        if not auth_result['success']:
            return jsonify(auth_result), 401
        
        # 生成令牌
        token_result = AuthService.generate_token(auth_result['user'])
        if not token_result['success']:
            return jsonify(token_result), 500
        
        return jsonify({
            'success': True,
            'message': '登录成功',
            'token': token_result['token'],
            'expires_in': token_result['expires_in'],
            'user': auth_result['user']
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'登录失败: {str(e)}'
        }), 500

@auth_bp.route('/logout', methods=['POST'])
@login_required
def logout():
    """用户登出"""
    # JWT是无状态的，客户端删除token即可
    # 这里可以记录登出日志或执行其他清理操作
    return jsonify({
        'success': True,
        'message': '登出成功'
    })

@auth_bp.route('/verify', methods=['GET'])
@login_required
def verify():
    """验证当前令牌是否有效"""
    from flask import g
    return jsonify({
        'success': True,
        'user': g.current_user
    })

@auth_bp.route('/refresh', methods=['POST'])
@login_required
def refresh_token():
    """刷新令牌"""
    from flask import g
    
    # 获取当前用户信息
    current_user = g.current_user
    
    # 从数据库获取最新的用户信息
    user_data = AuthService.get_user_by_username(current_user['username'])
    if not user_data:
        return jsonify({
            'success': False,
            'message': '用户不存在'
        }), 404
    
    # 构建用户字典
    user_dict = {
        'username': user_data['username'],
        'real_name': user_data['real_name'],
        'role': user_data['role'],
        'permissions': user_data['permissions']
    }
    
    # 生成新令牌
    token_result = AuthService.generate_token(user_dict)
    if not token_result['success']:
        return jsonify(token_result), 500
    
    return jsonify({
        'success': True,
        'message': '令牌刷新成功',
        'token': token_result['token'],
        'expires_in': token_result['expires_in']
    })

@auth_bp.route('/init-users', methods=['POST'])
def init_users():
    """初始化默认用户（仅用于开发）"""
    try:
        # 在生产环境应该禁用此接口
        from config.config import Config
        if not Config.DEBUG:
            return jsonify({
                'success': False,
                'message': '此功能仅在开发环境可用'
            }), 403
        
        AuthService.init_default_users()
        
        return jsonify({
            'success': True,
            'message': '默认用户初始化完成',
            'users': [
                {'username': 'admin', 'password': 'admin123', 'role': '系统管理员'},
                {'username': 'leader1', 'password': 'leader123', 'role': '部门领导'},
                {'username': 'zhangtongyi', 'password': 'zt123456', 'role': '张童义森'}
            ]
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'初始化失败: {str(e)}'
        }), 500 
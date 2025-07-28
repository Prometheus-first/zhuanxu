"""
认证中间件 - 验证请求的身份和权限
"""
from functools import wraps
from flask import request, jsonify, g
from auth.services import AuthService

def login_required(f):
    """需要登录的装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 获取令牌
        token = None
        auth_header = request.headers.get('Authorization')
        
        if auth_header:
            try:
                # Bearer token格式
                token = auth_header.split(' ')[1]
            except IndexError:
                return jsonify({
                    'success': False,
                    'message': '无效的认证头格式'
                }), 401
        
        if not token:
            return jsonify({
                'success': False,
                'message': '缺少认证令牌'
            }), 401
        
        # 验证令牌
        result = AuthService.verify_token(token)
        if not result['success']:
            return jsonify({
                'success': False,
                'message': result['message']
            }), 401
        
        # 将用户信息存储在g对象中
        g.current_user = result['payload']
        
        return f(*args, **kwargs)
    
    return decorated_function

def require_permission(permission):
    """需要特定权限的装饰器"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # 先检查是否登录
            if not hasattr(g, 'current_user'):
                return jsonify({
                    'success': False,
                    'message': '未登录'
                }), 401
            
            # 检查权限
            user_permissions = g.current_user.get('permissions', [])
            if permission not in user_permissions:
                return jsonify({
                    'success': False,
                    'message': f'缺少权限: {permission}'
                }), 403
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    return decorator

def require_role(role):
    """需要特定角色的装饰器"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # 先检查是否登录
            if not hasattr(g, 'current_user'):
                return jsonify({
                    'success': False,
                    'message': '未登录'
                }), 401
            
            # 检查角色
            user_role = g.current_user.get('role')
            if user_role != role:
                # 管理员拥有所有权限
                if user_role != 'admin':
                    return jsonify({
                        'success': False,
                        'message': f'需要角色: {role}'
                    }), 403
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    return decorator

def require_roles(roles):
    """需要多个角色之一的装饰器"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # 先检查是否登录
            if not hasattr(g, 'current_user'):
                return jsonify({
                    'success': False,
                    'message': '未登录'
                }), 401
            
            # 检查角色
            user_role = g.current_user.get('role')
            if user_role not in roles:
                # 管理员拥有所有权限
                if user_role != 'admin':
                    return jsonify({
                        'success': False,
                        'message': f'需要以下角色之一: {", ".join(roles)}'
                    }), 403
            
            return f(*args, **kwargs)
        
        return decorated_function
    
    return decorator 
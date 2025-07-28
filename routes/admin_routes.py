"""
管理员路由 - 用户管理、系统管理等功能
"""
from flask import Blueprint, request, jsonify, g
from auth.middleware import login_required, require_role
from auth.services import AuthService
from auth.models import UserRole
from utils.database import db

# 创建管理员蓝图
admin_bp = Blueprint('admin', __name__, url_prefix='/api/admin')

@admin_bp.route('/users', methods=['GET'])
@login_required
@require_role('admin')
def get_all_users():
    """获取所有用户列表"""
    try:
        # 从数据库获取所有用户（排除密码）
        users = list(db.db.users.find(
            {},
            {'password_hash': 0}  # 只排除密码哈希，其他字段全部返回
        ).sort('created_at', -1))
        
        # 转换ObjectId为字符串
        for user in users:
            user['_id'] = str(user['_id'])
            if user.get('created_at'):
                user['created_at'] = user['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if user.get('last_login'):
                user['last_login'] = user['last_login'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'data': {
                'users': users,
                'total': len(users)
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取用户列表失败: {str(e)}'
        }), 500

@admin_bp.route('/users', methods=['POST'])
@login_required
@require_role('admin')
def create_user():
    """创建新用户"""
    try:
        data = request.json
        
        # 验证必要字段
        required_fields = ['username', 'password', 'real_name', 'role']
        for field in required_fields:
            if not data.get(field):
                return jsonify({
                    'success': False,
                    'message': f'缺少必要字段: {field}'
                }), 400
        
        # 验证角色是否有效
        valid_roles = [role.value for role in UserRole]
        if data['role'] not in valid_roles:
            return jsonify({
                'success': False,
                'message': f'无效的角色，有效角色: {", ".join(valid_roles)}'
            }), 400
        
        # 创建用户
        result = AuthService.create_user(
            username=data['username'],
            password=data['password'],
            real_name=data['real_name'],
            role=data['role']
        )
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': '用户创建成功',
                'data': result.get('user')
            })
        else:
            return jsonify(result), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'创建用户失败: {str(e)}'
        }), 500

@admin_bp.route('/users/<user_id>', methods=['PUT'])
@login_required
@require_role('admin')
def update_user(user_id):
    """更新用户信息"""
    try:
        from bson import ObjectId
        data = request.json
        
        # 构建更新字段
        update_fields = {}
        
        if 'real_name' in data:
            update_fields['real_name'] = data['real_name']
        
        if 'role' in data:
            # 验证角色是否有效
            valid_roles = [role.value for role in UserRole]
            if data['role'] not in valid_roles:
                return jsonify({
                    'success': False,
                    'message': f'无效的角色，有效角色: {", ".join(valid_roles)}'
                }), 400
            update_fields['role'] = data['role']
            
            # 更新权限
            from auth.models import User
            temp_user = User("", "", "", data['role'])
            update_fields['permissions'] = temp_user.permissions
        
        if 'is_active' in data:
            update_fields['is_active'] = bool(data['is_active'])
        
        if 'department' in data:
            update_fields['department'] = data['department']
        
        if not update_fields:
            return jsonify({
                'success': False,
                'message': '没有可更新的字段'
            }), 400
        
        # 更新用户
        result = db.db.users.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': update_fields}
        )
        
        if result.matched_count == 0:
            return jsonify({
                'success': False,
                'message': '用户不存在'
            }), 404
        
        return jsonify({
            'success': True,
            'message': '用户更新成功'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'更新用户失败: {str(e)}'
        }), 500

@admin_bp.route('/users/<user_id>', methods=['DELETE'])
@login_required
@require_role('admin')
def delete_user(user_id):
    """删除用户"""
    try:
        from bson import ObjectId
        
        # 检查是否尝试删除自己
        current_user_id = str(db.db.users.find_one({'username': g.current_user['username']})['_id'])
        if user_id == current_user_id:
            return jsonify({
                'success': False,
                'message': '不能删除自己的账号'
            }), 400
        
        # 删除用户
        result = db.db.users.delete_one({'_id': ObjectId(user_id)})
        
        if result.deleted_count == 0:
            return jsonify({
                'success': False,
                'message': '用户不存在'
            }), 404
        
        return jsonify({
            'success': True,
            'message': '用户删除成功'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'删除用户失败: {str(e)}'
        }), 500

@admin_bp.route('/users/<user_id>/reset-password', methods=['POST'])
@login_required
@require_role('admin')
def reset_user_password(user_id):
    """重置用户密码"""
    try:
        from bson import ObjectId
        from werkzeug.security import generate_password_hash
        
        data = request.json
        new_password = data.get('new_password')
        
        if not new_password:
            return jsonify({
                'success': False,
                'message': '新密码不能为空'
            }), 400
        
        if len(new_password) < 1:
            return jsonify({
                'success': False,
                'message': '密码不能为空'
            }), 400
        
        # 更新密码
        password_hash = generate_password_hash(new_password)
        result = db.db.users.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': {'password_hash': password_hash}}
        )
        
        if result.matched_count == 0:
            return jsonify({
                'success': False,
                'message': '用户不存在'
            }), 404
        
        return jsonify({
            'success': True,
            'message': '密码重置成功'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'重置密码失败: {str(e)}'
        }), 500

@admin_bp.route('/stats', methods=['GET'])
@login_required
@require_role('admin')
def get_system_stats():
    """获取系统统计信息"""
    try:
        # 用户统计
        total_users = db.db.users.count_documents({})
        active_users = db.db.users.count_documents({'is_active': True})
        
        # 角色统计
        role_stats = {}
        for role in UserRole:
            count = db.db.users.count_documents({'role': role.value})
            role_stats[role.value] = count
        
        # 最近登录用户
        recent_logins = list(db.db.users.find(
            {'last_login': {'$exists': True}},
            {'username': 1, 'real_name': 1, 'last_login': 1}
        ).sort('last_login', -1).limit(5))
        
        for user in recent_logins:
            user['_id'] = str(user['_id'])
            if user.get('last_login'):
                user['last_login'] = user['last_login'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'data': {
                'user_stats': {
                    'total': total_users,
                    'active': active_users,
                    'inactive': total_users - active_users
                },
                'role_stats': role_stats,
                'recent_logins': recent_logins
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取统计信息失败: {str(e)}'
        }), 500 
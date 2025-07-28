"""
AI聊天路由 - 为领导提供AI助手对话功能
"""
from flask import Blueprint, request, jsonify, g, Response
from auth.middleware import login_required, require_role
from services.ai_service import ai_service
import asyncio
import json
import requests
from datetime import datetime

# 创建AI聊天蓝图
ai_bp = Blueprint('ai', __name__, url_prefix='/api/ai')

@ai_bp.route('/chat', methods=['POST'])
@login_required
@require_role('leader')  # 只允许领导使用
def chat_with_ai():
    """与AI助手聊天"""
    try:
        data = request.json
        
        # 验证请求数据
        if not data or 'message' not in data:
            return jsonify({
                'success': False,
                'message': '请提供聊天内容'
            }), 400
        
        user_message = data['message'].strip()
        if not user_message:
            return jsonify({
                'success': False,
                'message': '聊天内容不能为空'
            }), 400
        
        # 获取聊天历史（可选）
        chat_history = data.get('history', [])
        
        # 构建消息列表
        messages = []
        
        # 添加历史对话（最多保留最近10轮对话）
        if chat_history:
            recent_history = chat_history[-20:]  # 最多20条消息（10轮对话）
            for msg in recent_history:
                if msg.get('role') in ['user', 'assistant'] and msg.get('content'):
                    messages.append({
                        'role': msg['role'],
                        'content': msg['content']
                    })
        
        # 添加当前用户消息
        messages.append({
            'role': 'user',
            'content': user_message
        })
        
        # 调用AI服务
        # 由于是同步路由，我们需要处理异步调用
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(
            ai_service.chat_completion(messages, g.current_user['username'])
        )
        loop.close()
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': result['message'],
                'tokens_used': result.get('tokens_used', 0),
                'model': result.get('model', ''),
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'success': False,
                'message': result['message'],
                'error': result.get('error', '未知错误')
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'AI助手服务异常，请稍后重试',
            'error': str(e)
        }), 500

@ai_bp.route('/suggestions', methods=['GET'])
@login_required
@require_role('leader')
def get_suggestions():
    """获取建议问题列表"""
    try:
        suggestions = ai_service.get_suggested_questions()
        return jsonify({
            'success': True,
            'suggestions': suggestions
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': '获取建议问题失败',
            'error': str(e)
        }), 500

@ai_bp.route('/status', methods=['GET'])
@login_required
@require_role('leader')
def get_ai_status():
    """获取AI服务状态"""
    try:
        return jsonify({
            'success': True,
            'status': 'online',
            'model': ai_service.model,
            'message': 'AI助手服务正常'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'status': 'offline',
            'message': 'AI助手服务异常',
            'error': str(e)
        }), 500

@ai_bp.route('/stream-chat', methods=['POST'])
@login_required
@require_role('leader')
def stream_chat_with_ai():
    """流式AI聊天"""
    try:
        data = request.json
        
        # 验证请求数据
        if not data or 'message' not in data:
            return jsonify({
                'success': False,
                'message': '请提供聊天内容'
            }), 400
        
        user_message = data['message'].strip()
        if not user_message:
            return jsonify({
                'success': False,
                'message': '聊天内容不能为空'
            }), 400
        
        # 获取聊天历史
        chat_history = data.get('history', [])
        
        # 构建消息列表
        messages = []
        
        # 添加系统提示
        messages.append({
            'role': 'system',
            'content': ai_service.system_prompt
        })
        
        # 添加历史对话
        if chat_history:
            recent_history = chat_history[-20:]
            for msg in recent_history:
                if msg.get('role') in ['user', 'assistant'] and msg.get('content'):
                    messages.append({
                        'role': msg['role'],
                        'content': msg['content']
                    })
        
        # 添加当前用户消息
        messages.append({
            'role': 'user',
            'content': user_message
        })
        
        def generate_stream():
            try:
                # 直接调用API进行流式传输
                headers = {
                    "Authorization": f"Bearer {ai_service.api_key}",
                    "Content-Type": "application/json"
                }
                
                api_data = {
                    "model": ai_service.model,
                    "messages": messages,
                    "max_tokens": 2000,
                    "temperature": 0.7,
                    "stream": True
                }
                
                response = requests.post(
                    ai_service.base_url,
                    headers=headers,
                    json=api_data,
                    timeout=30,
                    stream=True
                )
                
                if response.status_code == 200:
                    for line in response.iter_lines():
                        if line:
                            line_str = line.decode('utf-8')
                            if line_str.startswith('data: '):
                                data_str = line_str[6:]
                                if data_str.strip() == '[DONE]':
                                    yield f"data: {json.dumps({'done': True})}\n\n"
                                    break
                                try:
                                    chunk_data = json.loads(data_str)
                                    if 'choices' in chunk_data and len(chunk_data['choices']) > 0:
                                        delta = chunk_data['choices'][0].get('delta', {})
                                        if 'content' in delta:
                                            yield f"data: {json.dumps({'content': delta['content']})}\n\n"
                                except json.JSONDecodeError:
                                    continue
                else:
                    yield f"data: {json.dumps({'error': f'API请求失败: {response.status_code}'})}\n\n"
                    
            except Exception as e:
                yield f"data: {json.dumps({'error': f'流式请求失败: {str(e)}'})}\n\n"
        
        return Response(
            generate_stream(),
            mimetype='text/plain',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*'
            }
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'AI助手服务异常，请稍后重试',
            'error': str(e)
        }), 500

@ai_bp.route('/clear-history', methods=['POST'])
@login_required
@require_role('leader')
def clear_chat_history():
    """清空聊天历史"""
    try:
        # 这里可以清空数据库中的聊天记录
        # 目前只是返回成功状态，实际清空由前端处理
        return jsonify({
            'success': True,
            'message': '聊天历史已清空'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': '清空聊天历史失败',
            'error': str(e)
        }), 500 
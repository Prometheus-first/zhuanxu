"""
AI聊天服务 - 使用Claude模型为领导提供智能助手
"""
import requests
import json
from typing import List, Dict, Optional

class AIService:
    """AI聊天服务类"""
    
    def __init__(self):
        """初始化AI服务配置"""
        self.api_key = "sk-iQ4dRpEjVpAcF5eHQTYL8pBpwgwOKkqsfdEcoQZz17PmK1Fo"
        self.base_url = "https://api.liandanxia.com/v1/chat/completions"
        self.model = "QWQ-32B"
        
        # 系统提示词 - 为运营部领导量身定制
        self.system_prompt = """你是运营部的AI智能助手颛顼，专门为部门领导提供支持。你具有以下特点：

🎯 **角色定位**：
- 专业的运营管理顾问
- 数据分析专家  
- 团队管理助手
- 业务决策支持

💼 **核心能力**：
- 运营数据分析和解读
- 团队绩效管理建议
- 业务流程优化方案
- 决策支持和风险评估
- 行业趋势分析
- 工作计划制定

🗣️ **交流风格**：
- 简洁专业，重点突出
- 提供具体可行的建议
- 用数据支撑观点
- 关注ROI和效率

请根据用户的问题，提供专业、实用的建议和分析。"""

    async def chat_completion(self, messages: List[Dict[str, str]], user_id: str = None) -> Dict:
        """
        发送聊天请求到Claude模型
        
        Args:
            messages: 聊天消息列表 [{"role": "user", "content": "..."}, ...]
            user_id: 用户ID（用于日志记录）
            
        Returns:
            响应结果字典
        """
        try:
            # 构建完整的消息列表（包含系统提示）
            full_messages = [
                {"role": "system", "content": self.system_prompt}
            ] + messages
            
            # 使用requests直接调用API
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "model": self.model,
                "messages": full_messages,
                "max_tokens": 2000,
                "temperature": 0.7,
                "stream": True  # 启用流式输出
            }
            
            response = requests.post(
                self.base_url,
                headers=headers,
                json=data,
                timeout=30,
                stream=True  # 启用流式响应
            )
            
            if response.status_code == 200:
                # 处理流式响应
                full_message = ""
                tokens_used = 0
                
                for line in response.iter_lines():
                    if line:
                        line_str = line.decode('utf-8')
                        if line_str.startswith('data: '):
                            data_str = line_str[6:]  # 移除 'data: ' 前缀
                            if data_str.strip() == '[DONE]':
                                break
                            try:
                                chunk_data = json.loads(data_str)
                                if 'choices' in chunk_data and len(chunk_data['choices']) > 0:
                                    delta = chunk_data['choices'][0].get('delta', {})
                                    if 'content' in delta:
                                        full_message += delta['content']
                                
                                # 获取token使用信息
                                if 'usage' in chunk_data:
                                    tokens_used = chunk_data['usage'].get('total_tokens', 0)
                            except json.JSONDecodeError:
                                continue
                
                return {
                    "success": True,
                    "message": full_message,
                    "tokens_used": tokens_used,
                    "model": self.model
                }
            else:
                raise Exception(f"API请求失败: {response.status_code} - {response.text}")
            
        except Exception as e:
            return {
                "success": False,
                "error": f"AI服务调用失败: {str(e)}",
                "message": "抱歉，AI助手暂时无法响应，请稍后再试。"
            }
    
    def get_suggested_questions(self) -> List[str]:
        """
        获取建议的问题列表
        
        Returns:
            建议问题列表
        """
        return [
            "分析本月的运营数据表现如何？",
            "如何提高团队工作效率？",
            "制定下季度的工作计划",
            "当前业务有哪些风险点？",
            "如何优化客户满意度？",
            "团队绩效考核建议",
            "行业发展趋势分析",
            "成本控制优化方案"
        ]
    
    def format_business_prompt(self, user_input: str, context: Dict = None) -> str:
        """
        格式化业务相关的提示词
        
        Args:
            user_input: 用户输入
            context: 业务上下文（如用户数据、报表数据等）
            
        Returns:
            格式化后的提示词
        """
        prompt = f"作为运营部AI助手，请针对以下问题提供专业建议：\n\n{user_input}"
        
        if context:
            prompt += f"\n\n当前业务背景：\n{json.dumps(context, ensure_ascii=False, indent=2)}"
        
        return prompt
    
    def is_business_related(self, message: str) -> bool:
        """
        判断消息是否与业务相关
        
        Args:
            message: 用户消息
            
        Returns:
            是否业务相关
        """
        business_keywords = [
            "运营", "管理", "团队", "绩效", "数据", "分析", "计划", "策略",
            "效率", "成本", "收入", "客户", "市场", "业务", "流程", "优化",
            "kpi", "roi", "报表", "统计", "预算", "目标", "风险", "决策"
        ]
        
        message_lower = message.lower()
        return any(keyword in message_lower for keyword in business_keywords)

# 创建全局AI服务实例
ai_service = AIService() 
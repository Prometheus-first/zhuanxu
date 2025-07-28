import pandas as pd
import pymongo
from datetime import datetime
import os

def process_and_store_data(file_path):
    """
    数据处理阶段：从Excel/CSV读取数据，处理合并后存入数据库
    :param file_path: 数据文件路径
    """
    print("\n" + "="*60)
    print("📊 数据处理与存储阶段")
    print("="*60)

    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return False

    # 根据文件扩展名选择读取方式
    file_ext = os.path.splitext(file_path)[1].lower()
    try:
        if file_ext in ['.xlsx', '.xls']:
            print(f"📖 读取Excel文件: {file_path}")
            df1 = pd.read_excel(file_path)
        elif file_ext == '.csv':
            print(f"📖 读取CSV文件: {file_path}")
            df1 = pd.read_csv(file_path)
        else:
            print(f"❌ 不支持的文件格式: {file_ext}")
            return False
    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return False

    print(f"✅ 成功读取数据，共 {len(df1)} 条记录")

    # 连接MongoDB
    try:
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        db = client['留存']  # 数据库名称
        collection = db['数据']  # 集合名称
        print("✅ 成功连接MongoDB数据库")
    except Exception as e:
        print(f"❌ 连接数据库失败: {e}")
        return False

    # 将DataFrame转换为字典列表
    data_records = df1.to_dict('records')

    # 处理数据格式并合并相同用户的记录
    from collections import defaultdict

    print("🔄 开始数据处理...")

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
            '访问时间': data['最早访问时间'],  # 使用最早访问时间
            '地域': region,
            '访问ip': ip,
            '访问日期': datetime.combine(date, datetime.min.time()),  # 转换为datetime对象
            '访问次数': data['访问次数'],
            '访问时长': data['总访问时长'],  # 保持原字段名，便于后续分析
            '总访问时长': data['总访问时长'],  # 同时保留新字段名
            '最早访问时间': data['最早访问时间'],
            '最晚访问时间': data['最晚访问时间'],
            **data['其他信息']  # 展开其他字段
        }
        final_records.append(final_record)

    print(f"原始数据: {len(data_records)} 条")
    print(f"合并后数据: {len(final_records)} 条")
    print(f"合并了 {len(data_records) - len(final_records)} 条重复记录")

    # 显示合并示例（前3个有多次访问的记录）
    print("\n📊 合并示例:")
    merge_examples = [(key, data) for key, data in merged_data.items() if data['访问次数'] > 1]
    if merge_examples:
        for i, ((ip, region, date), data) in enumerate(merge_examples[:3]):
            print(f"\n示例 {i+1}:")
            print(f"  IP: {ip}, 地域: {region}, 日期: {date}")
            print(f"  访问次数: {data['访问次数']} 次")
            print(f"  总访问时长: {data['总访问时长']} 秒")
            print(f"  访问时间段: {data['最早访问时间'].strftime('%H:%M:%S')} - {data['最晚访问时间'].strftime('%H:%M:%S')}")
    else:
        print("  没有发现需要合并的重复记录")

    # 显示访问时长处理统计
    print("\n📈 访问时长处理统计:")
    duration_stats = {'有效时长': 0, '未知转换': 0, '空值转换': 0, '总时长': 0}
    for record in data_records:
        original_duration = record.get('访问时长')
        if original_duration == 0:
            if str(record.get('访问时长', '')).strip() in ['未知', '']:
                duration_stats['未知转换'] += 1
            else:
                duration_stats['空值转换'] += 1
        else:
            duration_stats['有效时长'] += 1
        duration_stats['总时长'] += record.get('访问时长', 0)

    print(f"  有效访问时长记录: {duration_stats['有效时长']} 条")
    print(f"  '未知'转换为0: {duration_stats['未知转换']} 条")
    print(f"  空值转换为0: {duration_stats['空值转换']} 条")
    print(f"  所有记录总时长: {duration_stats['总时长']} 秒")

    # 显示合并后数据的前几条记录
    print("\n📋 合并后数据示例:")
    for i, record in enumerate(final_records[:3]):
        print(f"\n记录 {i+1}:")
        print(f"  访问时间: {record['访问时间']}")
        print(f"  IP: {record['访问ip']}, 地域: {record['地域']}")
        print(f"  访问次数: {record['访问次数']}")
        print(f"  访问时长: {record['访问时长']} 秒")
        print(f"  来源: {record.get('来源', 'N/A')}")
        print(f"  浏览器: {record.get('浏览器', 'N/A')}")

    try:
        # 批量插入合并后的数据到MongoDB
        result = collection.insert_many(final_records)
        print(f"\n✅ 数据插入成功！")
        print(f"共插入 {len(result.inserted_ids)} 条合并后的记录")
        print(f"插入的文档ID示例: {result.inserted_ids[:3]}...")

    except Exception as e:
        print(f"❌ 插入数据时出错: {e}")
        return False

    finally:
        # 关闭连接
        client.close()

    print("\n✅ 数据处理完成！")
    return True

# ==================== 留存分析功能 ====================

def analyze_retention(start_date=None, end_date=None):
    """
    分析用户留存情况
    :param start_date: 开始日期，格式：'2025-07-17' 或 datetime对象
    :param end_date: 结束日期，格式：'2025-07-27' 或 datetime对象
    :return: 留存分析结果
    """
    try:
        # 重新连接数据库进行查询
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        db = client['留存']
        collection = db['数据']

        # 处理日期参数
        query_filter = {}
        if start_date or end_date:
            time_filter = {}
            if start_date:
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                time_filter["$gte"] = start_date
            if end_date:
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
                time_filter["$lte"] = end_date
            query_filter["访问时间"] = time_filter

        # 获取指定日期范围内的数据，按时间排序
        all_data = list(collection.find(query_filter).sort("访问时间", 1))

        if not all_data:
            print("指定日期范围内没有数据")
            return

        date_range_info = ""
        if start_date or end_date:
            start_str = start_date.strftime('%Y-%m-%d') if start_date else "开始"
            end_str = end_date.strftime('%Y-%m-%d') if end_date else "结束"
            date_range_info = f"（日期范围: {start_str} 到 {end_str}）"

        print(f"\n开始分析留存情况{date_range_info}...")
        print(f"分析数据量: {len(all_data)} 条")

        # 按日期分组数据
        from collections import defaultdict
        daily_users = defaultdict(set)  # {日期: {(ip, 地域)}}

        for record in all_data:
            if record.get('访问时间') and record.get('访问ip') and record.get('地域'):
                # 提取日期（去掉时分秒）
                visit_date = record['访问时间'].date()
                user_key = (record['访问ip'], record['地域'])
                daily_users[visit_date].add(user_key)

        # 分析留存 - 比较每一天与后续所有天的留存情况
        retention_results = []
        dates = sorted(daily_users.keys())

        for i, base_date in enumerate(dates):
            base_users = daily_users[base_date]

            # 与后续每一天进行比较
            for j in range(i + 1, len(dates)):
                target_date = dates[j]
                target_users = daily_users[target_date]
                days_diff = (target_date - base_date).days

                # 计算留存用户
                retained_users = base_users.intersection(target_users)
                retention_rate = len(retained_users) / len(base_users) if base_users else 0

                result = {
                    '基准日期': base_date,
                    '对比日期': target_date,
                    '间隔天数': days_diff,
                    '基准日用户数': len(base_users),
                    '对比日用户数': len(target_users),
                    '留存用户数': len(retained_users),
                    '留存率': f"{retention_rate:.2%}",
                    '留存用户详情': list(retained_users)
                }
                retention_results.append(result)

        # 输出结果
        print(f"\n=== 留存分析结果 ===")
        for result in retention_results:
            print(f"\n基准日期: {result['基准日期']}")
            print(f"对比日期: {result['对比日期']} (间隔{result['间隔天数']}天)")
            print(f"基准日用户数: {result['基准日用户数']}")
            print(f"留存用户数: {result['留存用户数']}")
            print(f"留存率: {result['留存率']}")

            if result['留存用户详情']:
                print("留存用户详情:")
                for ip, region in result['留存用户详情'][:5]:  # 只显示前5个
                    print(f"  - IP: {ip}, 地域: {region}")
                if len(result['留存用户详情']) > 5:
                    print(f"  ... 还有{len(result['留存用户详情']) - 5}个用户")

        client.close()
        return retention_results

    except Exception as e:
        print(f"留存分析出错: {e}")
        return None



def custom_retention_analysis():
    """
    留存分析 - 交互式选择日期范围
    """
    print("\n" + "="*60)
    print("🔍 留存分析")
    print("="*60)

    try:
        # 获取日期范围
        print("\n📅 设置日期范围 (格式: YYYY-MM-DD，留空表示不限制)")
        start_date = input("开始日期 (如: 2025-07-17): ").strip()
        end_date = input("结束日期 (如: 2025-07-27): ").strip()

        start_date = start_date if start_date else None
        end_date = end_date if end_date else None

        # 整体留存分析
        print(f"\n🚀 开始整体留存分析...")
        results = analyze_retention(start_date=start_date, end_date=end_date)
        return results

    except ValueError as e:
        print(f"❌ 输入格式错误: {e}")
        return None
    except Exception as e:
        print(f"❌ 分析过程出错: {e}")
        return None

# 主程序执行
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🎯 数据处理与留存分析系统")
    print("="*60)

    try:
        print("\n请选择功能:")
        print("1. 数据处理与存储 (从Excel/CSV读取数据并存入数据库)")
        print("2. 留存分析 (从数据库读取数据进行留存分析)")
        print("3. 完整流程 (先处理数据，再进行分析)")

        choice = input("\n请输入选择 (1/2/3): ").strip()

        if choice == "1":
            # 数据处理与存储
            file_path = input("\n请输入数据文件路径 (如: 717.csv 或 data.xlsx): ").strip()
            if not file_path:
                file_path = "717.csv"  # 默认文件

            success = process_and_store_data(file_path)
            if success:
                print("\n🎉 数据处理与存储完成！")
            else:
                print("\n❌ 数据处理失败！")

        elif choice == "2":
            # 留存分析
            print("\n🔍 开始留存分析...")
            custom_retention_analysis()

        elif choice == "3":
            # 完整流程
            file_path = input("\n请输入数据文件路径 (如: 717.csv 或 data.xlsx): ").strip()
            if not file_path:
                file_path = "717.csv"  # 默认文件

            print("\n📊 第一步：数据处理与存储")
            success = process_and_store_data(file_path)

            if success:
                print("\n🔍 第二步：留存分析")
                custom_retention_analysis()
            else:
                print("\n❌ 数据处理失败，无法进行留存分析！")

        else:
            print("❌ 无效选择！")

    except KeyboardInterrupt:
        print("\n\n👋 用户取消操作")
    except Exception as e:
        print(f"\n❌ 执行出错: {e}")

    print("\n" + "="*60)
    print("✅ 程序执行完成！")
    print("="*60)

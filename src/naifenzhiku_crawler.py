#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import time
import math
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime
import random
import glob
import argparse

class NaifenzhikuCrawler:
    """奶粉之库数据爬虫"""
    
    def __init__(self, resume_from_page=0):
        """
        初始化爬虫
        参数:
            resume_from_page: 从哪一页开始爬取，0表示从头开始
        """
        # 基本URL和请求头
        self.base_url = "https://data.naifenzhiku.com/index/powder/index?page={}"
        
        # 请求头信息
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "authorization": "",
            "dm-ip": "104.16.53.111",
            "dm-mcode": "2a04f2a8b748b8e44f62c0aca754ab47",
            "dnt": "1",
            "origin": "https://naifenzhiku.com",
            "platform": "4",
            "priority": "u=1, i",
            "referer": "https://naifenzhiku.com/",
            "sec-ch-ua": "\"Chromium\";v=\"135\", \"Not-A.Brand\";v=\"8\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }
        
        # 随机User-Agent列表
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/119.0"
        ]
        
        # 随机IP地址列表
        self.ip_addresses = [
            "104.16.53.111",
            "104.17.96.22",
            "198.41.242.47",
            "205.198.76.188"
        ]
        
        # 输出目录
        self.output_dir = "data"
        
        # 恢复爬取相关
        self.resume_from_page = resume_from_page
        self.resume_file = f"{self.output_dir}/products_partial.json"
        
        # 存储所有产品数据
        self.all_products = []
        
        # 第一页的数据格式，用于后续页面的格式判断
        self.first_page_format = None
        
        # 配置爬虫参数
        self.retry_count = 5      # 重试次数
        self.retry_delay = 3      # 重试延迟（秒）
        self.page_delay = 1       # 页面间延迟（秒）
        self.connect_timeout = 10  # 连接超时（秒）
        self.read_timeout = 30    # 读取超时（秒）
        
        # 确保数据目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)

    def fetch_page(self, page):
        """
        获取指定页的数据
        参数:
            page: 页码
        返回:
            JSON格式的响应数据或None(如果请求失败)
        """
        url = self.base_url.format(page)
        
        max_retries = self.retry_count
        retry_delay = self.retry_delay
        
        for attempt in range(max_retries):
            # 定义curl_command变量，避免未定义错误
            curl_command = None
            
            try:
                print(f"正在获取第{page}页数据，第{attempt+1}/{max_retries}次尝试...")
                
                # 每次请求随机更换User-Agent和IP
                self.headers["user-agent"] = random.choice(self.user_agents)
                if "dm-ip" in self.headers:
                    self.headers["dm-ip"] = random.choice(self.ip_addresses)
                
                # 打印完整URL和请求头（仅用于第一次尝试）
                if attempt == 0:
                    print(f"请求URL: {url}")
                    print(f"请求头: {json.dumps(self.headers, indent=2, ensure_ascii=False)[:200]}...")
                
                # 生成可执行的curl命令用于调试
                curl_command = self.generate_curl_command(url, self.headers)
                
                # 设置超时参数，避免请求卡住
                session = requests.Session()
                
                # 设置TCP保持活动状态
                adapter = requests.adapters.HTTPAdapter(
                    max_retries=3,  # 连接级别的重试
                    pool_connections=10,
                    pool_maxsize=10,
                    pool_block=False
                )
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                
                # 使用session发起请求
                response = session.get(
                    url, 
                    headers=self.headers, 
                    timeout=(self.connect_timeout, self.read_timeout),
                    stream=False  # 关闭流式传输，避免管道断开
                )
                
                print(f"响应状态码: {response.status_code}")
                
                # 检查响应是否成功
                if response.status_code == 200:
                    # 先检查响应内容是否为空
                    if not response.text or response.text.strip() == '':
                        print("警告: 响应内容为空")
                        self.save_failed_curl(page, curl_command, "响应内容为空")
                        if attempt < max_retries - 1:
                            delay = retry_delay * (1 + random.random()) * (1 + attempt*0.5)  # 随尝试次数增加延迟
                            print(f"等待 {delay:.2f} 秒后重试...")
                            time.sleep(delay)
                            continue
                        return None
                        
                    try:
                        # 解析JSON
                        data = response.json()
                        
                        # 仅显示部分内容
                        data_preview = json.dumps(data, indent=2, ensure_ascii=False)
                        if len(data_preview) > 300:
                            data_preview = data_preview[:300] + "..."
                        print(f"响应内容预览: {data_preview}")
                        
                        # 如果是第一页，保存数据格式
                        if page == 1:
                            self.first_page_format = self.detect_response_format(data)
                            print(f"检测到首页数据格式: {self.first_page_format}")
                        
                        # 根据数据格式尝试处理
                        # 检查数据结构，看看是否有预期的字段
                        if data and isinstance(data, dict):
                            # 如果是成功响应，返回数据
                            if 'code' in data and data['code'] == 0 and 'data' in data:
                                print("数据获取成功")
                                return data
                            # 兼容旧版API格式
                            elif 'normal' in data or 'topping' in data:
                                print("获取到旧版API格式数据")
                                return data
                            # 如果响应中包含产品数据类型的字段，即使格式不完全匹配也返回
                            elif self.contains_product_data(data):
                                print("获取到可能包含产品数据的响应")
                                return data
                            # 如果是错误响应，但仍然是合法JSON
                            else:
                                print(f"警告: API返回错误代码或非预期数据结构: {data.get('code', 'unknown')}")
                                if 'msg' in data:
                                    print(f"错误信息: {data.get('msg', 'no message')}")
                                # 记录失败的请求
                                error_message = data.get('msg', f"错误代码: {data.get('code', 'unknown')}")
                                self.save_failed_curl(page, curl_command, error_message)
                                
                                # 如果是最后一次尝试，返回当前数据
                                if attempt == max_retries - 1:
                                    return data
                                
                                # 否则继续重试
                                delay = retry_delay * (1 + random.random()) * (1 + attempt*0.5)
                                print(f"等待 {delay:.2f} 秒后重试...")
                                time.sleep(delay)
                                continue
                        else:
                            print(f"警告: 响应不是有效的JSON对象")
                            self.save_failed_curl(page, curl_command, "响应不是有效的JSON对象")
                            if attempt < max_retries - 1:
                                delay = retry_delay * (1 + random.random()) * (1 + attempt*0.5)
                                print(f"等待 {delay:.2f} 秒后重试...")
                                time.sleep(delay)
                                continue
                            return None
                            
                    except json.JSONDecodeError as e:
                        print(f"JSON解析错误: {e}")
                        print(f"响应内容: {response.text[:500]}...")
                        self.save_failed_curl(page, curl_command, f"JSON解析错误: {e}")
                        if attempt < max_retries - 1:
                            delay = retry_delay * (1 + random.random()) * (1 + attempt*0.5)
                            print(f"等待 {delay:.2f} 秒后重试...")
                            time.sleep(delay)
                            continue
                        return None
                else:
                    print(f"请求失败，状态码: {response.status_code}")
                    print(f"响应内容: {response.text[:500]}...")
                    self.save_failed_curl(page, curl_command, f"HTTP错误: {response.status_code}")
                
                # 如果这不是最后一次尝试，则等待一段时间后重试
                if attempt < max_retries - 1:
                    delay = retry_delay * (1 + attempt) * (1 + random.random())  # 随着尝试次数增加延迟
                    print(f"等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
            
            except requests.exceptions.Timeout:
                print(f"请求超时")
                self.save_failed_curl(page, curl_command, "请求超时")
                if attempt < max_retries - 1:
                    delay = retry_delay * (1 + attempt) * (1 + random.random())
                    print(f"等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
            
            except requests.exceptions.ConnectionError as e:
                print(f"连接错误: {e}")
                self.save_failed_curl(page, curl_command, f"连接错误: {e}")
                
                # 对于连接错误，增加更长的等待时间
                if attempt < max_retries - 1:
                    delay = retry_delay * (2 + attempt*2) * (1 + random.random())
                    print(f"连接错误，等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
                    
                # 如果是最后一次尝试，尝试手动处理curl请求
                elif curl_command:
                    print("尝试通过执行curl命令获取数据...")
                    try:
                        # 保存curl到临时文件
                        temp_curl_file = f"logs/temp_curl_{page}.sh"
                        with open(temp_curl_file, 'w') as f:
                            f.write(f"{curl_command} > logs/curl_output_{page}.json")
                        
                        # 给文件添加执行权限
                        os.chmod(temp_curl_file, 0o755)
                        
                        # 执行curl命令
                        os.system(f"bash {temp_curl_file}")
                        
                        # 检查是否成功获取数据
                        curl_output_file = f"logs/curl_output_{page}.json"
                        if os.path.exists(curl_output_file) and os.path.getsize(curl_output_file) > 0:
                            print(f"成功通过curl获取数据")
                            with open(curl_output_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            return data
                    except Exception as curl_e:
                        print(f"通过curl获取数据失败: {curl_e}")
            
            except requests.exceptions.RequestException as e:
                print(f"请求异常: {e}")
                self.save_failed_curl(page, curl_command, f"请求异常: {e}")
                if attempt < max_retries - 1:
                    delay = retry_delay * (1 + attempt) * (1 + random.random())
                    print(f"等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
            
            except BrokenPipeError as e:
                print(f"管道断开错误: {e}")
                self.save_failed_curl(page, curl_command, f"管道断开错误: {e}")
                
                # 对于Broken Pipe错误，尝试直接通过curl获取数据
                if curl_command:
                    print("尝试通过执行curl命令获取数据...")
                    try:
                        # 保存curl到临时文件
                        temp_curl_file = f"logs/temp_curl_{page}.sh"
                        with open(temp_curl_file, 'w') as f:
                            f.write(f"{curl_command} > logs/curl_output_{page}.json")
                        
                        # 给文件添加执行权限
                        os.chmod(temp_curl_file, 0o755)
                        
                        # 执行curl命令
                        os.system(f"bash {temp_curl_file}")
                        
                        # 检查是否成功获取数据
                        curl_output_file = f"logs/curl_output_{page}.json"
                        if os.path.exists(curl_output_file) and os.path.getsize(curl_output_file) > 0:
                            print(f"成功通过curl获取数据")
                            with open(curl_output_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            return data
                    except Exception as curl_e:
                        print(f"通过curl获取数据失败: {curl_e}")
                
                if attempt < max_retries - 1:
                    delay = retry_delay * (2 + attempt) * (1 + random.random())
                    print(f"管道断开错误，等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
            
            except Exception as e:
                print(f"未预期的异常: {e}")
                self.save_failed_curl(page, curl_command, f"未预期的异常: {e}")
                if attempt < max_retries - 1:
                    delay = retry_delay * (1 + attempt) * (1 + random.random())
                    print(f"等待 {delay:.2f} 秒后重试...")
                    time.sleep(delay)
        
        # 所有重试都失败了，最后尝试直接通过curl获取数据
        if curl_command:
            print("尝试通过执行curl命令获取数据...")
            try:
                # 保存curl到临时文件
                temp_curl_file = f"logs/temp_curl_{page}.sh"
                os.makedirs("logs", exist_ok=True)
                with open(temp_curl_file, 'w') as f:
                    f.write(f"{curl_command} > logs/curl_output_{page}.json")
                
                # 给文件添加执行权限
                os.chmod(temp_curl_file, 0o755)
                
                # 执行curl命令
                os.system(f"bash {temp_curl_file}")
                
                # 检查是否成功获取数据
                curl_output_file = f"logs/curl_output_{page}.json"
                if os.path.exists(curl_output_file) and os.path.getsize(curl_output_file) > 0:
                    print(f"成功通过curl获取数据")
                    with open(curl_output_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    return data
            except Exception as curl_e:
                print(f"通过curl获取数据失败: {curl_e}")
        
        return None

    def generate_curl_command(self, url, headers):
        """
        生成等效的curl命令，用于调试
        参数:
            url: 请求URL
            headers: 请求头
        返回:
            curl命令字符串
        """
        curl_parts = [f"curl '{url}'"]
        
        # 添加请求头
        for key, value in headers.items():
            # 处理引号，确保命令正确
            value = value.replace("'", "'\\''") if isinstance(value, str) else value
            curl_parts.append(f"-H '{key}: {value}'")
        
        return " \\\n  ".join(curl_parts)

    def save_failed_curl(self, page, curl_command, error_message):
        """
        保存失败的curl命令到文件，用于后续调试
        参数:
            page: 页码
            curl_command: curl命令
            error_message: 错误信息
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 确保logs目录存在
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # 保存curl命令到文件
        filename = f"logs/failed_curl_page{page}_{timestamp}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"# 错误信息: {error_message}\n")
            f.write(f"# 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 页码: {page}\n\n")
            f.write(f"{curl_command}\n")
        
        print(f"已保存失败的curl命令到 {filename}")

    def get_total_pages(self, data):
        """
        根据API返回的数据计算总页数
        参数:
            data: API返回的第一页数据
        返回:
            总页数, 总条目数, 每页条目数
        """
        try:
            total_items = 0
            items_per_page = 0
            
            # 直接从最外层获取total和limit
            if 'total' in data and 'limit' in data:
                total_items = data['total']
                items_per_page = data['limit']
                print(f"从外层数据获取: 总数据量={total_items}, 每页限制={items_per_page}")
            # 从data字段获取total和per_page
            elif 'data' in data and 'total' in data['data']:
                total_items = data['data']['total']
                items_per_page = data['data'].get('per_page', 20)  # 新API默认每页20条
                print(f"从data字段获取: 总数据量={total_items}, 每页限制={items_per_page}")
            else:
                print("数据结构中找不到总数量信息")
                # 尝试从数据列表长度估算
                if 'data' in data and 'list' in data['data'] and isinstance(data['data']['list'], list):
                    list_length = len(data['data']['list'])
                    print(f"当前页面数据条数: {list_length}")
                    # 假设有20页数据
                    total_items = list_length * 20
                    items_per_page = list_length
                    print(f"估算: 总数据量≈{total_items}, 每页限制≈{items_per_page}")
                elif 'normal' in data and isinstance(data['normal'], list):
                    list_length = len(data['normal'])
                    print(f"当前页面normal数据条数: {list_length}")
                    total_items = 3642  # 根据用户提供的信息
                    items_per_page = 30  # 根据用户提供的信息
                    print(f"根据已知信息: 总数据量=3642, 每页限制=30")
                else:
                    return 0, 0, 0
            
            if total_items <= 0 or items_per_page <= 0:
                print("总商品数或每页商品数异常，使用默认值")
                total_items = 3642  # 使用默认值
                items_per_page = 30  # 使用默认值
            
            total_pages = math.ceil(total_items / items_per_page)
            print(f"总商品数: {total_items}, 每页商品数: {items_per_page}, 总页数: {total_pages}")
            return total_pages, total_items, items_per_page
        
        except Exception as e:
            print(f"计算总页数时出错: {e}")
            # 使用默认值
            return 122, 3642, 30  # 根据用户提供的信息估算

    def process_product_data(self, page_data, current_page):
        """
        处理产品数据，提取所需字段
        参数:
            page_data: 包含产品信息的JSON数据
            current_page: 当前页码
        返回:
            处理后的产品数据列表
        """
        result = []
        
        if not page_data:
            print("没有数据可处理")
            return result
            
        # 打印数据结构以便调试
        print(f"数据结构键列表: {list(page_data.keys())}")
        
        # 尝试兼容多种API结构
        product_list = []
        data_format = "unknown"
        
        # 新API结构: data > list
        if 'data' in page_data and isinstance(page_data['data'], dict) and 'list' in page_data['data']:
            print("使用新API结构处理数据")
            product_list = page_data['data']['list']
            data_format = "new_api"
            
        # 旧API结构: normal + topping
        elif 'normal' in page_data:
            print("使用旧API结构处理数据")
            normal_products = page_data.get('normal', [])
            # 确保topping_products始终是列表，防止None值导致连接错误
            topping_products = page_data.get('topping', []) or []
            product_list = normal_products + topping_products
            data_format = "old_api"
        
        # 旧API结构变体: 只有normal没有topping
        elif 'topping' in page_data:
            print("使用旧API结构变体处理数据(只有topping)")
            # 确保normal_products始终是列表，防止None值导致连接错误
            normal_products = page_data.get('normal', []) or []
            topping_products = page_data.get('topping', [])
            product_list = normal_products + topping_products
            data_format = "old_api_topping_only"
            
        # 尝试检测数组类型数据
        elif any(isinstance(page_data.get(key), list) and len(page_data.get(key)) > 0 for key in page_data.keys()):
            print("尝试从其他列表字段提取数据")
            for key in page_data.keys():
                if isinstance(page_data.get(key), list) and len(page_data.get(key)) > 0:
                    # 检查第一个元素是否像产品数据
                    first_item = page_data.get(key)[0]
                    if isinstance(first_item, dict) and ('id' in first_item or 'name' in first_item):
                        print(f"从 '{key}' 字段提取产品列表")
                        product_list = page_data.get(key)
                        data_format = f"list_in_{key}"
                        break
        
        # 检查顶层是否直接是产品数组
        elif isinstance(page_data, list) and len(page_data) > 0:
            print("数据本身是产品数组")
            product_list = page_data
            data_format = "direct_list"
            
        # 如果数据结构完全不符合预期，尝试深度搜索产品列表
        else:
            print("尝试深度搜索产品列表")
            product_list = self.deep_search_products(page_data)
            if product_list:
                print(f"通过深度搜索找到{len(product_list)}个产品")
                data_format = "deep_search"
            else:
                print("未知的数据结构，无法提取产品列表")
                print(f"数据预览: {json.dumps(page_data, indent=2, ensure_ascii=False)[:500]}...")
                return result
        
        # 确保product_list是列表类型
        if product_list is None:
            print("警告: 产品列表为None，使用空列表代替")
            product_list = []
        elif not isinstance(product_list, list):
            print(f"警告: 产品列表类型异常({type(product_list).__name__})，尝试转换为列表")
            try:
                product_list = list(product_list)
            except:
                print("转换失败，使用空列表")
                product_list = []
        
        if not product_list:
            print("产品列表为空")
            return result
            
        print(f"找到{len(product_list)}条产品记录，格式: {data_format}")
        
        for product in product_list:
            try:
                # 跳过非字典类型数据
                if not isinstance(product, dict):
                    continue
                    
                # 创建通用产品信息结构，确保至少有id和name
                product_info = {
                    'id': product.get('id', ''),
                    'name': product.get('name', '')
                }
                
                # 如果缺少关键字段，可能不是产品数据
                if not product_info['id'] and not product_info['name']:
                    continue
                
                # 尝试添加不同版本API的可能字段
                # 图片字段
                if 'image' in product:
                    product_info['thumbnail'] = product['image']
                elif 'thumbnail' in product:
                    product_info['thumbnail'] = product['thumbnail']
                elif 'img' in product:
                    product_info['thumbnail'] = product['img']
                elif 'picture' in product:
                    product_info['thumbnail'] = product['picture']
                    
                # 点击数字段
                if 'clicks' in product:
                    product_info['click_count'] = product['clicks']
                elif 'm_click' in product:
                    product_info['click_count'] = product['m_click']
                elif 'click_count' in product:
                    product_info['click_count'] = product['click_count']
                elif 'views' in product:
                    product_info['click_count'] = product['views']
                    
                # 来源字段
                if 'source' in product:
                    product_info['source'] = product['source']
                elif 'm_source' in product:
                    product_info['source'] = product['m_source']
                elif 'origin' in product:
                    product_info['source'] = product['origin']
                    
                # 价格字段
                if 'price' in product:
                    product_info['price'] = product['price']
                elif 'amount' in product:
                    product_info['price'] = product['amount']
                elif 'cost' in product:
                    product_info['price'] = product['cost']
                    
                # 地区字段
                if 'country' in product:
                    product_info['area'] = product['country']
                elif 'area' in product:
                    product_info['area'] = product['area']
                elif 'region' in product:
                    product_info['area'] = product['region']
                    
                # 标签字段
                if 'tag' in product:
                    product_info['tag'] = product['tag']
                elif 'label' in product:
                    product_info['tag'] = product['label']
                elif 'tags' in product and isinstance(product['tags'], list):
                    product_info['tag'] = ','.join(product['tags'])
                
                # 添加其他可能的重要信息
                for key, value in product.items():
                    if key not in product_info and not isinstance(value, (dict, list)):
                        product_info[key] = value
                
                result.append(product_info)
            except Exception as e:
                print(f"处理产品数据时出错: {e}")
                continue
        
        return result
        
    def deep_search_products(self, data, max_depth=3, current_depth=0):
        """
        递归搜索可能包含产品数据的列表
        参数:
            data: 要搜索的数据
            max_depth: 最大搜索深度
            current_depth: 当前搜索深度
        返回:
            产品列表或空列表
        """
        # 防止过深递归
        if current_depth > max_depth:
            return []
            
        # 如果是字典，遍历所有值
        if isinstance(data, dict):
            for key, value in data.items():
                # 如果值是列表，检查是否可能是产品列表
                if isinstance(value, list) and len(value) > 0:
                    # 检查第一个元素是否像产品数据
                    if isinstance(value[0], dict) and self.is_likely_product(value[0]):
                        return value
                
                # 递归搜索更深层次
                result = self.deep_search_products(value, max_depth, current_depth + 1)
                if result:
                    return result
                    
        # 如果是列表，检查是否可能是产品列表
        elif isinstance(data, list) and len(data) > 0:
            # 检查第一个元素是否像产品数据
            if isinstance(data[0], dict) and self.is_likely_product(data[0]):
                return data
                
            # 递归搜索列表中的每个元素
            for item in data:
                result = self.deep_search_products(item, max_depth, current_depth + 1)
                if result:
                    return result
                    
        return []
        
    def is_likely_product(self, item):
        """
        判断一个字典是否可能是产品数据
        参数:
            item: 待检查的字典
        返回:
            布尔值: 是否可能是产品
        """
        # 检查关键字段
        product_indicators = ['id', 'name', 'price', 'image', 'thumbnail']
        matched_fields = 0
        
        for field in product_indicators:
            if field in item:
                matched_fields += 1
                
        # 如果匹配了两个或更多关键字段，可能是产品
        return matched_fields >= 2

    def crawl_pages(self, start_page=1, max_pages=0):
        """
        爬取指定数量的页面
        参数:
            start_page: 起始页码
            max_pages: 最大页数
        """
        self.all_products = []
        end_page = start_page + max_pages - 1 if max_pages > 0 else 999999
        
        # 如果从中间页面开始，加载已保存的数据
        if start_page > 1 and os.path.exists(self.resume_file):
            try:
                with open(self.resume_file, 'r', encoding='utf-8') as f:
                    self.all_products = json.load(f)
                print(f"已加载{len(self.all_products)}条产品数据")
            except Exception as e:
                print(f"加载已保存数据失败: {e}")
                
        # 创建进度条
        try:
            # 如果是指定页数，直接使用指定的页数
            if max_pages > 0:
                total_pages = max_pages
            else:
                # 否则尝试获取总页数
                first_page_data = self.fetch_page(1)
                if first_page_data:
                    total_pages, _, _ = self.get_total_pages(first_page_data)
                else:
                    total_pages = 200
                    
            pbar = tqdm(total=total_pages, desc="爬取进度", unit="页")
            
            # 如果从中间页开始，更新进度条
            if start_page > 1:
                pbar.update(start_page - 1)
        except Exception as e:
            print(f"创建进度条失败: {e}")
            pbar = None
            
        try:
            current_page = start_page
            empty_page_count = 0
            max_empty_pages = 3
            
            while current_page <= end_page and empty_page_count < max_empty_pages:
                # 计算本次延迟时间
                if current_page > start_page:
                    delay = self.page_delay * (1 + random.random())
                    if current_page % 5 == 0:
                        delay += random.uniform(1, 3)
                    print(f"等待{delay:.2f}秒后继续...")
                    time.sleep(delay)
                
                # 获取当前页的数据
                page_data = self.fetch_page(current_page)
                
                if page_data:
                    try:
                        # 处理当前页的产品数据
                        products = self.process_product_data(page_data, current_page)
                        
                        if products and len(products) > 0:
                            empty_page_count = 0
                            self.all_products.extend(products)
                            print(f"第{current_page}页: 获取到{len(products)}个产品")
                            
                            # 每爬取10页保存一次数据
                            if current_page % 10 == 0:
                                self.save_products_data(is_final=False)
                                self.save_resume_info(current_page + 1)
                        else:
                            empty_page_count += 1
                            print(f"第{current_page}页: 未获取到产品数据 (连续空页计数: {empty_page_count}/{max_empty_pages})")
                    except Exception as e:
                        print(f"处理第{current_page}页数据时出错: {e}")
                        error_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第{current_page}页错误: {str(e)}"
                        self.log_error(error_message)
                else:
                    empty_page_count += 1
                    print(f"第{current_page}页: 获取数据失败 (连续空页计数: {empty_page_count}/{max_empty_pages})")
                    if empty_page_count >= 2:
                        extra_delay = random.uniform(5, 10)
                        print(f"连续获取失败，额外等待{extra_delay:.2f}秒...")
                        time.sleep(extra_delay)
                
                # 更新进度条
                if pbar:
                    pbar.update(1)
                    pbar.set_description(f"爬取进度 (已获取{len(self.all_products)}个产品)")
                
                current_page += 1
                
                # 如果达到指定页数，退出循环
                if max_pages > 0 and current_page > end_page:
                    print(f"已达到指定的{max_pages}页，停止爬取")
                    break
            
            # 关闭进度条
            if pbar:
                pbar.close()
            
            print(f"爬取完成，共获取{len(self.all_products)}个产品")
            
            # 强制保存最终数据，确保即使只爬取一页也会保存
            if self.all_products:
                self.save_products_data(is_final=True)
            
            return self.all_products
            
        except KeyboardInterrupt:
            print("用户中断爬取")
            # 保存当前进度
            self.save_products_data(is_final=False)
            self.save_resume_info(current_page)
            
            # 关闭进度条
            if pbar:
                pbar.close()
                
            return self.all_products
            
        except Exception as e:
            print(f"爬取过程中出现异常: {e}")
            # 保存当前进度
            self.save_products_data(is_final=False)
            self.save_resume_info(current_page)
            
            # 关闭进度条
            if pbar:
                pbar.close()
                
            # 记录错误信息
            error_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 爬取过程异常: {str(e)}"
            self.log_error(error_message)
            
            return self.all_products

    def crawl_all_products(self):
        """
        爬取所有产品数据
        """
        self.all_products = []
        
        # 如果存在已保存的进度，则从上次中断的地方继续
        start_page = 1
        
        if self.resume_from_page > 0:
            start_page = self.resume_from_page
            print(f"从第{start_page}页继续爬取...")
            
            # 加载已保存的数据
            if os.path.exists(self.resume_file):
                try:
                    with open(self.resume_file, 'r', encoding='utf-8') as f:
                        self.all_products = json.load(f)
                    print(f"已加载{len(self.all_products)}条产品数据")
                except Exception as e:
                    print(f"加载已保存数据失败: {e}")
        
        # 创建进度条
        try:
            # 获取第一页数据来计算总页数
            first_page_data = self.fetch_page(1)
            if first_page_data:
                total_pages, _, _ = self.get_total_pages(first_page_data)
            else:
                total_pages = 200  # 如果无法获取第一页，使用默认值
                print("无法获取首页数据，使用默认页数: 200")
                
            if total_pages <= 0:
                total_pages = 200  # 假设最大页数，后面会根据实际情况调整
                
            pbar = tqdm(total=total_pages, desc="爬取进度", unit="页")
            
            # 如果从中间页开始，更新进度条
            if start_page > 1:
                pbar.update(start_page - 1)
        except Exception as e:
            print(f"创建进度条失败: {e}")
            total_pages = 200
            pbar = None
        
        try:
            current_page = start_page
            empty_page_count = 0
            max_empty_pages = 3  # 连续遇到3个空页面则认为爬取完成
            
            while current_page <= total_pages and empty_page_count < max_empty_pages:
                # 计算本次延迟时间
                if current_page > start_page:
                    # 为每个页面间添加随机延迟，避免被检测为爬虫
                    delay = self.page_delay * (1 + random.random())
                    # 如果页码是5的倍数，增加一些额外的延迟
                    if current_page % 5 == 0:
                        delay += random.uniform(1, 3)
                    print(f"等待{delay:.2f}秒后继续...")
                    time.sleep(delay)
                
                # 获取当前页的数据
                page_data = self.fetch_page(current_page)
                
                if page_data:
                    try:
                        # 处理当前页的产品数据
                        products = self.process_product_data(page_data, current_page)
                        
                        if products and len(products) > 0:
                            empty_page_count = 0  # 重置空页面计数
                            self.all_products.extend(products)
                            print(f"第{current_page}页: 获取到{len(products)}个产品")
                            
                            # 每爬取10页保存一次数据
                            if current_page % 10 == 0:
                                self.save_products_data(is_final=False)
                                self.save_resume_info(current_page + 1)  # 保存下一页作为恢复点
                        else:
                            empty_page_count += 1
                            print(f"第{current_page}页: 未获取到产品数据 (连续空页计数: {empty_page_count}/{max_empty_pages})")
                    except Exception as e:
                        print(f"处理第{current_page}页数据时出错: {e}")
                        error_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 第{current_page}页错误: {str(e)}"
                        self.log_error(error_message)
                        # 即使出错也继续下一页
                else:
                    empty_page_count += 1
                    print(f"第{current_page}页: 获取数据失败 (连续空页计数: {empty_page_count}/{max_empty_pages})")
                    # 如果连续多次获取数据失败，可能是被封禁，增加等待时间
                    if empty_page_count >= 2:
                        extra_delay = random.uniform(5, 10)
                        print(f"连续获取失败，额外等待{extra_delay:.2f}秒...")
                        time.sleep(extra_delay)
                
                # 更新进度条
                if pbar:
                    pbar.update(1)
                    pbar.set_description(f"爬取进度 (已获取{len(self.all_products)}个产品)")
                
                current_page += 1
            
            # 关闭进度条
            if pbar:
                pbar.close()
            
            # 保存最终数据
            print(f"爬取完成，共获取{len(self.all_products)}个产品")
            self.save_products_data(is_final=True)
            
            # 清理临时文件
            self.cleanup_temp_files()
            
            return self.all_products
                
        except KeyboardInterrupt:
            print("用户中断爬取")
            # 保存当前进度
            self.save_products_data(is_final=False)
            self.save_resume_info(current_page)
            print(f"已保存爬取进度到第{current_page}页，下次运行可以从此处继续")
            
            # 关闭进度条
            if pbar:
                pbar.close()
            
            return self.all_products
        
        except Exception as e:
            print(f"爬取过程中出现异常: {e}")
            # 保存当前进度
            self.save_products_data(is_final=False)
            self.save_resume_info(current_page)
            print(f"已保存爬取进度到第{current_page}页，下次运行可以从此处继续")
            
            # 关闭进度条
            if pbar:
                pbar.close()
            
            # 记录错误信息
            error_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 爬取过程异常: {str(e)}"
            self.log_error(error_message)
            
            return self.all_products
            
    def cleanup_temp_files(self):
        """
        清理临时文件，仅保留最终数据文件
        """
        print("开始清理临时文件...")
        
        # 要删除的文件类型
        temp_patterns = [
            "logs/temp_curl_*.sh",
            "logs/failed_curl_page*.txt",
            "data/resume_info_*.json",  # 添加中间恢复点文件
            "data/naifenzhiku_products_20*.json"  # 添加中间数据文件
        ]
        
        # 排除最终数据文件
        final_patterns = [
            "data/naifenzhiku_products_final_*.json",
            "data/naifenzhiku_products_final_*.csv"
        ]
        
        # 获取最终文件列表
        final_files = []
        for pattern in final_patterns:
            final_files.extend(glob.glob(pattern))
        
        # 如果爬取成功完成，可以选择删除以下临时文件
        if len(self.all_products) > 0:
            temp_patterns.extend([
                "logs/curl_output_*.json"
            ])
        
        files_removed = 0
        
        for pattern in temp_patterns:
            for file_path in glob.glob(pattern):
                # 跳过最终文件
                if file_path in final_files:
                    continue
                    
                try:
                    os.remove(file_path)
                    files_removed += 1
                except Exception as e:
                    print(f"删除文件 {file_path} 失败: {e}")
        
        print(f"临时文件清理完成，共删除 {files_removed} 个文件")
        
        # 如果日志目录为空，可以考虑删除目录
        try:
            if os.path.exists("logs") and not os.listdir("logs"):
                os.rmdir("logs")
                print("已删除空的logs目录")
        except Exception as e:
            print(f"删除logs目录失败: {e}")
            
    def save_products_data(self, is_final=False):
        """
        保存产品数据到文件
        参数:
            is_final: 是否是最终数据
        """
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.output_dir}/naifenzhiku_products_{timestamp}.json"
        
        if is_final:
            filename = f"{self.output_dir}/naifenzhiku_products_final_{timestamp}.json"
            
            # 同时生成CSV文件
            try:
                self.save_to_csv(filename.replace('.json', '.csv'))
            except Exception as e:
                print(f"生成CSV文件失败: {e}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.all_products, f, ensure_ascii=False, indent=2)
        
        print(f"已保存产品数据到 {filename}")
        return filename
        
    def save_to_csv(self, csv_filename):
        """
        将产品数据保存为CSV格式
        参数:
            csv_filename: CSV文件名
        """
        if not self.all_products:
            print("没有数据可保存")
            return False
            
        try:
            # 确定所有可能的字段
            all_fields = set()
            for product in self.all_products:
                all_fields.update(product.keys())
                
            # 过滤掉复杂结构字段
            csv_fields = []
            for field in all_fields:
                # 分析第一个包含该字段的产品
                for product in self.all_products:
                    if field in product and product[field] is not None:
                        if not isinstance(product[field], (dict, list)):
                            csv_fields.append(field)
                        break
            
            # 创建DataFrame并保存为CSV
            data_for_csv = [{field: product.get(field, '') for field in csv_fields} for product in self.all_products]
            df = pd.DataFrame(data_for_csv)
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            print(f"已生成CSV文件: {csv_filename}")
            return True
        except Exception as e:
            print(f"生成CSV文件时出错: {e}")
            return False

    def save_resume_info(self, next_page):
        """
        保存爬取进度信息到文件
        参数:
            next_page: 下次爬取的页码
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.output_dir}/resume_info_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({'next_page': next_page}, f)
        
        print(f"已保存爬取进度到 {filename}")

    def log_error(self, error_message):
        """
        记录错误信息到日志文件
        参数:
            error_message: 错误信息
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 确保logs目录存在
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # 将错误信息记录到日志文件
        with open('logs/crawler_errors.log', 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {error_message}\n")
            f.write("-" * 80 + "\n")

    def detect_response_format(self, data):
        """
        检测响应数据的格式类型
        参数:
            data: API返回的数据
        返回:
            格式类型: 'new_api', 'old_api', 或 'unknown'
        """
        if 'data' in data and isinstance(data['data'], dict) and 'list' in data['data']:
            return 'new_api'
        elif 'normal' in data or 'topping' in data:
            return 'old_api'
        else:
            return 'unknown'
    
    def contains_product_data(self, data):
        """
        检查数据是否包含产品特征字段
        参数:
            data: API返回的数据
        返回:
            布尔值: 是否可能包含产品数据
        """
        # 产品数据中常见的字段
        product_fields = ['id', 'name', 'price', 'image', 'thumbnail', 'source', 'area', 'country']
        
        # 检查顶层字段
        for field in product_fields:
            if field in data:
                return True
                
        # 检查data中的字段
        if 'data' in data and isinstance(data['data'], dict):
            for field in product_fields:
                if field in data['data']:
                    return True
            
            # 检查data.list中的字段
            if 'list' in data['data'] and isinstance(data['data']['list'], list) and len(data['data']['list']) > 0:
                first_item = data['data']['list'][0]
                if isinstance(first_item, dict):
                    for field in product_fields:
                        if field in first_item:
                            return True
        
        # 检查normal中的字段
        if 'normal' in data and isinstance(data['normal'], list) and len(data['normal']) > 0:
            first_item = data['normal'][0]
            if isinstance(first_item, dict):
                for field in product_fields:
                    if field in first_item:
                        return True
        
        return False

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="奶粉之库产品数据爬虫")
    parser.add_argument("--resume", type=int, default=0, help="从指定页码继续爬取，默认从头开始")
    parser.add_argument("--clean", action="store_true", help="爬取完成后清理临时文件")
    parser.add_argument("--keep-temp", action="store_true", help="保留中间临时文件")
    parser.add_argument("--pages", type=int, default=0, help="指定爬取的页数，0表示爬取所有页")
    
    # 解析命令行参数
    args = parser.parse_args()
    
    print("=" * 50)
    print("奶粉之库产品数据爬虫启动")
    print("=" * 50)
    
    # 初始化爬虫并开始爬取
    crawler = NaifenzhikuCrawler(resume_from_page=args.resume)
    
    print(f"配置信息：")
    print(f"- 重试次数: {crawler.retry_count}")
    print(f"- 重试延迟: {crawler.retry_delay}秒")
    print(f"- 页面延迟: {crawler.page_delay}秒")
    print(f"- 连接超时: {crawler.connect_timeout}秒")
    print(f"- 读取超时: {crawler.read_timeout}秒")
    print(f"- 清理临时文件: {'否' if args.keep_temp else '是'}")
    if args.pages > 0:
        print(f"- 爬取页数: {args.pages}页")
    
    # 开始爬取
    if args.resume > 0:
        print(f"从第{args.resume}页继续爬取")
    else:
        print("从第1页开始爬取")
    
    # 根据是否指定页数调用不同的方法
    if args.pages > 0:
        crawler.crawl_pages(start_page=args.resume or 1, max_pages=args.pages)
    else:    
        crawler.crawl_all_products()
    
    if crawler.all_products:
        crawler.save_products_data(is_final=True)
        print(f"爬取完成！共获取 {len(crawler.all_products)} 条产品记录")
        
        # 根据参数决定是否清理临时文件
        if args.clean or not args.keep_temp:
            crawler.cleanup_temp_files()
    else:
        print("爬取完成，但没有获取到任何产品数据")

if __name__ == "__main__":
    main() 
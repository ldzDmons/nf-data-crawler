#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import time
import os
import random
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import argparse
import logging
import sys
from pathlib import Path

class NaifenzhikuMoreDetailCrawler:
    """奶粉智库产品额外详情爬虫"""
    
    def __init__(
        self, 
        product_file=None, 
        output_dir="data", 
        retry_count=3, 
        retry_delay=3, 
        delay_range=(1, 2),
        logger=None,
        username=None,
        password=None,
        auth_token=None,
        config_file=None
    ):
        """
        初始化爬虫
        参数:
            product_file: 包含产品ID的输入文件（JSON或CSV）
            output_dir: 输出目录
            retry_count: 重试次数
            retry_delay: 重试延迟
            delay_range: 请求延迟范围(最小秒数, 最大秒数)
            logger: 日志对象
            username: 奶粉智库用户名(手机号)
            password: 奶粉智库密码
            auth_token: 直接提供的授权token
            config_file: 配置文件路径
        """
        # 创建输出目录和日志目录
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(parents=True, exist_ok=True)
        
        # 设置日志
        self.logger = logger or self.setup_logger()
        
        # 加载配置文件
        config = {}
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.logger.info(f"已从配置文件 {config_file} 加载配置")
            except Exception as e:
                self.logger.error(f"加载配置文件时出错: {e}")
        
        # 配置优先级：直接参数 > 配置文件
        nfzk_config = config.get('naifenzhiku', {})
        
        # 其他初始化
        self.product_file = product_file
        self.output_dir = output_dir or config.get('output_dir', 'data')
        self.retry_count = retry_count or nfzk_config.get('retry_count', 3)
        self.retry_delay = retry_delay or nfzk_config.get('retry_delay', 3)
        self.delay_range = delay_range or tuple(nfzk_config.get('delay_range', (1, 2)))
        self.username = username or nfzk_config.get('username')
        self.password = password or nfzk_config.get('password')
        self.auth_token = auth_token or ""
        
        # 接口URL
        self.login_url = "https://data.naifenzhiku.com/index/login/login"
        self.more_detail_url = "https://data.naifenzhiku.com/index/powder/detailMore"
        
        # 请求头信息
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "authorization": self.auth_token,
            "dm-ip": "205.198.76.188",
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
        
        # 存储所有产品额外详情数据
        self.all_more_details = []
        
        # 如果提供了auth_token
        if self.auth_token:
            self.logger.info(f"使用提供的授权token: {self.auth_token[:20]}...")
        # 否则尝试登录获取token
        elif self.username and self.password:
            self.logger.info(f"尝试使用账号 {self.username} 登录")
            self.login()
        else:
            self.logger.warning("未提供登录信息，将无法获取需要授权的数据")
    
    def setup_logger(self):
        """设置日志配置"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        log_file = f"logs/more_detail_crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logger = logging.getLogger("MoreDetailCrawler")
        logger.setLevel(logging.DEBUG)  # 设置为DEBUG级别以捕获所有日志
        
        # 清除已有的处理器
        if logger.handlers:
            logger.handlers.clear()
            
        # 文件处理器 - 记录所有级别日志
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)
        
        # 控制台处理器 - 只显示INFO及以上级别
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(console_handler)
        
        return logger
    
    def load_products(self):
        """
        从文件加载产品ID列表
        返回:
            产品ID列表
        """
        if not self.product_file or not os.path.exists(self.product_file):
            self.logger.error(f"输入文件 {self.product_file} 不存在")
            return []
        
        self.logger.info(f"从 {self.product_file} 加载产品数据")
        
        try:
            # 判断文件类型
            if self.product_file.endswith('.json'):
                with open(self.product_file, 'r', encoding='utf-8') as f:
                    products = json.load(f)
                
                # 提取产品ID
                product_ids = [str(product.get('id', '')) for product in products if product.get('id')]
                
            elif self.product_file.endswith('.csv'):
                df = pd.read_csv(self.product_file, encoding='utf-8')
                
                # 检查是否存在id列
                if 'id' in df.columns:
                    product_ids = [str(product_id) for product_id in df['id'].tolist() if product_id]
                else:
                    self.logger.error("CSV文件中未找到'id'列")
                    return []
            else:
                self.logger.error(f"不支持的文件格式: {self.product_file}")
                return []
            
            self.logger.info(f"成功加载 {len(product_ids)} 个产品ID")
            return product_ids
            
        except Exception as e:
            self.logger.error(f"加载产品数据时出错: {e}")
            return []
    
    def fetch_more_detail(self, product_id):
        """
        获取产品额外详情
        参数:
            product_id: 产品ID
        返回:
            产品额外详情字典
        """
        # 检查是否有授权token
        if not self.auth_token and self.username and self.password:
            self.logger.info("没有有效的授权token，尝试重新登录")
            self.login()
        
        # 如果没有授权token，记录警告
        if not self.auth_token:
            self.logger.warning("未提供登录信息或登录失败，接口可能需要授权")
        
        params = {
            "product_id": product_id
        }
        
        for attempt in range(self.retry_count):
            try:
                self.logger.info(f"正在获取产品 {product_id} 的额外详情 (第 {attempt+1}/{self.retry_count} 次尝试)")
                
                # 随机选择一个User-Agent和IP
                self.headers["user-agent"] = random.choice(self.user_agents)
                self.headers["dm-ip"] = random.choice(self.ip_addresses)
                
                # 发送请求
                response = requests.get(
                    self.more_detail_url, 
                    params=params,
                    headers=self.headers, 
                    timeout=(10, 30)
                )
                
                # 记录响应状态和内容（用于调试）
                self.logger.debug(f"响应状态码: {response.status_code}")
                self.logger.debug(f"响应内容: {response.text[:500]}...")
                
                # 检查响应状态
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        # 记录完整响应数据（用于调试）
                        debug_file = f"logs/debug_more_detail_{product_id}.json"
                        with open(debug_file, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        
                        # 检查是否需要登录
                        if 'status' in data and data['status'] == 303 and data.get('mesg') == '请先登录':
                            self.logger.warning("接口返回需要登录，尝试重新登录")
                            if self.username and self.password and self.login():
                                # 登录成功，重试本次请求
                                continue
                            else:
                                self.logger.error("登录失败或未提供登录信息，无法获取详情")
                                return {'id': product_id, '额外详情状态': '需要登录'}
                        
                        # 检查是否有id字段，这表示返回的是直接的详情数据
                        if 'id' in data and str(data['id']) == str(product_id):
                            self.logger.info(f"成功获取产品 {product_id} 的额外详情")
                            return self.process_more_detail(data, product_id)
                        # 检查老格式接口返回
                        elif 'code' in data and data['code'] == 0 and 'data' in data:
                            self.logger.info(f"成功获取产品 {product_id} 的额外详情(老格式)")
                            return self.process_more_detail(data, product_id)
                        else:
                            error_msg = data.get('msg', '未知错误')
                            self.logger.warning(f"接口返回错误: {error_msg}")
                            
                            # 如果是产品不存在，创建基本空数据结构返回
                            if '不存在' in error_msg or error_msg == '未知错误':
                                self.logger.info(f"产品 {product_id} 可能不存在，将返回基本结构")
                                return {'id': product_id, '额外详情状态': '无数据'}
                    except json.JSONDecodeError:
                        self.logger.warning(f"响应不是有效的JSON: {response.text[:200]}...")
                else:
                    self.logger.warning(f"请求失败，状态码: {response.status_code}, 响应: {response.text[:200]}...")
            
            except requests.exceptions.Timeout:
                self.logger.warning(f"请求超时")
            
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"连接错误: {e}")
            
            except Exception as e:
                self.logger.warning(f"请求异常: {e}")
            
            # 如果不是最后一次尝试，等待后重试
            if attempt < self.retry_count - 1:
                delay = self.retry_delay * (attempt + 1) * (1 + random.random())
                self.logger.info(f"等待 {delay:.2f} 秒后重试...")
                time.sleep(delay)
        
        self.logger.error(f"获取产品 {product_id} 的额外详情失败，已达到最大重试次数")
        # 返回基本结构，避免空数据
        return {'id': product_id, '额外详情状态': '获取失败'}
    
    def process_more_detail(self, data, product_id):
        """
        处理额外详情数据
        参数:
            data: API返回的数据
            product_id: 产品ID
        返回:
            处理后的额外详情数据
        """
        try:
            # 提取所需的额外详情字段
            more_detail = {'id': product_id}
            
            # 检查是否是直接的详情数据格式
            if 'id' in data and str(data['id']) == str(product_id):
                # 直接处理新格式的数据
                
                # 处理配方评价
                if 'fg_comment' in data:
                    more_detail['配方评价'] = data['fg_comment']
                
                # 处理配料表
                if 'mixture' in data:
                    more_detail['配料表'] = data['mixture']
                
                # 处理营养成分
                if 'nutrient' in data and isinstance(data['nutrient'], list):
                    nutrients = {}
                    for item in data['nutrient']:
                        if isinstance(item, dict) and 'ingredient_name' in item and 'content' in item:
                            name = item['ingredient_name']
                            content = item.get('content', '')
                            unit = item.get('unit', '')
                            desc = item.get('desc', '')
                            nutrients[name] = {
                                '含量': content,
                                '单位': unit,
                                '描述': desc
                            }
                    more_detail['营养成分'] = nutrients
                
                # 处理其他字段
                for key, value in data.items():
                    if key not in ['id', 'fg_comment', 'mixture', 'nutrient'] and value:
                        more_detail[f"详情_{key}"] = value
            
            # 老格式数据处理
            elif 'data' in data and isinstance(data['data'], dict):
                api_data = data['data']
                
                # 处理基本详情信息
                if 'info' in api_data and isinstance(api_data['info'], dict):
                    info = api_data['info']
                    
                    # 提取必要字段
                    for key, value in info.items():
                        # 将API返回的字段添加到详情中
                        more_detail[f"更多_{key}"] = value
                
                # 处理配方特点
                if 'formula' in api_data and isinstance(api_data['formula'], list):
                    formula_features = []
                    for item in api_data['formula']:
                        if isinstance(item, dict) and 'name' in item:
                            formula_features.append(item['name'])
                    
                    more_detail['配方特点'] = formula_features
                
                # 处理产品优点
                if 'features' in api_data and isinstance(api_data['features'], list):
                    product_features = []
                    for item in api_data['features']:
                        if isinstance(item, dict) and 'title' in item and 'content' in item:
                            feature = {
                                '标题': item['title'],
                                '内容': item['content']
                            }
                            product_features.append(feature)
                    
                    more_detail['产品优点'] = product_features
                
                # 处理用户评价
                if 'comments' in api_data and isinstance(api_data['comments'], dict):
                    comments = api_data['comments']
                    if 'list' in comments and isinstance(comments['list'], list):
                        user_comments = []
                        for item in comments['list']:
                            if isinstance(item, dict):
                                comment = {
                                    '用户': item.get('nickname', '匿名用户'),
                                    '评分': item.get('score', 0),
                                    '内容': item.get('content', ''),
                                    '时间': item.get('create_time', '')
                                }
                                user_comments.append(comment)
                        
                        more_detail['用户评价'] = user_comments
                    
                    # 评分汇总
                    if 'total' in comments and isinstance(comments['total'], dict):
                        more_detail['评分汇总'] = comments['total']
            
            return more_detail
            
        except Exception as e:
            self.logger.error(f"处理额外详情数据时出错: {e}")
            # 保存原始数据用于调试
            error_file = f"logs/error_more_detail_{product_id}.json"
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存错误数据到 {error_file}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {'id': product_id, '额外详情状态': '处理错误'}
    
    def crawl_all_more_details(self):
        """
        爬取所有产品的额外详情
        返回:
            产品额外详情列表
        """
        # 加载产品ID
        product_ids = self.load_products()
        if not product_ids:
            self.logger.error("未加载到任何产品ID")
            return []
        
        # 开始爬取
        self.logger.info(f"开始爬取 {len(product_ids)} 个产品的额外详情")
        self.all_more_details = []
        
        try:
            with tqdm(total=len(product_ids), desc="爬取额外详情", unit="产品") as pbar:
                for i, product_id in enumerate(product_ids):
                    # 添加随机延迟
                    if i > 0:
                        delay = random.uniform(*self.delay_range)
                        if i % 10 == 0:  # 每10个请求增加额外延迟
                            delay += random.uniform(1, 3)
                        time.sleep(delay)
                    
                    # 获取产品额外详情
                    more_detail = self.fetch_more_detail(product_id)
                    
                    if more_detail:
                        self.all_more_details.append(more_detail)
                        
                        # 每爬取10个产品保存一次
                        if (i + 1) % 10 == 0 or (i + 1) == len(product_ids):
                            self.save_more_details(is_final=(i + 1) == len(product_ids))
                    
                    # 更新进度条
                    pbar.update(1)
                    pbar.set_description(f"爬取额外详情 (已获取 {len(self.all_more_details)} 个)")
            
            # 确保即使空数据也会保存
            if len(self.all_more_details) == 0:
                self.logger.warning("未获取到任何产品额外详情，将保存空文件")
                self.save_more_details(is_final=True)
            else:
                self.logger.info(f"爬取完成，共获取 {len(self.all_more_details)} 个产品额外详情")
                
            return self.all_more_details
            
        except KeyboardInterrupt:
            self.logger.warning("用户中断爬取")
            self.save_more_details(is_final=False)
            return self.all_more_details
            
        except Exception as e:
            self.logger.error(f"爬取过程中出现异常: {e}")
            self.save_more_details(is_final=False)
            return self.all_more_details
    
    def save_more_details(self, is_final=False):
        """
        保存产品额外详情到文件
        参数:
            is_final: 是否是最终数据
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 构建文件名
        prefix = "final_" if is_final else ""
        json_filename = f"{self.output_dir}/naifenzhiku_more_details_{prefix}{timestamp}.json"
        
        # 保存JSON格式
        try:
            # 即使列表为空也保存文件
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(self.all_more_details, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存产品额外详情到 {json_filename}")
            
            # 如果是最终数据且有数据要保存，则创建一个额外的标准命名的文件，方便流程识别
            if is_final:
                standard_name = f"{self.output_dir}/naifenzhiku_more_details_final_latest.json"
                with open(standard_name, 'w', encoding='utf-8') as f:
                    json.dump(self.all_more_details, f, ensure_ascii=False, indent=2)
                self.logger.info(f"已保存最新产品额外详情到 {standard_name}")
        except Exception as e:
            self.logger.error(f"保存JSON文件时出错: {e}")
        
        return True
    
    def merge_with_main_data(self, main_data_file):
        """
        将额外详情与主数据合并
        参数:
            main_data_file: 主数据文件路径
        返回:
            合并后的数据列表
        """
        if not main_data_file or not os.path.exists(main_data_file):
            self.logger.error(f"主数据文件 {main_data_file} 不存在")
            return None
        
        self.logger.info(f"开始将额外详情与主数据 {main_data_file} 合并")
        
        try:
            # 加载主数据
            with open(main_data_file, 'r', encoding='utf-8') as f:
                main_data = json.load(f)
            self.logger.info(f"成功加载 {len(main_data)} 条主数据记录")
            
            # 创建ID到额外详情的映射
            more_detail_map = {detail['id']: detail for detail in self.all_more_details if 'id' in detail}
            self.logger.info(f"创建了 {len(more_detail_map)} 个产品ID到额外详情的映射")
            
            # 合并数据
            merged_data = []
            with tqdm(total=len(main_data), desc="合并数据", unit="产品") as pbar:
                for product in main_data:
                    product_id = str(product.get('id', ''))
                    
                    if product_id in more_detail_map:
                        # 合并主数据和额外详情
                        merged_product = product.copy()
                        # 添加额外详情字段
                        for key, value in more_detail_map[product_id].items():
                            if key != 'id':  # 跳过ID字段
                                merged_product[key] = value
                        
                        merged_data.append(merged_product)
                    else:
                        # 如果没有额外详情，只使用主数据
                        merged_data.append(product)
                    
                    pbar.update(1)
            
            self.logger.info(f"数据合并完成，共 {len(merged_data)} 条记录")
            
            # 保存合并后的数据
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            merged_file = f"{self.output_dir}/naifenzhiku_full_data_{timestamp}.json"
            merged_csv = f"{self.output_dir}/naifenzhiku_full_data_{timestamp}.csv"
            
            # 保存JSON格式
            with open(merged_file, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存合并数据到 {merged_file}")
            
            # 保存CSV格式
            # 将复杂字段展平处理，以便CSV能正确显示
            df_data = []
            for item in merged_data:
                flat_item = {}
                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        flat_item[key] = json.dumps(value, ensure_ascii=False)
                    else:
                        flat_item[key] = value
                df_data.append(flat_item)
            
            df = pd.DataFrame(df_data)
            df.to_csv(merged_csv, index=False, encoding='utf-8')
            self.logger.info(f"已保存合并数据到 {merged_csv}")
            
            return merged_data
            
        except Exception as e:
            self.logger.error(f"合并数据时出错: {e}")
            return None
    
    def login(self):
        """登录获取授权token"""
        self.logger.info(f"尝试使用账号 {self.username} 登录奶粉智库")
        
        # 登录数据
        login_data = {
            "tel": self.username,
            "password": self.password
        }
        
        # 设置登录请求头
        login_headers = self.headers.copy()
        login_headers["content-type"] = "application/json;charset=UTF-8"
        login_headers.pop("authorization", None)  # 移除authorization头
        
        try:
            # 发送登录请求
            response = requests.post(
                self.login_url,
                data=json.dumps(login_data),  # 使用json.dumps确保与curl一致
                headers=login_headers,
                timeout=(10, 30)
            )
            
            self.logger.info(f"登录响应状态码: {response.status_code}")
            self.logger.info(f"登录响应完整内容: {response.text}")
            
            # 检查响应
            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    self.logger.error(f"无法解析登录响应JSON: {response.text}")
                    return False
                
                # 保存响应以便调试
                debug_file = f"logs/login_response.json"
                with open(debug_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                self.logger.info(f"已保存登录响应到 {debug_file}")
                
                # 检查登录是否成功
                if data.get('status') == 1 and data.get('mesg') == '登录成功' and 'token' in data:
                    self.auth_token = data['token']
                    self.headers["authorization"] = self.auth_token
                    self.logger.info(f"登录成功，已获取授权token: {self.auth_token[:20]}...")
                    return True
                else:
                    error_msg = data.get('mesg', '未知错误')
                    self.logger.error(f"登录失败: {error_msg}")
            else:
                self.logger.error(f"登录请求失败，状态码: {response.status_code}")
        
        except Exception as e:
            self.logger.error(f"登录过程中出现异常: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        
        return False

    def set_auth_token(self, token):
        """
        手动设置授权token
        参数:
            token: 授权token
        """
        self.auth_token = token
        self.headers["authorization"] = self.auth_token
        self.logger.info(f"已手动设置授权token: {self.auth_token[:20]}...")

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="奶粉智库产品额外详情爬虫")
    parser.add_argument("--input", "-i", type=str, required=True, help="包含产品ID的输入文件(JSON或CSV)")
    parser.add_argument("--output", "-o", type=str, default="data", help="输出目录，默认为'data'")
    parser.add_argument("--min-delay", type=float, default=1.0, help="最小请求延迟(秒)，默认为1.0秒")
    parser.add_argument("--max-delay", type=float, default=3.0, help="最大请求延迟(秒)，默认为3.0秒")
    parser.add_argument("--merge", "-m", type=str, help="要合并的主数据文件路径")
    parser.add_argument("--username", "-u", type=str, help="奶粉智库账号(手机号)")
    parser.add_argument("--password", "-p", type=str, help="奶粉智库密码")
    parser.add_argument("--token", "-t", type=str, help="直接提供的授权token")
    parser.add_argument("--token-file", "-tf", type=str, help="包含授权token的文件路径")
    parser.add_argument("--config", "-c", type=str, default="config.json", help="配置文件路径，默认为'config.json'")
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 处理token参数
    auth_token = args.token
    if not auth_token and args.token_file:
        try:
            with open(args.token_file, 'r') as f:
                token_data = json.load(f)
                if isinstance(token_data, dict) and 'token' in token_data:
                    auth_token = token_data['token']
                    print(f"已从文件加载授权token: {auth_token[:20]}...")
                else:
                    print(f"无法从文件加载token，文件内容格式不符")
        except Exception as e:
            print(f"读取token文件时出错: {e}")
    
    print("=" * 50)
    print("奶粉智库产品额外详情爬虫启动")
    print("=" * 50)
    
    # 检查配置文件
    config_file = args.config
    if os.path.exists(config_file):
        print(f"使用配置文件: {config_file}")
    else:
        print(f"警告: 配置文件 {config_file} 不存在")
    
    # 初始化爬虫
    crawler = NaifenzhikuMoreDetailCrawler(
        product_file=args.input,
        output_dir=args.output,
        delay_range=(args.min_delay, args.max_delay),
        username=args.username,
        password=args.password,
        auth_token=auth_token,
        config_file=config_file
    )
    
    # 开始爬取
    more_details = crawler.crawl_all_more_details()
    
    if more_details:
        print(f"爬取完成！共获取 {len(more_details)} 个产品额外详情")
        
        # 如果指定了合并文件，则进行合并
        if args.merge:
            merged_data = crawler.merge_with_main_data(args.merge)
            if merged_data:
                print(f"数据合并完成！共 {len(merged_data)} 条记录")
            else:
                print("数据合并失败！")
    else:
        print("爬取完成，但没有获取到任何产品额外详情")

if __name__ == "__main__":
    main() 
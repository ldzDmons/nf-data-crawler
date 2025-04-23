#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import time
import os
import random
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm
import argparse
import logging
import re

class NaifenzhikuDetailCrawler:
    """奶粉之库产品详情爬虫"""
    
    def __init__(self, input_file=None, output_dir="data", delay_range=(1, 3)):
        """
        初始化爬虫
        参数:
            input_file: 包含产品ID的输入文件
            output_dir: 输出目录
            delay_range: 请求延迟范围(最小秒数, 最大秒数)
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.delay_range = delay_range
        
        # 详情页URL模板
        self.detail_url_template = "https://naifenzhiku.com/powder/detail-{}.html"
        
        # 请求头信息
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-ch-ua": "\"Chromium\";v=\"116\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }
        
        # 随机User-Agent列表
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/119.0"
        ]
        
        # 存储所有产品详情数据
        self.all_product_details = []
        
        # 配置爬虫参数
        self.retry_count = 3
        self.retry_delay = 2
        
        # 设置日志
        self.setup_logger()
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def setup_logger(self):
        """设置日志"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler("logs/detail_crawler.log", encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("DetailCrawler")
    
    def load_products(self):
        """
        从文件加载产品ID列表
        返回:
            产品ID列表
        """
        if not self.input_file or not os.path.exists(self.input_file):
            self.logger.error(f"输入文件 {self.input_file} 不存在")
            return []
        
        self.logger.info(f"从 {self.input_file} 加载产品数据")
        
        try:
            # 判断文件类型
            if self.input_file.endswith('.json'):
                with open(self.input_file, 'r', encoding='utf-8') as f:
                    products = json.load(f)
                
                # 提取产品ID
                product_ids = [str(product.get('id', '')) for product in products if product.get('id')]
                
            elif self.input_file.endswith('.csv'):
                df = pd.read_csv(self.input_file, encoding='utf-8')
                
                # 检查是否存在id列
                if 'id' in df.columns:
                    product_ids = [str(product_id) for product_id in df['id'].tolist() if product_id]
                else:
                    self.logger.error("CSV文件中未找到'id'列")
                    return []
            else:
                self.logger.error(f"不支持的文件格式: {self.input_file}")
                return []
            
            self.logger.info(f"成功加载 {len(product_ids)} 个产品ID")
            return product_ids
            
        except Exception as e:
            self.logger.error(f"加载产品数据时出错: {e}")
            return []
    
    def fetch_detail(self, product_id):
        """
        获取产品详情
        参数:
            product_id: 产品ID
        返回:
            产品详情字典
        """
        url = self.detail_url_template.format(product_id)
        
        for attempt in range(self.retry_count):
            try:
                self.logger.info(f"正在获取产品 {product_id} 的详情 (第 {attempt+1}/{self.retry_count} 次尝试)")
                
                # 随机选择一个User-Agent
                self.headers["user-agent"] = random.choice(self.user_agents)
                
                # 发送请求
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    timeout=(10, 30)
                )
                
                # 检查响应状态
                if response.status_code == 200:
                    self.logger.info(f"成功获取产品 {product_id} 的详情")
                    return self.parse_detail_page(response.text, product_id)
                else:
                    self.logger.warning(f"请求失败，状态码: {response.status_code}")
                    
                    # 如果状态码是404，说明产品不存在，直接返回空
                    if response.status_code == 404:
                        self.logger.warning(f"产品 {product_id} 不存在")
                        return None
            
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
        
        self.logger.error(f"获取产品 {product_id} 的详情失败，已达到最大重试次数")
        return None
    
    def parse_detail_page(self, html_content, product_id):
        """
        解析详情页面
        参数:
            html_content: HTML内容
            product_id: 产品ID
        返回:
            产品详情字典
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            product_details = {'id': product_id}
            
            # 提取产品名称
            product_name = soup.select_one('h1.title')
            if product_name:
                product_details['name'] = product_name.text.strip()
            
            # 提取产品左侧信息
            left_items = soup.select('ul.left.new-left-box li.item')
            for item in left_items:
                try:
                    key_value = item.text.strip().split('：', 1)
                    if len(key_value) == 2:
                        key, value = key_value
                        product_details[key.strip()] = value.strip()
                except Exception as e:
                    self.logger.warning(f"解析左侧信息项时出错: {e}")
            
            # 提取产品右侧信息
            right_items = soup.select('ul.right li.item')
            for item in right_items:
                try:
                    key_value = item.text.strip().split('：', 1)
                    if len(key_value) == 2:
                        key, value = key_value
                        # 处理价格字段，移除货币符号
                        if '价' in key:
                            value = value.replace('￥', '').strip()
                        product_details[key.strip()] = value.strip()
                except Exception as e:
                    self.logger.warning(f"解析右侧信息项时出错: {e}")
            
            # 提取配料表信息
            mixtu_section = soup.find('div', id='mixtu')
            if mixtu_section:
                mixtu_content = mixtu_section.get_text(strip=True)
                product_details['配料表'] = mixtu_content
            
            # 提取营养成分信息
            nutrient_section = soup.find('div', id='nutrient')
            if nutrient_section:
                nutrient_content = nutrient_section.get_text(strip=True)
                product_details['营养成分'] = nutrient_content
            
            # 提取奶粉点评信息
            comment_section = soup.find('div', id='fg_comment')
            if comment_section:
                comment_content = comment_section.get_text(strip=True)
                product_details['奶粉点评'] = comment_content
            
            # 尝试提取配方注册号
            registration_number = None
            for item in right_items:
                if '配方注册号' in item.text:
                    match = re.search(r'配方注册号：\s*([^\s<]+)', item.text)
                    if match:
                        registration_number = match.group(1)
                        product_details['配方注册号'] = registration_number
                        break
            
            self.logger.info(f"成功解析产品 {product_id} 的详情")
            return product_details
            
        except Exception as e:
            self.logger.error(f"解析产品 {product_id} 的详情页面时出错: {e}")
            # 保存错误页面用于调试
            error_file = f"logs/error_page_{product_id}.html"
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.logger.info(f"已保存错误页面到 {error_file}")
            return None
    
    def crawl_all_details(self):
        """
        爬取所有产品的详情
        返回:
            产品详情列表
        """
        # 加载产品ID
        product_ids = self.load_products()
        if not product_ids:
            self.logger.error("未加载到任何产品ID")
            return []
        
        # 开始爬取
        self.logger.info(f"开始爬取 {len(product_ids)} 个产品的详情")
        self.all_product_details = []
        
        try:
            with tqdm(total=len(product_ids), desc="爬取进度", unit="产品") as pbar:
                for i, product_id in enumerate(product_ids):
                    # 添加随机延迟
                    if i > 0:
                        delay = random.uniform(*self.delay_range)
                        if i % 10 == 0:  # 每10个请求增加额外延迟
                            delay += random.uniform(1, 3)
                        time.sleep(delay)
                    
                    # 获取产品详情
                    product_detail = self.fetch_detail(product_id)
                    
                    if product_detail:
                        self.all_product_details.append(product_detail)
                        
                        # 每爬取10个产品保存一次
                        if (i + 1) % 10 == 0 or (i + 1) == len(product_ids):
                            self.save_details(is_final=(i + 1) == len(product_ids))
                    
                    # 更新进度条
                    pbar.update(1)
                    pbar.set_description(f"爬取进度 (已获取 {len(self.all_product_details)} 个详情)")
            
            self.logger.info(f"爬取完成，共获取 {len(self.all_product_details)} 个产品详情")
            return self.all_product_details
            
        except KeyboardInterrupt:
            self.logger.warning("用户中断爬取")
            self.save_details(is_final=False)
            return self.all_product_details
            
        except Exception as e:
            self.logger.error(f"爬取过程中出现异常: {e}")
            self.save_details(is_final=False)
            return self.all_product_details
    
    def save_details(self, is_final=False):
        """
        保存产品详情到文件
        参数:
            is_final: 是否是最终数据
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 构建文件名
        prefix = "final_" if is_final else ""
        json_filename = f"{self.output_dir}/naifenzhiku_details_{prefix}{timestamp}.json"
        csv_filename = f"{self.output_dir}/naifenzhiku_details_{prefix}{timestamp}.csv"
        
        # 保存JSON格式
        try:
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(self.all_product_details, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存产品详情到 {json_filename}")
        except Exception as e:
            self.logger.error(f"保存JSON文件时出错: {e}")
        
        # 保存CSV格式
        try:
            # 将数据转换为DataFrame
            df = pd.DataFrame(self.all_product_details)
            
            # 保存为CSV
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            self.logger.info(f"已保存产品详情到 {csv_filename}")
        except Exception as e:
            self.logger.error(f"保存CSV文件时出错: {e}")
        
        return True

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="奶粉之库产品详情爬虫")
    parser.add_argument("--input", "-i", type=str, required=True, help="包含产品ID的输入文件(JSON或CSV)")
    parser.add_argument("--output", "-o", type=str, default="data", help="输出目录，默认为'data'")
    parser.add_argument("--min-delay", type=float, default=1.0, help="最小请求延迟(秒)，默认为1.0秒")
    parser.add_argument("--max-delay", type=float, default=3.0, help="最大请求延迟(秒)，默认为3.0秒")
    
    # 解析命令行参数
    args = parser.parse_args()
    
    print("=" * 50)
    print("奶粉之库产品详情爬虫启动")
    print("=" * 50)
    
    # 初始化爬虫
    crawler = NaifenzhikuDetailCrawler(
        input_file=args.input,
        output_dir=args.output,
        delay_range=(args.min_delay, args.max_delay)
    )
    
    # 开始爬取
    product_details = crawler.crawl_all_details()
    
    if product_details:
        print(f"爬取完成！共获取 {len(product_details)} 个产品详情")
    else:
        print("爬取完成，但没有获取到任何产品详情")

if __name__ == "__main__":
    main() 
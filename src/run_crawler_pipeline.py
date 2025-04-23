#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import argparse
import pandas as pd
from datetime import datetime
import logging
import sys
from tqdm import tqdm

# 导入爬虫模块
from naifenzhiku_crawler import NaifenzhikuCrawler
from naifenzhiku_detail_crawler import NaifenzhikuDetailCrawler
from naifenzhiku_more_detail_crawler import NaifenzhikuMoreDetailCrawler

class CrawlerPipeline:
    """奶粉智库爬虫数据处理流水线"""
    
    def __init__(self, output_dir="data", resume_from_page=0, max_pages=0,
                 min_delay=1.0, max_delay=3.0, skip_products=False, 
                 skip_details=False, skip_more_details=False,
                 product_file=None, username=None, password=None, auth_token=None):
        """
        初始化数据处理流水线
        参数:
            output_dir: 输出目录
            resume_from_page: 从哪一页开始爬取，0表示从头开始
            max_pages: 最大爬取页数，0表示不限制
            min_delay: 最小请求延迟(秒)
            max_delay: 最大请求延迟(秒)
            skip_products: 是否跳过产品列表爬取
            skip_details: 是否跳过产品详情爬取
            skip_more_details: 是否跳过产品额外详情爬取
            product_file: 产品列表文件路径，如果提供则不爬取产品列表
            username: 奶粉智库账号(手机号)
            password: 奶粉智库密码
            auth_token: 授权token
        """
        self.output_dir = output_dir
        self.resume_from_page = resume_from_page
        self.max_pages = max_pages
        self.delay_range = (min_delay, max_delay)
        self.skip_products = skip_products
        self.skip_details = skip_details
        self.skip_more_details = skip_more_details
        self.product_file = product_file
        self.username = username
        self.password = password
        self.auth_token = auth_token
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # 设置日志
        self.setup_logger()
        
        # 存储爬取结果
        self.products = []
        self.product_details = []
        self.more_details = []
        self.combined_data = []
        self.full_data = []
        
        # 文件路径
        self.latest_product_file = product_file if product_file else None
        self.latest_detail_file = None
        self.latest_more_detail_file = None
        self.combined_file = None
        self.full_data_file = None
    
    def setup_logger(self):
        """设置日志"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger("CrawlerPipeline")
    
    def run_product_crawler(self):
        """运行产品列表爬虫"""
        if self.skip_products and self.product_file:
            self.logger.info(f"跳过产品列表爬取，直接使用文件: {self.product_file}")
            self.latest_product_file = self.product_file
            return
        
        self.logger.info("开始爬取产品列表...")
        
        # 初始化产品爬虫
        crawler = NaifenzhikuCrawler(resume_from_page=self.resume_from_page)
        
        # 开始爬取
        if self.max_pages > 0:
            products = crawler.crawl_pages(start_page=self.resume_from_page or 1, max_pages=self.max_pages)
        else:
            products = crawler.crawl_all_products()
        
        # 获取最新的产品文件
        self.latest_product_file = self.get_latest_file(self.output_dir, "naifenzhiku_products_final_", ".json")
        
        if not self.latest_product_file:
            # 如果没有找到最终文件，尝试找到最新的中间文件
            self.latest_product_file = self.get_latest_file(self.output_dir, "naifenzhiku_products_20", ".json")
        
        if self.latest_product_file:
            self.logger.info(f"产品列表爬取完成，最新文件: {self.latest_product_file}")
        else:
            self.logger.error("未找到产品列表文件！")
    
    def run_detail_crawler(self):
        """运行产品详情爬虫"""
        if self.skip_details:
            self.logger.info("跳过产品详情爬取")
            return
        
        if not self.latest_product_file:
            self.logger.error("没有产品列表文件，无法爬取详情！")
            return
        
        self.logger.info(f"开始爬取产品详情，使用产品列表文件: {self.latest_product_file}")
        
        # 初始化详情爬虫
        crawler = NaifenzhikuDetailCrawler(
            input_file=self.latest_product_file,
            output_dir=self.output_dir,
            delay_range=self.delay_range
        )
        
        # 开始爬取
        product_details = crawler.crawl_all_details()
        
        # 获取最新的详情文件
        self.latest_detail_file = self.get_latest_file(self.output_dir, "naifenzhiku_details_final_", ".json")
        
        if not self.latest_detail_file:
            # 如果没有找到最终文件，尝试找到最新的中间文件
            self.latest_detail_file = self.get_latest_file(self.output_dir, "naifenzhiku_details_20", ".json")
        
        if self.latest_detail_file:
            self.logger.info(f"产品详情爬取完成，最新文件: {self.latest_detail_file}")
        else:
            self.logger.error("未找到产品详情文件！")
    
    def run_more_detail_crawler(self):
        """运行产品额外详情爬虫"""
        if self.skip_more_details:
            self.logger.info("跳过产品额外详情爬取")
            return
        
        if not self.latest_product_file:
            self.logger.error("没有产品列表文件，无法爬取额外详情！")
            return
        
        self.logger.info(f"开始爬取产品额外详情，使用产品列表文件: {self.latest_product_file}")
        
        # 初始化额外详情爬虫
        crawler = NaifenzhikuMoreDetailCrawler(
            product_file=self.latest_product_file,
            output_dir=self.output_dir,
            delay_range=self.delay_range,
            username=self.username,
            password=self.password,
            auth_token=self.auth_token
        )
        
        # 开始爬取
        more_details = crawler.crawl_all_more_details()
        
        # 获取最新的额外详情文件
        self.latest_more_detail_file = self.get_latest_file(self.output_dir, "naifenzhiku_more_details_final_", ".json")
        
        if not self.latest_more_detail_file:
            # 如果没有找到最终文件，尝试找到最新的中间文件
            self.latest_more_detail_file = self.get_latest_file(self.output_dir, "naifenzhiku_more_details_", ".json")
        
        if self.latest_more_detail_file:
            self.logger.info(f"产品额外详情爬取完成，最新文件: {self.latest_more_detail_file}")
        else:
            self.logger.error("未找到产品额外详情文件！")
    
    def combine_data(self):
        """将产品列表和详情数据组合在一起"""
        if not self.latest_product_file or not self.latest_detail_file:
            self.logger.error("缺少产品列表或详情文件，无法组合数据！")
            return False
        
        self.logger.info("开始组合产品列表和详情数据...")
        
        # 加载产品列表和详情数据
        try:
            with open(self.latest_product_file, 'r', encoding='utf-8') as f:
                self.products = json.load(f)
            self.logger.info(f"成功加载{len(self.products)}个产品信息")
            
            with open(self.latest_detail_file, 'r', encoding='utf-8') as f:
                self.product_details = json.load(f)
            self.logger.info(f"成功加载{len(self.product_details)}个产品详情")
        except Exception as e:
            self.logger.error(f"加载数据文件时出错: {e}")
            return False
        
        # 创建产品ID到详情的映射
        detail_map = {str(detail.get('id', '')): detail for detail in self.product_details}
        
        # 组合数据
        self.combined_data = []
        with tqdm(total=len(self.products), desc="组合数据", unit="产品") as pbar:
            for product in self.products:
                product_id = str(product.get('id', ''))
                if product_id in detail_map:
                    # 合并基本信息和详情
                    combined_product = {**product, **detail_map[product_id]}
                    self.combined_data.append(combined_product)
                else:
                    # 如果没有详情，只使用基本信息
                    self.combined_data.append(product)
                pbar.update(1)
        
        self.logger.info(f"数据组合完成，共{len(self.combined_data)}个产品")
        
        # 保存组合数据
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.combined_file = f"{self.output_dir}/naifenzhiku_combined_{timestamp}.json"
        self.combined_csv = f"{self.output_dir}/naifenzhiku_combined_{timestamp}.csv"
        
        try:
            # 保存JSON格式
            with open(self.combined_file, 'w', encoding='utf-8') as f:
                json.dump(self.combined_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存组合数据到 {self.combined_file}")
            
            # 保存CSV格式
            # 将复杂字段展平处理，以便CSV能正确显示
            df_data = []
            for item in self.combined_data:
                flat_item = {}
                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        flat_item[key] = json.dumps(value, ensure_ascii=False)
                    else:
                        flat_item[key] = value
                df_data.append(flat_item)
            
            df = pd.DataFrame(df_data)
            df.to_csv(self.combined_csv, index=False, encoding='utf-8')
            self.logger.info(f"已保存组合数据到 {self.combined_csv}")
            
            return True
        except Exception as e:
            self.logger.error(f"保存组合数据时出错: {e}")
            return False
    
    def combine_full_data(self):
        """将基础组合数据和额外详情数据进一步组合"""
        if not self.combined_file or not self.latest_more_detail_file:
            self.logger.error("缺少组合数据或额外详情文件，无法组合完整数据！")
            return False
        
        self.logger.info("开始组合完整数据...")
        
        # 加载组合数据和额外详情数据
        try:
            with open(self.combined_file, 'r', encoding='utf-8') as f:
                self.combined_data = json.load(f)
            self.logger.info(f"成功加载{len(self.combined_data)}个组合数据记录")
            
            with open(self.latest_more_detail_file, 'r', encoding='utf-8') as f:
                self.more_details = json.load(f)
            self.logger.info(f"成功加载{len(self.more_details)}个额外详情记录")
        except Exception as e:
            self.logger.error(f"加载数据文件时出错: {e}")
            return False
        
        # 创建ID到额外详情的映射
        more_detail_map = {str(detail.get('id', '')): detail for detail in self.more_details if 'id' in detail}
        
        # 组合数据
        self.full_data = []
        with tqdm(total=len(self.combined_data), desc="组合完整数据", unit="产品") as pbar:
            for product in self.combined_data:
                product_id = str(product.get('id', ''))
                
                if product_id in more_detail_map:
                    # 合并基本组合数据和额外详情
                    full_product = product.copy()
                    # 添加额外详情字段
                    for key, value in more_detail_map[product_id].items():
                        if key != 'id':  # 跳过ID字段
                            full_product[key] = value
                    
                    self.full_data.append(full_product)
                else:
                    # 如果没有额外详情，只使用基本组合数据
                    self.full_data.append(product)
                
                pbar.update(1)
        
        self.logger.info(f"完整数据组合完成，共{len(self.full_data)}个产品")
        
        # 保存完整数据
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.full_data_file = f"{self.output_dir}/naifenzhiku_full_data_{timestamp}.json"
        self.full_data_csv = f"{self.output_dir}/naifenzhiku_full_data_{timestamp}.csv"
        
        try:
            # 保存JSON格式
            with open(self.full_data_file, 'w', encoding='utf-8') as f:
                json.dump(self.full_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存完整数据到 {self.full_data_file}")
            
            # 保存CSV格式
            # 将复杂字段展平处理，以便CSV能正确显示
            df_data = []
            for item in self.full_data:
                flat_item = {}
                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        flat_item[key] = json.dumps(value, ensure_ascii=False)
                    else:
                        flat_item[key] = value
                df_data.append(flat_item)
            
            df = pd.DataFrame(df_data)
            df.to_csv(self.full_data_csv, index=False, encoding='utf-8')
            self.logger.info(f"已保存完整数据到 {self.full_data_csv}")
            
            return True
        except Exception as e:
            self.logger.error(f"保存完整数据时出错: {e}")
            return False
    
    def get_latest_file(self, directory, prefix, suffix):
        """获取指定目录下最新的文件"""
        files = [os.path.join(directory, f) for f in os.listdir(directory) 
                if f.startswith(prefix) and f.endswith(suffix)]
        
        if not files:
            return None
            
        # 按文件修改时间排序，返回最新的文件
        return max(files, key=os.path.getmtime)
    
    def run_pipeline(self):
        """运行完整的爬虫流水线"""
        self.logger.info("奶粉智库爬虫数据处理流水线启动")
        
        # 运行产品列表爬虫
        if not self.skip_products or not self.product_file:
            self.run_product_crawler()
        else:
            self.logger.info(f"跳过产品列表爬取，直接使用文件: {self.product_file}")
        
        # 运行产品详情爬虫
        if not self.skip_details:
            self.run_detail_crawler()
        
        # 组合产品列表和详情数据
        if not self.skip_details:
            combine_success = self.combine_data()
            if not combine_success:
                self.logger.error("基础数据组合未能完成！")
                return None
        
        # 运行产品额外详情爬虫
        if not self.skip_more_details:
            self.run_more_detail_crawler()
            
            # 组合完整数据
            full_data_success = self.combine_full_data()
            if full_data_success:
                self.logger.info(f"数据处理流水线已完成，完整数据已保存至: {self.full_data_file}")
                return self.full_data_file
            else:
                self.logger.error("完整数据组合未能完成！")
                return self.combined_file
        else:
            self.logger.info("跳过产品额外详情爬取，只返回基础组合数据")
            return self.combined_file

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="奶粉智库爬虫数据处理流水线")
    parser.add_argument("--output", "-o", type=str, default="data", help="输出目录，默认为'data'")
    parser.add_argument("--resume", type=int, default=0, help="从指定页码继续爬取产品列表，默认从头开始")
    parser.add_argument("--pages", type=int, default=0, help="指定爬取的页数，0表示爬取所有页")
    parser.add_argument("--min-delay", type=float, default=1.0, help="最小请求延迟(秒)，默认为1.0秒")
    parser.add_argument("--max-delay", type=float, default=3.0, help="最大请求延迟(秒)，默认为3.0秒")
    parser.add_argument("--skip-products", action="store_true", help="跳过产品列表爬取")
    parser.add_argument("--skip-details", action="store_true", help="跳过产品详情爬取")
    parser.add_argument("--skip-more-details", action="store_true", help="跳过产品额外详情爬取")
    parser.add_argument("--product-file", type=str, help="产品列表文件路径，如果提供则不爬取产品列表")
    parser.add_argument("--username", type=str, help="奶粉智库账号(手机号)")
    parser.add_argument("--password", type=str, help="奶粉智库密码")
    parser.add_argument("--token", type=str, help="直接提供的授权token")
    parser.add_argument("--token-file", type=str, help="包含授权token的文件路径")
    
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
    print("奶粉智库爬虫数据处理流水线启动")
    print("=" * 50)
    
    # 初始化流水线
    pipeline = CrawlerPipeline(
        output_dir=args.output,
        resume_from_page=args.resume,
        max_pages=args.pages,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        skip_products=args.skip_products,
        skip_details=args.skip_details,
        skip_more_details=args.skip_more_details,
        product_file=args.product_file,
        username=args.username,
        password=args.password,
        auth_token=auth_token
    )
    
    # 运行流水线
    result_file = pipeline.run_pipeline()
    
    if result_file:
        print(f"流水线执行成功！数据已保存至: {result_file}")
    else:
        print("流水线执行失败！")

if __name__ == "__main__":
    main() 
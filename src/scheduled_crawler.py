#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import argparse
import logging
import sys
import psycopg2
from psycopg2 import extras
from datetime import datetime
from tqdm import tqdm

# 导入爬虫模块
from naifenzhiku_crawler import NaifenzhikuCrawler
from naifenzhiku_detail_crawler import NaifenzhikuDetailCrawler
from naifenzhiku_more_detail_crawler import NaifenzhikuMoreDetailCrawler
from run_crawler_pipeline import CrawlerPipeline
from db_import import DatabaseImporter

class ScheduledCrawler:
    """定时爬虫：根据tag_time判断是否需要更新产品详情"""
    
    def __init__(self, output_dir="data", check_updates=False, skip_existing=False,
                 db_host="localhost", db_port=5432, db_name="milk_products", 
                 db_user="postgres", db_password="postgres",
                 max_pages=0, min_delay=2.0, max_delay=5.0):
        """
        初始化定时爬虫
        参数:
            output_dir: 输出目录
            check_updates: 是否检查更新
            skip_existing: 是否跳过已存在的产品
            db_host: 数据库主机
            db_port: 数据库端口
            db_name: 数据库名称
            db_user: 数据库用户
            db_password: 数据库密码
            max_pages: 最大爬取页数，0表示爬取所有页面
            min_delay: 最小请求延迟(秒)
            max_delay: 最大请求延迟(秒)
        """
        self.output_dir = output_dir
        self.check_updates = check_updates
        self.skip_existing = skip_existing
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.max_pages = max_pages
        self.delay_range = (min_delay, max_delay)
        
        # 存储已有产品信息
        self.existing_products = {}
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # 设置日志
        self.setup_logger()
        
        # 如果需要检查更新，连接数据库并获取已有产品信息
        if check_updates:
            self.connect_db()
            self.load_existing_products()
    
    def setup_logger(self):
        """设置日志"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(f"logs/scheduled_crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger("ScheduledCrawler")
    
    def connect_db(self):
        """连接到PostgreSQL数据库"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            self.logger.info(f"已成功连接到数据库: {self.db_name}@{self.db_host}:{self.db_port}")
            
            # 创建游标
            self.cur = self.conn.cursor(cursor_factory=extras.DictCursor)
            
            # 设置自动提交
            self.conn.autocommit = False
            
            return True
        except Exception as e:
            self.logger.error(f"连接数据库时出错: {e}")
            return False
    
    def close_db(self):
        """关闭数据库连接"""
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            self.logger.info("数据库连接已关闭")
    
    def load_existing_products(self):
        """从数据库加载已有产品信息"""
        try:
            sql = """
            SELECT product_id, tag_time, updated_at
            FROM milk_products
            """
            
            self.cur.execute(sql)
            products = self.cur.fetchall()
            
            for product in products:
                self.existing_products[str(product['product_id'])] = {
                    'tag_time': product['tag_time'],
                    'updated_at': product['updated_at']
                }
            
            self.logger.info(f"已从数据库加载 {len(self.existing_products)} 个产品信息")
            return True
        except Exception as e:
            self.logger.error(f"加载已有产品信息时出错: {e}")
            return False
    
    def run_crawler_and_filter(self):
        """运行爬虫并根据tag_time筛选需要更新的产品"""
        self.logger.info("开始运行爬虫并筛选需要更新的产品...")
        
        # 初始化产品爬虫
        crawler = NaifenzhikuCrawler()
        
        # 开始爬取产品列表
        products = []
        
        try:
            if self.max_pages > 0:
                # 爬取指定页数
                self.logger.info(f"爬取前 {self.max_pages} 页的产品")
                products = crawler.crawl_pages(start_page=1, max_pages=self.max_pages)
            else:
                # 爬取所有页面
                self.logger.info("爬取所有页面的产品")
                products = crawler.crawl_all_products()
                
            self.logger.info(f"成功爬取了 {len(products)} 个产品信息")
        except Exception as e:
            self.logger.error(f"爬取产品列表时出错: {e}")
            return None
        
        # 筛选需要更新的产品
        new_products = []
        updated_products = []
        unchanged_products = []
        
        for product in products:
            product_id = str(product.get('id', ''))
            
            # 如果产品ID不存在，跳过
            if not product_id:
                continue
            
            # 获取产品的tag_time
            tag_time = product.get('tag_time', 0)
            
            # 判断是否需要更新
            if product_id not in self.existing_products:
                # 新产品
                new_products.append(product)
                self.logger.info(f"发现新产品: {product_id} - {product.get('name', '')}")
            elif tag_time != self.existing_products[product_id]['tag_time']:
                # tag_time变化，需要更新
                updated_products.append(product)
                self.logger.info(f"发现需要更新的产品: {product_id} - {product.get('name', '')}")
            else:
                # 产品未变化
                unchanged_products.append(product)
        
        self.logger.info(f"共发现 {len(new_products)} 个新产品, {len(updated_products)} 个需要更新的产品, {len(unchanged_products)} 个无需更新的产品")
        
        # 创建要处理的产品列表
        products_to_process = new_products + updated_products
        
        if not products_to_process:
            self.logger.info("没有需要处理的产品，任务完成")
            return None
        
        # 保存需要处理的产品列表到临时文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        products_file = f"{self.output_dir}/naifenzhiku_products_to_update_{timestamp}.json"
        
        try:
            with open(products_file, 'w', encoding='utf-8') as f:
                json.dump(products_to_process, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存需要处理的产品列表到 {products_file}")
            
            return products_file
        except Exception as e:
            self.logger.error(f"保存产品列表时出错: {e}")
            return None
    
    def process_products(self, products_file):
        """处理需要更新的产品"""
        self.logger.info(f"开始处理需要更新的产品: {products_file}")
        
        # 初始化爬虫流水线
        pipeline = CrawlerPipeline(
            output_dir=self.output_dir,
            product_file=products_file,
            skip_products=True,  # 直接使用筛选后的产品列表
            min_delay=self.delay_range[0],
            max_delay=self.delay_range[1]
        )
        
        # 运行流水线
        result_file = pipeline.run_pipeline()
        
        if result_file:
            self.logger.info(f"产品更新完成，结果保存在: {result_file}")
            return result_file
        else:
            self.logger.error("产品更新失败!")
            return None
    
    def import_to_database(self, data_file):
        """将更新后的产品数据导入到数据库"""
        self.logger.info(f"开始将更新后的产品数据导入到数据库: {data_file}")
        
        # 初始化数据库导入器
        importer = DatabaseImporter(
            host=self.db_host,
            port=self.db_port,
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            json_file=data_file
        )
        
        # 执行数据导入
        success = importer.import_data()
        
        if success:
            self.logger.info("数据导入成功!")
            return True
        else:
            self.logger.error("数据导入失败!")
            return False
    
    def run(self):
        """运行定时爬虫任务"""
        self.logger.info("定时爬虫任务开始执行...")
        
        try:
            # 1. 运行爬虫并筛选需要更新的产品
            if self.check_updates:
                products_file = self.run_crawler_and_filter()
                
                if not products_file:
                    self.logger.info("没有需要更新的产品，任务结束")
                    return True
            else:
                # 直接运行完整流水线
                pipeline = CrawlerPipeline(
                    output_dir=self.output_dir,
                    max_pages=self.max_pages,
                    min_delay=self.delay_range[0],
                    max_delay=self.delay_range[1]
                )
                
                result_file = pipeline.run_pipeline()
                
                if result_file:
                    self.logger.info(f"爬虫流水线执行成功，结果保存在: {result_file}")
                    
                    # 导入到数据库
                    success = self.import_to_database(result_file)
                    return success
                else:
                    self.logger.error("爬虫流水线执行失败!")
                    return False
            
            # 2. 处理需要更新的产品
            result_file = self.process_products(products_file)
            
            if not result_file:
                self.logger.error("处理产品失败!")
                return False
            
            # 3. 导入到数据库
            success = self.import_to_database(result_file)
            
            return success
        except Exception as e:
            self.logger.error(f"执行定时爬虫任务时出错: {e}")
            return False
        finally:
            # 关闭数据库连接
            if hasattr(self, 'conn'):
                self.close_db()

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="定时爬虫：根据tag_time判断是否需要更新产品详情")
    parser.add_argument("--output", "-o", type=str, default="data", help="输出目录，默认为'data'")
    parser.add_argument("--check-updates", action="store_true", help="是否检查更新")
    parser.add_argument("--skip-existing", action="store_true", help="是否跳过已存在的产品")
    parser.add_argument("--db-host", type=str, default="localhost", help="数据库主机，默认为localhost")
    parser.add_argument("--db-port", type=int, default=5432, help="数据库端口，默认为5432")
    parser.add_argument("--db-name", type=str, default="milk_products", help="数据库名称，默认为milk_products")
    parser.add_argument("--db-user", type=str, default="postgres", help="数据库用户，默认为postgres")
    parser.add_argument("--db-password", type=str, default="postgres", help="数据库密码，默认为postgres")
    parser.add_argument("--max-pages", type=int, default=0, help="最大爬取页数，0表示爬取所有页面")
    parser.add_argument("--min-delay", type=float, default=2.0, help="最小请求延迟(秒)，默认为2.0秒")
    parser.add_argument("--max-delay", type=float, default=5.0, help="最大请求延迟(秒)，默认为5.0秒")
    
    # 解析命令行参数
    args = parser.parse_args()
    
    print("=" * 50)
    print("定时爬虫启动")
    print("=" * 50)
    
    # 初始化定时爬虫
    crawler = ScheduledCrawler(
        output_dir=args.output,
        check_updates=args.check_updates,
        skip_existing=args.skip_existing,
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
        max_pages=args.max_pages,
        min_delay=args.min_delay,
        max_delay=args.max_delay
    )
    
    # 运行定时爬虫
    success = crawler.run()
    
    if success:
        print("定时爬虫执行成功!")
        sys.exit(0)
    else:
        print("定时爬虫执行失败!")
        sys.exit(1)

if __name__ == "__main__":
    main() 
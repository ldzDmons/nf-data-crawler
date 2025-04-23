#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import argparse
import logging
import psycopg2
from psycopg2 import extras
from datetime import datetime
import sys
from tqdm import tqdm

class DatabaseImporter:
    """奶粉智库数据导入器：将爬取的JSON数据导入到PostgreSQL数据库"""
    
    def __init__(self, host="localhost", port=5432, dbname="milk_products", 
                 user="postgres", password="postgres", json_file=None):
        """
        初始化数据库导入器
        参数:
            host: 数据库主机
            port: 数据库端口
            dbname: 数据库名称
            user: 数据库用户
            password: 数据库密码
            json_file: 要导入的JSON文件路径
        """
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.json_file = json_file
        
        # 设置日志
        self.setup_logger()
        
        # 连接数据库
        self.connect_db()
    
    def setup_logger(self):
        """设置日志"""
        os.makedirs("logs", exist_ok=True)
        
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(f"logs/db_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger("DatabaseImporter")
    
    def connect_db(self):
        """连接到PostgreSQL数据库"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.logger.info(f"已成功连接到数据库: {self.dbname}@{self.host}:{self.port}")
            
            # 创建游标
            self.cur = self.conn.cursor()
            
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
    
    def load_json_data(self, json_file=None):
        """加载要导入的JSON数据"""
        file_path = json_file or self.json_file
        
        if not file_path:
            self.logger.error("没有指定JSON文件路径!")
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.logger.info(f"已成功加载JSON数据文件: {file_path}, 包含{len(data)}条记录")
            return data
        except Exception as e:
            self.logger.error(f"加载JSON数据时出错: {e}")
            return None
    
    def import_products(self, data):
        """导入奶粉产品基本信息"""
        if not data:
            self.logger.error("没有数据可以导入!")
            return 0
        
        self.logger.info("开始导入奶粉产品基本信息...")
        inserted_count = 0
        
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    for item in tqdm(data, desc="导入产品基本信息", unit="产品"):
                        # 确保产品ID存在
                        if 'id' not in item:
                            continue
                        
                        # 准备SQL语句和参数
                        sql = """
                        INSERT INTO milk_products 
                        (product_id, name, thumbnail, thumbnail_alt, click_count, price, tag, tag_time, icon)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_id) 
                        DO UPDATE SET 
                            name = EXCLUDED.name,
                            thumbnail = EXCLUDED.thumbnail,
                            thumbnail_alt = EXCLUDED.thumbnail_alt,
                            click_count = EXCLUDED.click_count,
                            price = EXCLUDED.price,
                            tag = EXCLUDED.tag,
                            tag_time = EXCLUDED.tag_time,
                            icon = EXCLUDED.icon,
                            updated_at = NOW()
                        RETURNING id
                        """
                        
                        params = (
                            item.get('id'),
                            item.get('name'),
                            item.get('thumbnail'),
                            item.get('thumbnail_alt'),
                            item.get('click_count'),
                            item.get('price'),
                            item.get('tag'),
                            item.get('tag_time'),
                            item.get('icon')
                        )
                        
                        # 执行SQL
                        cur.execute(sql, params)
                        inserted_count += 1
            
            self.logger.info(f"成功导入或更新了 {inserted_count} 条产品基本信息")
            return inserted_count
        except Exception as e:
            self.logger.error(f"导入产品基本信息时出错: {e}")
            return 0
    
    def import_product_details(self, data):
        """导入奶粉产品详情信息"""
        if not data:
            self.logger.error("没有数据可以导入!")
            return 0
        
        self.logger.info("开始导入奶粉产品详情信息...")
        inserted_count = 0
        
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    for item in tqdm(data, desc="导入产品详情", unit="产品"):
                        # 确保产品ID存在
                        if 'id' not in item:
                            continue
                        
                        # 准备SQL语句和参数
                        sql = """
                        INSERT INTO milk_product_details 
                        (product_id, brand, series, origin, milk_source, age_range, 
                        manufacturer, operator, specification, stage, reference_price, 
                        category, version, formula_registration, formula_evaluation, ingredients)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_id) 
                        DO UPDATE SET 
                            brand = EXCLUDED.brand,
                            series = EXCLUDED.series,
                            origin = EXCLUDED.origin,
                            milk_source = EXCLUDED.milk_source,
                            age_range = EXCLUDED.age_range,
                            manufacturer = EXCLUDED.manufacturer,
                            operator = EXCLUDED.operator,
                            specification = EXCLUDED.specification,
                            stage = EXCLUDED.stage,
                            reference_price = EXCLUDED.reference_price,
                            category = EXCLUDED.category,
                            version = EXCLUDED.version,
                            formula_registration = EXCLUDED.formula_registration,
                            formula_evaluation = EXCLUDED.formula_evaluation,
                            ingredients = EXCLUDED.ingredients,
                            updated_at = NOW()
                        RETURNING id
                        """
                        
                        params = (
                            item.get('id'),
                            item.get('品牌'),
                            item.get('系列'),
                            item.get('产地'),
                            item.get('奶源'),
                            item.get('适用年龄'),
                            item.get('厂家'),
                            item.get('运营商'),
                            item.get('规格'),
                            item.get('段位'),
                            item.get('参考价'),
                            item.get('类别'),
                            item.get('版本'),
                            item.get('配方注册号'),
                            item.get('配方评价'),
                            item.get('配料表')
                        )
                        
                        # 执行SQL
                        cur.execute(sql, params)
                        inserted_count += 1
            
            self.logger.info(f"成功导入或更新了 {inserted_count} 条产品详情信息")
            return inserted_count
        except Exception as e:
            self.logger.error(f"导入产品详情信息时出错: {e}")
            return 0
    
    def import_nutrients(self, data):
        """导入奶粉产品营养成分信息"""
        if not data:
            self.logger.error("没有数据可以导入!")
            return 0
        
        self.logger.info("开始导入奶粉产品营养成分信息...")
        inserted_count = 0
        total_inserted = 0
        
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    for item in tqdm(data, desc="导入营养成分", unit="产品"):
                        # 确保产品ID和营养成分存在
                        if 'id' not in item or '营养成分' not in item or not isinstance(item['营养成分'], dict):
                            continue
                        
                        product_id = item.get('id')
                        nutrients = item.get('营养成分', {})
                        
                        # 遍历每个营养成分
                        for nutrient_name, nutrient_data in nutrients.items():
                            if not isinstance(nutrient_data, dict):
                                continue
                                
                            # 准备SQL语句和参数
                            sql = """
                            INSERT INTO milk_product_nutrients
                            (product_id, nutrient_name, content, unit, description)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (product_id, nutrient_name) 
                            DO UPDATE SET 
                                content = EXCLUDED.content,
                                unit = EXCLUDED.unit,
                                description = EXCLUDED.description,
                                updated_at = NOW()
                            RETURNING id
                            """
                            
                            params = (
                                product_id,
                                nutrient_name,
                                nutrient_data.get('含量'),
                                nutrient_data.get('单位'),
                                nutrient_data.get('描述')
                            )
                            
                            # 执行SQL
                            cur.execute(sql, params)
                            inserted_count += 1
                        
                        total_inserted += 1
            
            self.logger.info(f"成功导入或更新了 {inserted_count} 条营养成分信息，涉及 {total_inserted} 个产品")
            return inserted_count
        except Exception as e:
            self.logger.error(f"导入营养成分信息时出错: {e}")
            return 0
    
    def import_extra_details(self, data):
        """导入奶粉产品额外详情信息"""
        if not data:
            self.logger.error("没有数据可以导入!")
            return 0
        
        self.logger.info("开始导入奶粉产品额外详情信息...")
        inserted_count = 0
        total_products = 0
        
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    for item in tqdm(data, desc="导入额外详情", unit="产品"):
                        # 确保产品ID存在
                        if 'id' not in item:
                            continue
                        
                        product_id = item.get('id')
                        has_extra_details = False
                        
                        # 检查是否有详情类别字段
                        extra_detail_keys = [k for k in item.keys() if k.startswith('详情_')]
                        
                        if extra_detail_keys:
                            for key in extra_detail_keys:
                                # 准备SQL语句和参数
                                sql = """
                                INSERT INTO milk_product_extra_details
                                (product_id, key, value)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (product_id, key) 
                                DO UPDATE SET 
                                    value = EXCLUDED.value,
                                    updated_at = NOW()
                                RETURNING id
                                """
                                
                                # 如果值是复杂类型，转换为JSON字符串
                                value = item.get(key)
                                if isinstance(value, (dict, list)):
                                    value = json.dumps(value, ensure_ascii=False)
                                
                                params = (product_id, key, value)
                                
                                # 执行SQL
                                cur.execute(sql, params)
                                inserted_count += 1
                                has_extra_details = True
                        
                        if has_extra_details:
                            total_products += 1
            
            self.logger.info(f"成功导入或更新了 {inserted_count} 条额外详情信息，涉及 {total_products} 个产品")
            return inserted_count
        except Exception as e:
            self.logger.error(f"导入额外详情信息时出错: {e}")
            return 0
    
    def import_data(self, json_file=None):
        """执行完整的数据导入过程"""
        # 加载JSON数据
        file_path = json_file or self.json_file
        data = self.load_json_data(file_path)
        
        if not data:
            self.logger.error("没有数据可以导入!")
            return False
        
        try:
            # 导入产品基本信息
            products_count = self.import_products(data)
            
            # 导入产品详情信息
            details_count = self.import_product_details(data)
            
            # 导入营养成分信息
            nutrients_count = self.import_nutrients(data)
            
            # 导入额外详情信息
            extra_details_count = self.import_extra_details(data)
            
            self.logger.info(f"数据导入完成，共导入或更新了:")
            self.logger.info(f"- {products_count} 条产品基本信息")
            self.logger.info(f"- {details_count} 条产品详情信息")
            self.logger.info(f"- {nutrients_count} 条营养成分信息")
            self.logger.info(f"- {extra_details_count} 条额外详情信息")
            
            return True
        except Exception as e:
            self.logger.error(f"导入数据时出错: {e}")
            return False
        finally:
            # 关闭数据库连接
            self.close_db()

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="奶粉智库数据导入器：将爬取的JSON数据导入到PostgreSQL数据库")
    parser.add_argument("--host", type=str, default="localhost", help="数据库主机，默认为localhost")
    parser.add_argument("--port", type=int, default=5432, help="数据库端口，默认为5432")
    parser.add_argument("--dbname", type=str, default="milk_products", help="数据库名称，默认为milk_products")
    parser.add_argument("--user", type=str, default="postgres", help="数据库用户，默认为postgres")
    parser.add_argument("--password", type=str, default="postgres", help="数据库密码，默认为postgres")
    parser.add_argument("--file", type=str, required=True, help="要导入的JSON文件路径")
    
    # 解析命令行参数
    args = parser.parse_args()
    
    print("=" * 50)
    print("奶粉智库数据导入器启动")
    print("=" * 50)
    
    # 初始化数据库导入器
    importer = DatabaseImporter(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        json_file=args.file
    )
    
    # 执行数据导入
    success = importer.import_data()
    
    if success:
        print("数据导入成功!")
    else:
        print("数据导入失败!")
        sys.exit(1)

if __name__ == "__main__":
    main() 
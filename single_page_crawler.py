#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import argparse
from datetime import datetime
import logging
import sys
import tempfile

# 导入爬虫模块
from src.naifenzhiku_crawler import NaifenzhikuCrawler
from src.naifenzhiku_detail_crawler import NaifenzhikuDetailCrawler
from src.naifenzhiku_more_detail_crawler import NaifenzhikuMoreDetailCrawler

def setup_logger():
    """设置日志"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f"logs/single_page_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger("SinglePageCrawler")

def crawl_single_page(page_number, output_dir="data", token_file=None, product_count=None):
    """
    爬取单个页面的产品数据，并获取其详情信息
    
    参数:
        page_number: 要爬取的页码
        output_dir: 输出目录
        token_file: 包含授权token的文件路径
        product_count: 限制爬取的产品数量
    """
    logger = setup_logger()
    logger.info(f"===== 开始爬取奶粉智库第{page_number}页产品及详情 =====")
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # 1. 爬取指定页面的产品列表
    logger.info(f"第一步: 爬取第{page_number}页产品列表...")
    crawler = NaifenzhikuCrawler()
    products = crawler.crawl_pages(start_page=page_number, max_pages=1)
    
    if not products:
        logger.error(f"未能获取第{page_number}页产品列表！")
        return None
    
    logger.info(f"获取到{len(products)}个产品")
    
    # 如果需要限制产品数量
    if product_count and product_count < len(products):
        logger.info(f"限制爬取前{product_count}个产品")
        products = products[:product_count]
    
    # 将产品列表保存到临时文件
    temp_product_file = os.path.join(output_dir, f"temp_products_page{page_number}.json")
    with open(temp_product_file, 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    logger.info(f"产品列表已保存到临时文件: {temp_product_file}")
    
    # 2. 爬取产品详情
    logger.info("第二步: 爬取产品详情...")
    detail_crawler = NaifenzhikuDetailCrawler(
        input_file=temp_product_file,
        output_dir=output_dir
    )
    details = detail_crawler.crawl_all_details()
    
    if not details:
        logger.error("未能获取产品详情！")
        return None
    
    logger.info(f"获取到{len(details)}个产品详情")
    
    # 3. 爬取产品额外详情（如果提供了token文件）
    more_details = None
    auth_token = None
    
    if token_file:
        try:
            with open(token_file, 'r') as f:
                token_data = json.load(f)
                if isinstance(token_data, dict) and 'token' in token_data:
                    auth_token = token_data['token']
                    logger.info(f"已从文件加载授权token: {auth_token[:20]}...")
                else:
                    logger.error(f"无法从文件加载token，文件内容格式不符")
        except Exception as e:
            logger.error(f"读取token文件时出错: {e}")
    
    if auth_token:
        logger.info("第三步: 爬取产品额外详情...")
        more_detail_crawler = NaifenzhikuMoreDetailCrawler(
            product_file=temp_product_file,
            output_dir=output_dir,
            auth_token=auth_token
        )
        more_details = more_detail_crawler.crawl_all_more_details()
        
        if not more_details:
            logger.error("未能获取产品额外详情！")
        else:
            logger.info(f"获取到{len(more_details)}个产品额外详情")
    
    # 4. 合并数据
    logger.info("第四步: 合并所有数据...")
    combined_data = []
    
    # 创建ID映射
    detail_map = {str(detail.get('id', '')): detail for detail in details}
    more_detail_map = {}
    if more_details:
        more_detail_map = {str(detail.get('id', '')): detail for detail in more_details if 'id' in detail}
    
    # 合并所有数据
    for product in products:
        product_id = str(product.get('id', ''))
        combined_product = product.copy()
        
        # 添加详情数据
        if product_id in detail_map:
            for key, value in detail_map[product_id].items():
                if key != 'id':  # 跳过ID字段
                    combined_product[key] = value
        
        # 添加额外详情数据
        if product_id in more_detail_map:
            for key, value in more_detail_map[product_id].items():
                if key != 'id':  # 跳过ID字段
                    combined_product[key] = value
        
        combined_data.append(combined_product)
    
    # 保存合并后的数据
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    combined_file = os.path.join(output_dir, f"naifenzhiku_page{page_number}_combined_{timestamp}.json")
    
    with open(combined_file, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, ensure_ascii=False, indent=2)
    logger.info(f"合并数据已保存到: {combined_file}")
    
    # 创建最新数据的软链接
    latest_file = os.path.join(output_dir, f"naifenzhiku_page{page_number}_combined_latest.json")
    combined_file_basename = os.path.basename(combined_file)  # 只获取文件名部分
    
    if os.path.exists(latest_file):
        try:
            os.remove(latest_file)
        except:
            pass
    
    try:
        # 使用相对路径而不是绝对路径，避免创建错误的符号链接
        os.symlink(combined_file_basename, latest_file)
        logger.info(f"创建最新数据链接: {latest_file}")
    except Exception as e:
        # 如果创建软链接失败，则复制文件
        import shutil
        shutil.copy2(combined_file, latest_file)
        logger.info(f"创建最新数据副本: {latest_file} (原因: {str(e)})")
    
    # 删除临时文件
    try:
        os.remove(temp_product_file)
        logger.info(f"已删除临时文件: {temp_product_file}")
    except:
        logger.warning(f"无法删除临时文件: {temp_product_file}")
    
    logger.info(f"===== 爬取奶粉智库第{page_number}页产品及详情完成 =====")
    return combined_file

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="奶粉智库单页爬虫")
    parser.add_argument("--page", type=int, required=True, help="要爬取的页码")
    parser.add_argument("--output", "-o", type=str, default="data", help="输出目录，默认为'data'")
    parser.add_argument("--token-file", type=str, help="包含授权token的文件路径")
    parser.add_argument("--count", type=int, help="限制爬取的产品数量")
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 运行爬虫
    result_file = crawl_single_page(
        page_number=args.page,
        output_dir=args.output,
        token_file=args.token_file,
        product_count=args.count
    )
    
    if result_file:
        print(f"爬取成功！数据已保存至: {result_file}")
    else:
        print("爬取失败！")

if __name__ == "__main__":
    main() 
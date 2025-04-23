请确保填写正确的奶粉智库账号和密码

# 奶粉智库数据爬虫与数据库管理系统

这是一个用于爬取奶粉智库网站的数据，并将其存储到PostgreSQL数据库的完整解决方案。整个系统支持Docker化部署，方便在各种环境中快速启动和使用。

## 配置指南

### 配置文件设置

系统使用配置文件来存储敏感信息，如账号密码。在项目根目录下创建 `config` 目录并放入 `config.json` 文件:

```json
{
  "naifenzhiku": {
    "username": "你的奶粉智库账号",
    "password": "你的奶粉智库密码",
    "delay_range": [1, 3],
    "retry_count": 3,
    "retry_delay": 3
  },
  "output_dir": "/app/data",
  "log_dir": "/app/logs"
}
```

此配置文件会被挂载到Docker容器的 `/app/config` 目录，而不是构建到镜像中，确保敏感信息安全。

## 功能特点

- 爬取奶粉智库的产品列表、详情和营养成分数据
- 将爬取的数据保存为JSON格式
- 导入数据到结构化的PostgreSQL数据库
- 完整的Docker部署方案，包括数据库和数据导入服务
- 支持增量更新和断点续传

## 系统架构

- **爬虫模块**：负责从网站爬取数据并保存为JSON格式
- **数据库模块**：基于PostgreSQL的结构化数据库，包含多个相关表
- **数据导入模块**：将JSON数据导入到数据库的处理脚本

## 数据库结构

系统使用PostgreSQL数据库，包含以下表：

1. `milk_products`：奶粉产品基本信息表
2. `milk_product_details`：奶粉产品详情表
3. `milk_product_nutrients`：奶粉产品营养成分表
4. `milk_product_extra_details`：奶粉产品额外详情表

## 快速开始

### 1. 使用Docker部署

确保安装了Docker和Docker Compose，然后执行：

```bash
# 确保创建了config/config.json文件并填写了账号信息

# 构建并启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f
```

### 2. 手动部署

#### 2.1 安装依赖

```bash
pip install -r requirements.txt
```

#### 2.2 创建并配置config.json

```bash
mkdir -p config
# 编辑 config/config.json 文件，填入账号密码
```

#### 2.3 运行爬虫

```bash
python src/run_crawler_pipeline.py --config config/config.json
```

#### 2.4 设置数据库

```bash
# 创建PostgreSQL数据库
psql -U postgres -c "CREATE DATABASE milk_products;"

# 初始化数据库结构
psql -U postgres -d milk_products -f database/schema.sql
```

#### 2.5 导入数据

```bash
python src/db_import.py --file data/naifenzhiku_page2_combined_20250423_110444.json
```

## 使用方法

### 爬虫命令行参数

```bash
python src/run_crawler_pipeline.py [--output OUTPUT_DIR] [--resume PAGE] 
                                   [--pages NUM_PAGES] [--min-delay MIN_DELAY] 
                                   [--max-delay MAX_DELAY] [--skip-products] 
                                   [--skip-details] [--skip-more-details] 
                                   [--product-file FILE]
```

### 数据导入命令行参数

```bash
python src/db_import.py [--host HOST] [--port PORT] [--dbname DBNAME]
                         [--user USER] [--password PASSWORD] --file FILE
```

## 开发与贡献

1. 克隆仓库
```bash
git clone https://github.com/yourusername/nf-data-crawler.git
cd nf-data-crawler
```

2. 创建虚拟环境
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. 安装开发依赖
```bash
pip install -r requirements.txt
```

## 许可证

MIT 

## 定时任务配置

系统配置了自动爬虫任务，默认每周日凌晨2点执行。您可以通过环境变量调整定时爬取的时间和其他参数：

```yaml
environment:
  # 定时任务配置，格式为: 分 时 日 月 周
  # 例如: 0 2 * * 0 表示每周日凌晨2点执行
  - CRON_SCHEDULE=0 2 * * 0
  # 爬虫参数配置
  - CRAWLER_MAX_PAGES=0  # 0表示爬取所有页面
  - CRAWLER_MIN_DELAY=2.0  # 请求间最小延迟(秒)
  - CRAWLER_MAX_DELAY=5.0  # 请求间最大延迟(秒)
```

### 手动触发爬虫

您可以随时手动触发爬虫任务，无需等待定时执行：

```bash
# 执行增量更新（根据tag_time对比只更新变化的产品）
docker-compose exec cron-crawler python src/scheduled_crawler.py --check-updates --output /app/data --db-host postgres

# 执行完整爬取（不检查现有数据）
docker-compose exec cron-crawler python src/scheduled_crawler.py --output /app/data --db-host postgres

# 只爬取第一页数据并自动导入数据库（快速测试适用）
docker-compose exec cron-crawler python src/scheduled_crawler.py --max-pages 1 --output /app/data --db-host postgres
```

> **注意**：使用`--max-pages 1`参数可以限制爬虫只爬取第一页数据，适合快速测试系统功能。系统会自动保存数据并导入到数据库中。使用`scheduled_crawler.py`脚本可以确保完整的爬取、数据处理和数据库导入流程。

### 连接到容器

系统配置了SSH服务，您可以直接连接到容器进行操作：

```bash
# 通过SSH连接到容器
ssh -p 2222 root@localhost  # 密码: password

# 连接后可以查看日志
tail -f /app/logs/cron_crawler.log

# 或手动导入数据
python src/db_import.py --host postgres --dbname milk_products --file /app/data/文件名.json
``` 
#!/bin/bash
set -e

# 启动SSH服务
echo "启动SSH服务..."
service ssh start

# 检查配置文件
CONFIG_FILE="/app/config/config.json"
if [ -f "$CONFIG_FILE" ]; then
    echo "检测到配置文件: $CONFIG_FILE"
    # 提取配置信息用于调试
    if command -v jq &> /dev/null; then
        echo "配置文件内容概要:"
        jq 'del(.naifenzhiku.password)' "$CONFIG_FILE"
    else
        echo "已找到配置文件，但未安装jq工具，无法显示内容概要"
    fi
else
    echo "警告: 未找到配置文件 $CONFIG_FILE，将使用默认配置"
    if [ -f "/app/config/config.template.json" ]; then
        echo "创建默认配置文件..."
        cp /app/config/config.template.json "$CONFIG_FILE"
    fi
fi

# 创建cron任务
echo "配置定时爬虫任务..."
CONFIG_PARAM=""
if [ -f "$CONFIG_FILE" ]; then
    CONFIG_PARAM="--config $CONFIG_FILE"
fi

echo "${CRON_SCHEDULE:-0 2 * * 0} cd /app && python src/scheduled_crawler.py --check-updates --output ${CRAWLER_OUTPUT_DIR:-/app/data} --skip-existing --max-pages ${CRAWLER_MAX_PAGES:-0} --min-delay ${CRAWLER_MIN_DELAY:-2.0} --max-delay ${CRAWLER_MAX_DELAY:-5.0} --db-host ${DB_HOST:-postgres} --db-port ${DB_PORT:-5432} --db-name ${DB_NAME:-milk_products} --db-user ${DB_USER:-postgres} --db-password ${DB_PASSWORD:-postgres} $CONFIG_PARAM >> /app/logs/cron_crawler.log 2>&1" > /etc/cron.d/crawler-cron
chmod 0644 /etc/cron.d/crawler-cron
crontab /etc/cron.d/crawler-cron

# 打印当前cron配置信息
echo "===== 已配置的定时爬虫任务 ====="
crontab -l
echo "================================"

echo "当前使用的配置："
echo "- 定时计划: ${CRON_SCHEDULE:-0 2 * * 0}"
echo "- 输出目录: ${CRAWLER_OUTPUT_DIR:-/app/data}"
echo "- 最大页数: ${CRAWLER_MAX_PAGES:-0}"
echo "- 延迟范围: ${CRAWLER_MIN_DELAY:-2.0}秒 ~ ${CRAWLER_MAX_DELAY:-5.0}秒"
echo "- 数据库主机: ${DB_HOST:-postgres}:${DB_PORT:-5432}"
echo "- 数据库名称: ${DB_NAME:-milk_products}"
echo "- 数据库用户: ${DB_USER:-postgres}"
echo "- 配置文件: $CONFIG_FILE"
echo "================================"

echo "容器已完成初始化，可通过以下命令手动执行爬虫:"
echo "python src/scheduled_crawler.py --check-updates --output /app/data --db-host postgres $CONFIG_PARAM"
echo ""

# 如果传入了命令，则执行该命令，否则启动cron服务
if [ $# -eq 0 ]; then
    echo "启动cron服务..."
    cron -f
else
    echo "执行命令: $@"
    exec "$@"
fi 
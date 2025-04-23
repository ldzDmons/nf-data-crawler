#!/bin/bash
set -e

# 启动SSH服务
echo "启动SSH服务..."
service ssh start

# 创建cron任务
echo "配置定时爬虫任务..."
echo "${CRON_SCHEDULE:-0 2 * * 0} cd /app && python src/scheduled_crawler.py --check-updates --output ${CRAWLER_OUTPUT_DIR:-/app/data} --skip-existing --max-pages ${CRAWLER_MAX_PAGES:-0} --min-delay ${CRAWLER_MIN_DELAY:-2.0} --max-delay ${CRAWLER_MAX_DELAY:-5.0} --db-host ${DB_HOST:-postgres} --db-port ${DB_PORT:-5432} --db-name ${DB_NAME:-milk_products} --db-user ${DB_USER:-postgres} --db-password ${DB_PASSWORD:-postgres} >> /app/logs/cron_crawler.log 2>&1" > /etc/cron.d/crawler-cron
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
echo "================================"

echo "容器已完成初始化，可通过以下命令手动执行爬虫:"
echo "python src/scheduled_crawler.py --check-updates --output /app/data --db-host postgres"
echo ""

# 如果传入了命令，则执行该命令，否则启动cron服务
if [ $# -eq 0 ]; then
    echo "启动cron服务..."
    cron -f
else
    echo "执行命令: $@"
    exec "$@"
fi 
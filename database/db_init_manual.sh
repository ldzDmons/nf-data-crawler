#!/bin/bash
set -e

echo "正在手动初始化数据库表结构..."

# 检查数据库是否存在，如果不存在则创建
psql -U postgres -c "SELECT 1 FROM pg_database WHERE datname = 'milk_products'" | grep -q 1 || psql -U postgres -c "CREATE DATABASE milk_products;"

# 执行schema.sql脚本创建表
psql -U postgres -d milk_products -f /tmp/schema.sql

echo "数据库表结构初始化完成！" 
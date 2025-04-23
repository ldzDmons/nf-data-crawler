#!/bin/bash
set -e

echo "PostgreSQL is being initialized with data from schema.sql"

# 切换到数据库并执行初始化SQL脚本
# 注意：PostgreSQL会自动创建POSTGRES_DB环境变量指定的数据库
psql -U postgres -d "${POSTGRES_DB}" -f /docker-entrypoint-initdb.d/schema.sql

echo "Database initialization completed successfully" 
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 使用阿里云apt源（Debian 12 Bookworm）
RUN echo "deb https://mirrors.aliyun.com/debian/ bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list && \
    echo "deb https://mirrors.aliyun.com/debian/ bookworm-updates main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.aliyun.com/debian/ bookworm-backports main contrib non-free non-free-firmware" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.aliyun.com/debian-security bookworm-security main contrib non-free non-free-firmware" >> /etc/apt/sources.list

# 安装cron、SSH服务和其他必要工具
RUN apt-get update && \
    apt-get install -y cron openssh-server vim nano less curl && \
    rm -rf /var/lib/apt/lists/*

# 配置SSH服务
RUN mkdir /var/run/sshd && \
    echo 'root:password' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

# 设置pip国内源
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# 安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制源代码（不包含敏感配置文件）
COPY src/ /app/src/

# 创建日志、数据和配置目录
RUN mkdir -p /app/logs /app/data /app/config

# 创建默认配置文件模板
RUN echo '{\n  "naifenzhiku": {\n    "username": "",\n    "password": "",\n    "delay_range": [1, 3],\n    "retry_count": 3,\n    "retry_delay": 3\n  },\n  "output_dir": "data",\n  "log_dir": "logs"\n}' > /app/config/config.template.json

# 启动脚本
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 声明卷，这样外部可以挂载配置文件、数据和日志
VOLUME ["/app/config", "/app/data", "/app/logs"]

# 默认命令
CMD ["python", "src/scheduled_crawler.py", "--help"]

# 暴露SSH端口
EXPOSE 22 
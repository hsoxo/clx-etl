FROM python:3.13-bookworm

# 设置时区和避免交互式
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# 安装系统依赖（包含 git 和构建常用工具）
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir .

# 复制源码
COPY . .

# 默认命令
CMD ["python", "scheduler.py"]
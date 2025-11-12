import os
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv
from sqlalchemy import create_engine

from .models import Kline1d, Kline1h, Kline1m

load_dotenv()

__all__ = [
    "Kline1d",
    "Kline1h",
    "Kline1m",
    "async_bulk_insert",
    "async_client",
    "engine",
]

CLICKHOUSE_HOST = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_PORT = int(os.environ["CLICKHOUSE_PORT"] or 8123)
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_DATABASE = os.environ["CLICKHOUSE_DATABASE"]

# 创建 ClickHouse SQLAlchemy 引擎
engine = create_engine(
    f"clickhouse+native://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"
)


async def get_async_client():
    return await clickhouse_connect.create_async_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


async def async_bulk_insert(
    values: list[dict[str, Any]],
    model,
    batch_size: int = 5000,
):
    """
    异步批量插入 ClickHouse（ReplacingMergeTree 自动去重）
    :param values: list[dict] 数据
    :param model: SQLAlchemy ORM 模型类
    :param batch_size: 批量大小（默认 5000）
    """
    if not values:
        return

    table = model.__table__
    valid_cols = [c.name for c in table.columns]

    insert_values = [{k: v for k, v in row.items() if k in valid_cols} for row in values]
    columns = list(insert_values[0].keys())

    async with await get_async_client() as client:
        for i in range(0, len(insert_values), batch_size):
            chunk = insert_values[i : i + batch_size]
            data = [tuple(row[col] for col in columns) for row in chunk]

            await client.insert(
                table.name,
                data,
                column_names=columns,
            )

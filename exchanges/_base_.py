from abc import ABC, abstractmethod
import asyncio
import time
import traceback
from typing import Literal

from aiohttp import ClientSession
from constants import INTERVAL_TO_SECONDS
from databases.clickhouse import Kline1d, Kline1h, Kline1m, async_bulk_insert
from databases.mysql import ExchangeSymbol, async_upsert, sync_engine
from loguru import logger
from sqlalchemy import text


class BaseClient(ABC):
    def __init__(self):
        self._exchange_id = None

    @abstractmethod
    def base_url(self):
        raise NotImplementedError

    @abstractmethod
    def exchange_name(self) -> str:
        raise NotImplementedError

    @property
    def exchange_id(self):
        if self._exchange_id:
            return self._exchange_id
        with sync_engine.begin() as conn:
            result = conn.execute(text("SELECT id FROM exchange_info WHERE name = :name"), {"name": self.exchange_name})
            row = result.scalar_one_or_none()
            return row

    @abstractmethod
    def inst_type(self):
        raise NotImplementedError

    async def send_request(self, method: Literal["GET", "POST"], endpoint: str, params=None, headers=None) -> dict:
        url = f"{self.base_url}{endpoint}"
        async with ClientSession() as session:
            if method == "GET":
                response = await session.get(url, params=params, headers=headers)
            elif method == "POST":
                response = await session.post(url, json=params, headers=headers)
            return await response.json()

    @abstractmethod
    async def get_all_symbols(self):
        raise NotImplementedError

    async def update_all_symbols(self):
        values = await self.get_all_symbols()
        await async_upsert(
            values,
            ExchangeSymbol,
            [
                "tick_size",
                "step_size",
                "price_precision",
                "quantity_precision",
                "status",
            ],
        )

    async def _get_kline(
        self,
        url: str,
        params: dict,
        get_data,
        format_item,
        start_time_key: str,
        limit: int,
        end_time_key: str | None = None,
        time_unit: Literal["ms", "s"] = "ms",
        interval: Literal["1m", "1h", "1d"] = "1m",
        start_ms: int | None = None,
        end_ms: int | None = None,
        sleep_ms: int = 100,
        **kwargs,
    ):
        second = 1 if time_unit == "s" else 1000
        end_ms = end_ms or int(time.time() * second)
        if start_ms:
            params[start_time_key] = int(start_ms // (1000 / second))
        try:
            while True:
                if end_time_key:
                    params[end_time_key] = params[start_time_key] + limit * second * INTERVAL_TO_SECONDS.get(interval)
                results = []
                data = await self.send_request("GET", url, params=params)
                for d in get_data(data):
                    results.append(format_item(d))
                yield results
                if results:
                    params[start_time_key] = int((results[-1]["timestamp"] + second) // (1000 / second))
                else:
                    params[start_time_key] += int(INTERVAL_TO_SECONDS.get(interval) * second * limit)
                if params[start_time_key] > end_ms:
                    break
                await asyncio.sleep(sleep_ms / 1000)
        except Exception as e:
            logger.error(
                {
                    "url": self.base_url + url,
                    "params": params,
                    "error": e,
                    "traceback": traceback.format_exc(),
                }
            )

    async def update_kline(
        self,
        symbol: str,
        interval: Literal["1m", "1h", "1d"] = "1m",
        start_ms: int | None = None,
        end_ms: int | None = None,
    ):
        model = Kline1m
        if interval == "1h":
            model = Kline1h
        elif interval == "1d":
            model = Kline1d
        async for klines in self.get_kline(symbol, interval, start_ms, end_ms):
            await async_bulk_insert(klines, model)

import asyncio

from exchanges.aster import AsterPerpClient
from exchanges.binance import BinancePerpClient, BinanceSpotClient
from exchanges.bitget import BitgetPerpClient, BitgetSpotClient
from exchanges.bitmart import BitmartPerpClient, BitmartSpotClient
from exchanges.bybit import BybitPerpClient, BybitSpotClient
from exchanges.gate import GatePerpClient, GateSpotClient
from exchanges.mexc import MexcPerpClient, MexcSpotClient
from exchanges.okx import OkxPerpClient, OkxSpotClient
from exchanges.woox import WooxPerpClient, WooxSpotClient


async def sync_symbols():
    clients = [
        AsterPerpClient(),
        BinanceSpotClient(),
        BinancePerpClient(),
        BitgetSpotClient(),
        BitgetPerpClient(),
        BitmartSpotClient(),
        BitmartPerpClient(),
        BybitSpotClient(),
        BybitPerpClient(),
        GateSpotClient(),
        GatePerpClient(),
        MexcSpotClient(),
        MexcPerpClient(),
        OkxSpotClient(),
        OkxPerpClient(),
        WooxSpotClient(),
        WooxPerpClient(),
    ]

    await asyncio.gather(*(client.update_all_symbols() for client in clients))


if __name__ == "__main__":
    asyncio.run(sync_symbols())

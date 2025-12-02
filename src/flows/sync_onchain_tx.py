from macro_markets.oklink.fetcher import OklinkOnchainInfo
from prefect import flow

from databases.doris import get_stream_loader


@flow(name="sync-large-transfer")
async def sync_onchain_large_transfer():
    stream_loader = get_stream_loader()
    oklink_onchain_info = OklinkOnchainInfo()

    result = await oklink_onchain_info.large_tranfer_monitor()
    await stream_loader.send_rows(result, "onchain_large_transfer")


if __name__ == "__main__":
    import asyncio

    asyncio.run(sync_onchain_large_transfer())

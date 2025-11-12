from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger

from jobs.sync_klines import sync_klines_1h, sync_klines_1m
from jobs.sync_symbols import sync_symbols

scheduler = BlockingScheduler()
scheduler.add_job(sync_klines_1m, "interval", days=1, max_instances=1)
scheduler.add_job(sync_klines_1h, "interval", days=1, max_instances=1)
scheduler.add_job(sync_symbols, "interval", days=1, max_instances=1)

if __name__ == "__main__":
    logger.info("Starting scheduler...")
    scheduler.start()

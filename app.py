from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio

async def example_job():
    print("Running example job...")

async def main():
    scheduler = AsyncIOScheduler(timezone='UTC')
    scheduler.add_job(example_job, 'interval', seconds=5)
    scheduler.start()
    print("Scheduler started.")
    
    await asyncio.sleep(20)  # Keep the script running for a while

asyncio.run(main())

from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager

app = FastAPI()

async def example_job():
    print("Running example job...")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting scheduler setup...")
    scheduler = AsyncIOScheduler(timezone='UTC')
    scheduler.add_job(example_job, 'interval', seconds=5)
    print("Scheduled example_job every 5 seconds.")
    
    scheduler.start()
    print("Scheduler started.")

    yield

    print("Shutting down scheduler...")
    scheduler.shutdown()
    print("Scheduler shut down.")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"message": "Server is running with scheduler"}

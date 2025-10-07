from fastapi import FastAPI
from .api.evaluate import router as evaluate_router

app = FastAPI()

app.include_router(evaluate_router, prefix="/api")

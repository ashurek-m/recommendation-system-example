from fastapi import FastAPI, HTTPException, Depends
from datetime import datetime
from typing import List
from sqlalchemy import func
from pydantic import BaseModel
from loguru import logger
import psycopg2
from psycopg2.extras import RealDictCursor
import uvicorn
import os

app = FastAPI()


class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True


def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH


def load_features():
    pass


def load_models():
    model_path = get_model_path("/my/super/path")
    # LOAD MODEL HERE PLS :)
    pass


@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
        id: int,
        time: datetime,
        limit: int = 10) -> List[PostGet]:
    pass


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=5000)

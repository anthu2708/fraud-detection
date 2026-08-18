from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from prometheus_fastapi_instrumentator import Instrumentator

from .routers import events, transactions
from .services import db, kafka

_STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.Base.metadata.create_all(db.engine)
    db.migrate()
    kafka.startup()
    yield
    kafka.shutdown()


app = FastAPI(title="Fraud Detection Query API", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=_STATIC), name="static")
app.include_router(transactions.router)
app.include_router(events.router)

Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


@app.get("/", response_class=HTMLResponse)
def landing():
    return (_STATIC / "landing.html").read_text(encoding="utf-8")


@app.get("/about", response_class=HTMLResponse)
def about():
    return (_STATIC / "about.html").read_text(encoding="utf-8")


@app.get("/demo", response_class=HTMLResponse)
def demo():
    return (_STATIC / "index.html").read_text(encoding="utf-8")

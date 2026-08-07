import asyncio

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from ..services import kafka

router = APIRouter()


@router.get("/events")
async def sse_events():
    q: asyncio.Queue = kafka.subscribe_sse()

    async def stream():
        try:
            while True:
                data = await q.get()
                yield f"data: {data}\n\n"
        finally:
            kafka.unsubscribe_sse(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

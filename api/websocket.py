import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from api import database
import config

logger = logging.getLogger(__name__)

router = APIRouter()

active_connections: Dict[WebSocket, dict] = {}
active_connections_lock = asyncio.Lock()


@router.websocket(
    "/api/v1/ws/rss-items",
    name="rss_items_websocket",
    summary="WebSocket for real-time RSS items updates",
    description="""
    Establish a WebSocket connection for receiving real-time updates about new RSS items.

    This WebSocket endpoint allows clients to subscribe to live news updates filtered by language preferences.

    **Connection Process:**
    1. Client connects to WebSocket endpoint
    2. Client sends subscription message within 10 seconds
    3. Server acknowledges connection and starts sending updates

    **Subscription Message Format:**
    ```json
    {
        "type": "subscribe",
        "display_language": "en",
        "original_language": "en",
        "use_translations": true
    }
    ```

    **Supported Message Types:**
    - `subscribe`: Initial subscription with filter parameters
    - `ping`: Keep-alive ping (server responds with `pong`)
    - `update_params`: Update filtering parameters

    **Update Message Format:**
    ```json
    {
        "type": "new_rss_items",
        "timestamp": "2024-01-01T12:00:00",
        "count": 3,
        "rss_items": [
            {
                "news_id": "abc123",
                "title": "Breaking News...",
                "category": "Technology",
                "published_at": "2024-01-01T12:00:00Z"
            }
        ]
    }
    ```

    **Filtering Parameters:**
    - `display_language`: Language for displaying content (en, ru, de, fr)
    - `original_language`: Filter by original article language
    - `use_translations`: Whether to use translated titles when available

    **Connection Limits:** No explicit rate limiting, but server may disconnect inactive connections.
    """
)
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    params = None
    try:
        data = await asyncio.wait_for(websocket.receive_text(), timeout=10.0)
        message = json.loads(data)
        if message.get("type") != "subscribe":
            await websocket.send_text(json.dumps({"error": "Expected subscribe message"}))
            await websocket.close()
            return
        params = {
            "original_language": message.get("original_language"),
            "display_language": message.get("display_language"),
            "use_translations": message.get("use_translations", False),
        }
        async with active_connections_lock:
            active_connections[websocket] = params
        logger.info(f"[WebSocket] New connection with params: {params}. Total connections: {len(active_connections)}")
    except asyncio.TimeoutError:
        await websocket.send_text(json.dumps({"error": "Subscribe timeout"}))
        await websocket.close()
        return
    except json.JSONDecodeError:
        await websocket.send_text(json.dumps({"error": "Invalid JSON"}))
        await websocket.close()
        return
    except Exception as e:
        logger.error(f"[WebSocket] Unexpected error during subscribe: {e}")
        await websocket.close()
        return

    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong", "timestamp": datetime.now().isoformat()}))
                elif message.get("type") == "update_params":
                    new_params = {
                        "original_language": message.get("original_language", params.get("original_language")),
                        "display_language": message.get("display_language", params.get("display_language")),
                        "use_translations": message.get("use_translations", params.get("use_translations", False)),
                    }
                    async with active_connections_lock:
                        active_connections[websocket] = new_params
                    params = new_params
                    await websocket.send_text(json.dumps({"type": "params_updated"}))
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"type": "echo", "data": data}))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"[WebSocket] Unexpected error: {e}")
        try:
            await websocket.close()
        except Exception:
            pass
    finally:
        async with active_connections_lock:
            active_connections.pop(websocket, None)
        logger.info(f"[WebSocket] Connection closed. Total connections: {len(active_connections)}")


async def broadcast_new_rss_items(rss_items_payload: List[dict]):
    if not active_connections:
        return
    disconnected = []
    async with active_connections_lock:
        connections_snapshot = list(active_connections.items())
    for ws, params in connections_snapshot:
        filtered_items = []
        for item in rss_items_payload:
            if params.get("original_language") and item.get("original_language") != params["original_language"]:
                continue
            title = item.get("original_title", "")[:100] + "..." if item.get("original_title", "") else "Без заголовка"
            if params.get("use_translations", False) and params.get("display_language"):
                trans = item.get("translations", {}).get(params["display_language"], {})
                if trans.get("title"):
                    t = trans["title"]
                    title = t[:100] + "..." if len(t) > 100 else t
            filtered_items.append(
                {
                    "news_id": item.get("news_id"),
                    "title": title,
                    "category": item.get("category", "Без категории"),
                    "published_at": item.get("published_at"),
                }
            )
        if filtered_items:
            message = {
                "type": "new_rss_items",
                "timestamp": datetime.now().isoformat(),
                "count": len(filtered_items),
                "rss_items": filtered_items[:5],
            }
            try:
                await ws.send_text(json.dumps(message, ensure_ascii=False))
            except WebSocketDisconnect:
                disconnected.append(ws)
            except Exception as e:
                logger.error(f"[WebSocket] Error sending to connection: {e}")
                disconnected.append(ws)
    if disconnected:
        async with active_connections_lock:
            for conn in disconnected:
                active_connections.pop(conn, None)
        logger.info(f"[WebSocket] Removed {len(disconnected)} disconnected clients")


last_rss_items_check_time = datetime.now()


async def check_for_new_rss_items():
    global last_rss_items_check_time
    pool = await database.get_db_pool()
    if pool is None:
        logger.error("[RSS Items Check] Database pool is not available.")
        return
    while True:
        await asyncio.sleep(config.RSS_ITEM_CHECK_INTERVAL_SECONDS)
        try:
            rss_items_payload = await database.get_recent_rss_items_for_broadcast(pool, last_rss_items_check_time)
            if rss_items_payload:
                await broadcast_new_rss_items(rss_items_payload)
            last_rss_items_check_time = datetime.now()
        except Exception as e:
            logger.error(f"[RSS Items Check] Error checking for new rss items: {e}")

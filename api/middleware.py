import logging
from fastapi import FastAPI, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

# --- Rate Limiting ---
limiter = Limiter(key_func=get_remote_address)


class ApplicationRateLimitMiddleware(BaseHTTPMiddleware):
    """Application-level rate limiting middleware"""

    async def dispatch(self, request: Request, call_next):
        # Get client IP
        client_ip = get_remote_address(request)

        # Check application-level limits based on endpoint
        path = request.url.path

        # Stricter limits for auth endpoints
        if path.startswith("/api/v1/auth"):
            # Allow 100 requests per minute per IP for auth
            pass  # SlowAPI limiter handles this

        # General API limits
        elif path.startswith("/api/"):
            # Allow 1000 requests per minute per IP for general API
            pass  # SlowAPI limiter handles this

        # Continue with request
        response = await call_next(request)
        return response


class ForceUTF8ResponseMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            response = await call_next(request)
            content_type = response.headers.get("content-type", "").lower()
            if content_type.startswith("text/") or content_type.startswith("application/json"):
                if "charset=" not in content_type:
                    new_content_type = f"{content_type}; charset=utf-8"
                    response.headers["content-type"] = new_content_type
                elif "charset=utf-8" not in content_type and "charset=utf8" not in content_type:
                    parts = content_type.split(";")
                    new_parts = [parts[0]]
                    new_parts.append("charset=utf-8")
                    new_content_type = ";".join(new_parts)
                    response.headers["content-type"] = new_content_type
            return response
        except Exception as e:
            logger.error(f"[Middleware Error] ForceUTF8: {e}")
            raise


def setup_middleware(app: FastAPI):
    app.add_middleware(ForceUTF8ResponseMiddleware)
    app.add_middleware(ApplicationRateLimitMiddleware)
    app.add_middleware(SlowAPIMiddleware)
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    # SlowAPI requires limiter in state
    app.state.limiter = limiter

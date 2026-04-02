from contextvars import ContextVar
from uuid import uuid4
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

# 1. Define the variable
correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default='')

class CorrelationIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Get from header (for cross-service tracing) or generate new
        correlation_id = request.headers.get('X-Correlation-ID', str(uuid4()))
        
        # 2. Set and store the token
        token = correlation_id_var.set(correlation_id)
        
        try:
            response = await call_next(request)
            response.headers['X-Correlation-ID'] = correlation_id
            return response
        finally:
            # 3. Reset the context after the request is done
            correlation_id_var.reset(token)

def get_correlation_id() -> str:
    return correlation_id_var.get()

def add_correlation_id(logger, method_name, event_dict):
    """The Structlog Processor"""
    event_dict["correlation_id"] = get_correlation_id()
    return event_dict
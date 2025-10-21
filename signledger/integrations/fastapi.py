"""FastAPI integration for SignLedger."""

import asyncio
import json
import logging
import uuid
from typing import Any, Dict, Optional, List, Callable, Tuple
from functools import wraps
from datetime import datetime
from contextvars import ContextVar

try:
    from fastapi import FastAPI, Request, Response, HTTPException, Depends, Query
    from fastapi.responses import JSONResponse, StreamingResponse
    from starlette.middleware.base import BaseHTTPMiddleware
    from pydantic import BaseModel, Field
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    BaseModel = object

from ..core.ledger import Ledger
from ..core.entry import Entry
from ..backends.memory import MemoryBackend
from ..backends.sqlite import SQLiteBackend

logger = logging.getLogger(__name__)

# Context variables for request-scoped data
_request_audit_data: ContextVar[Dict[str, Any]] = ContextVar('request_audit_data', default={})
_request_id: ContextVar[str] = ContextVar('request_id', default='')


# Pydantic models
class AuditEntry(BaseModel):
    """Audit entry model."""
    sequence: int
    timestamp: str
    data: Dict[str, Any]
    hash: str
    previous_hash: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    signature: Optional[str] = None


class AuditSearchCriteria(BaseModel):
    """Search criteria for audit entries."""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    user: Optional[str] = None
    type: Optional[str] = None
    request_id: Optional[str] = None
    data_filters: Dict[str, Any] = Field(default_factory=dict)


class AuditStats(BaseModel):
    """Audit statistics."""
    total_entries: int
    latest_sequence: Optional[int]
    latest_timestamp: Optional[str]
    entry_types: Dict[str, int]
    integrity_status: bool = True


class VerificationResult(BaseModel):
    """Integrity verification result."""
    valid_count: int
    invalid_count: int
    invalid_sequences: List[int]
    integrity: bool


class SignLedgerMiddleware(BaseHTTPMiddleware):
    """Middleware for automatic audit logging."""
    
    def __init__(
        self,
        app,
        ledger: Ledger,
        audit_methods: List[str] = ['POST', 'PUT', 'DELETE', 'PATCH'],
        exclude_patterns: List[str] = ['/docs', '/redoc', '/openapi.json', '/health'],
        include_request_body: bool = True,
        include_response_body: bool = False,
        max_body_size: int = 10000
    ):
        super().__init__(app)
        self.ledger = ledger
        self.audit_methods = audit_methods
        self.exclude_patterns = exclude_patterns
        self.include_request_body = include_request_body
        self.include_response_body = include_response_body
        self.max_body_size = max_body_size
    
    async def dispatch(self, request: Request, call_next):
        # Generate request ID
        request_id = str(uuid.uuid4())
        _request_id.set(request_id)
        
        # Check if we should audit this request
        if not self._should_audit(request):
            response = await call_next(request)
            return response
        
        # Start timing
        start_time = datetime.utcnow()
        
        # Prepare audit data
        audit_data = {
            'request_id': request_id,
            'type': 'http_request',
            'method': request.method,
            'path': request.url.path,
            'query_params': dict(request.query_params),
            'client_host': request.client.host if request.client else None,
            'headers': dict(request.headers),
            'timestamp': start_time.isoformat(),
        }
        
        # Store in context
        _request_audit_data.set(audit_data)
        
        # Capture request body if configured
        if self.include_request_body and request.method in self.audit_methods:
            try:
                body = await request.body()
                if body:
                    try:
                        audit_data['request_body'] = json.loads(body)
                    except:
                        audit_data['request_body'] = body[:self.max_body_size].decode('utf-8', errors='ignore')
            except Exception as e:
                logger.debug(f"Failed to capture request body: {e}")
        
        # Process request
        response = await call_next(request)
        
        # Add response data
        end_time = datetime.utcnow()
        audit_data['status_code'] = response.status_code
        audit_data['duration_ms'] = int((end_time - start_time).total_seconds() * 1000)
        
        # Capture response body if configured
        if self.include_response_body and hasattr(response, 'body'):
            try:
                audit_data['response_body'] = response.body[:self.max_body_size].decode('utf-8', errors='ignore')
            except:
                pass
        
        # Create audit entry
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            entry = await loop.run_in_executor(
                None,
                self.ledger.append,
                audit_data
            )
            
            # Add audit headers
            response.headers['X-Audit-ID'] = request_id
            response.headers['X-Audit-Sequence'] = str(entry.sequence)
            
        except Exception as e:
            logger.error(f"Failed to create audit entry: {e}")
        
        return response
    
    def _should_audit(self, request: Request) -> bool:
        """Check if request should be audited."""
        # Check excluded paths
        for pattern in self.exclude_patterns:
            if pattern in request.url.path:
                return False
        
        # Check method for full audit
        if request.method in self.audit_methods:
            return True
        
        # Could add more complex logic here
        return False


class FastAPISignLedger:
    """FastAPI integration for SignLedger."""
    
    def __init__(self, ledger: Optional[Ledger] = None):
        self.ledger = ledger
        self._initialized = False
    
    def init_app(
        self,
        app: FastAPI,
        ledger: Optional[Ledger] = None,
        backend_type: str = 'memory',
        backend_config: Optional[Dict[str, Any]] = None,
        middleware_config: Optional[Dict[str, Any]] = None
    ) -> None:
        """Initialize FastAPI application with SignLedger."""
        if not HAS_FASTAPI:
            raise ImportError("FastAPI is required. Install with: pip install fastapi")
        
        # Initialize ledger
        if ledger:
            self.ledger = ledger
        elif not self.ledger:
            self.ledger = self._create_ledger(backend_type, backend_config or {})
        
        # Add middleware
        middleware_config = middleware_config or {}
        app.add_middleware(SignLedgerMiddleware, ledger=self.ledger, **middleware_config)
        
        # Store reference
        app.state.signledger = self
        
        # Add startup/shutdown handlers
        @app.on_event("startup")
        async def startup_event():
            logger.info("SignLedger initialized for FastAPI")
        
        @app.on_event("shutdown")
        async def shutdown_event():
            if hasattr(self.ledger.storage, 'close'):
                self.ledger.storage.close()
            logger.info("SignLedger shutdown complete")
        
        self._initialized = True
    
    def _create_ledger(self, backend_type: str, config: Dict[str, Any]) -> Ledger:
        """Create ledger with specified backend."""
        if backend_type == 'sqlite':
            db_path = config.get('database_path', 'fastapi_audit.db')
            backend = SQLiteBackend(database_path=db_path)
        else:
            backend = MemoryBackend()
        
        return Ledger(
            name=config.get('name', 'fastapi_audit'),
            storage=backend
        )
    
    async def audit_event(
        self,
        event_type: str,
        data: Dict[str, Any],
        user: Optional[str] = None,
        **metadata
    ) -> Entry:
        """Create a custom audit entry."""
        audit_data = {
            'type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': data,
        }
        
        # Add request context if available
        request_id = _request_id.get()
        if request_id:
            audit_data['request_id'] = request_id
        
        if user:
            audit_data['user'] = user
        
        if metadata:
            audit_data['metadata'] = metadata
        
        # Create entry in thread pool
        loop = asyncio.get_event_loop()
        entry = await loop.run_in_executor(
            None,
            self.ledger.append,
            audit_data
        )
        
        return entry
    
    async def search(self, criteria: AuditSearchCriteria) -> List[Entry]:
        """Search audit entries."""
        search_dict = {}
        
        if criteria.start_time:
            search_dict['start_time'] = criteria.start_time.isoformat()
        
        if criteria.end_time:
            search_dict['end_time'] = criteria.end_time.isoformat()
        
        if criteria.user:
            search_dict['data'] = {'user': criteria.user}
        
        if criteria.type:
            if 'data' not in search_dict:
                search_dict['data'] = {}
            search_dict['data']['type'] = criteria.type
        
        if criteria.request_id:
            if 'data' not in search_dict:
                search_dict['data'] = {}
            search_dict['data']['request_id'] = criteria.request_id
        
        # Add custom data filters
        if criteria.data_filters:
            if 'data' not in search_dict:
                search_dict['data'] = {}
            search_dict['data'].update(criteria.data_filters)
        
        # Search in thread pool
        loop = asyncio.get_event_loop()
        entries = await loop.run_in_executor(
            None,
            self.ledger.search,
            search_dict
        )
        
        return entries
    
    async def verify_integrity(
        self,
        start: int = 0,
        end: Optional[int] = None
    ) -> VerificationResult:
        """Verify ledger integrity."""
        if end is None:
            latest = self.ledger.get_latest()
            end = latest.sequence if latest else 0
        
        # Verify in thread pool
        loop = asyncio.get_event_loop()
        valid, invalid = await loop.run_in_executor(
            None,
            self.ledger.verify_range,
            start,
            end
        )
        
        return VerificationResult(
            valid_count=len(valid),
            invalid_count=len(invalid),
            invalid_sequences=invalid[:100],  # Limit to first 100
            integrity=len(invalid) == 0
        )


# Dependency injection
def get_signledger(request: Request) -> FastAPISignLedger:
    """Get SignLedger instance from app state."""
    if not hasattr(request.app.state, 'signledger'):
        raise HTTPException(500, "SignLedger not initialized")
    return request.app.state.signledger


def get_request_id() -> str:
    """Get current request ID."""
    return _request_id.get()


def get_audit_data() -> Dict[str, Any]:
    """Get current request's audit data."""
    return _request_audit_data.get()


# Decorators
def audit_action(
    action_type: str,
    include_args: bool = True,
    include_result: bool = False
):
    """Decorator to audit function/endpoint calls."""
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                signledger = kwargs.get('signledger')
                if not signledger:
                    # Try to get from FastAPI dependency injection
                    for arg in args:
                        if isinstance(arg, FastAPISignLedger):
                            signledger = arg
                            break
                
                if not signledger:
                    # No SignLedger instance, just execute
                    return await func(*args, **kwargs)
                
                # Prepare audit data
                audit_data = {
                    'function': func.__name__,
                    'module': func.__module__,
                }
                
                if include_args:
                    try:
                        # Filter out non-serializable args
                        audit_data['args'] = str(args)[:1000]
                        audit_data['kwargs'] = str({k: v for k, v in kwargs.items() if k != 'signledger'})[:1000]
                    except:
                        pass
                
                # Execute function
                error = None
                result = None
                
                try:
                    result = await func(*args, **kwargs)
                    audit_data['success'] = True
                    
                    if include_result:
                        try:
                            audit_data['result'] = str(result)[:1000]
                        except:
                            pass
                    
                except Exception as e:
                    error = e
                    audit_data['success'] = False
                    audit_data['error'] = str(e)
                    raise
                
                finally:
                    # Create audit entry
                    await signledger.audit_event(action_type, audit_data)
                
                return result
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                # Similar logic for sync functions
                return func(*args, **kwargs)
            
            return sync_wrapper
    
    return decorator


# API Router
def create_audit_router(
    prefix: str = "/audit",
    tags: List[str] = ["audit"],
    dependencies: Optional[List[Depends]] = None
):
    """Create API router for audit endpoints."""
    if not HAS_FASTAPI:
        raise ImportError("FastAPI is required")
    
    from fastapi import APIRouter
    
    router = APIRouter(
        prefix=prefix,
        tags=tags,
        dependencies=dependencies or []
    )
    
    @router.get("/entries", response_model=List[AuditEntry])
    async def get_entries(
        signledger: FastAPISignLedger = Depends(get_signledger),
        skip: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=1000)
    ):
        """Get audit entries with pagination."""
        loop = asyncio.get_event_loop()
        all_entries = await loop.run_in_executor(
            None,
            signledger.ledger.get_all
        )
        
        # Paginate
        entries = all_entries[skip:skip + limit]
        
        return [
            AuditEntry(**entry.to_dict())
            for entry in entries
        ]
    
    @router.get("/entries/{sequence}", response_model=AuditEntry)
    async def get_entry(
        sequence: int,
        signledger: FastAPISignLedger = Depends(get_signledger)
    ):
        """Get specific audit entry."""
        loop = asyncio.get_event_loop()
        entry = await loop.run_in_executor(
            None,
            signledger.ledger.get,
            sequence
        )
        
        if not entry:
            raise HTTPException(404, "Entry not found")
        
        return AuditEntry(**entry.to_dict())
    
    @router.post("/search", response_model=List[AuditEntry])
    async def search_entries(
        criteria: AuditSearchCriteria,
        signledger: FastAPISignLedger = Depends(get_signledger)
    ):
        """Search audit entries."""
        entries = await signledger.search(criteria)
        
        return [
            AuditEntry(**entry.to_dict())
            for entry in entries
        ]
    
    @router.post("/verify", response_model=VerificationResult)
    async def verify_integrity(
        start: int = Query(0, ge=0),
        end: Optional[int] = Query(None, ge=0),
        signledger: FastAPISignLedger = Depends(get_signledger)
    ):
        """Verify audit trail integrity."""
        return await signledger.verify_integrity(start, end)
    
    @router.get("/stats", response_model=AuditStats)
    async def get_stats(
        signledger: FastAPISignLedger = Depends(get_signledger)
    ):
        """Get audit statistics."""
        loop = asyncio.get_event_loop()
        
        # Get latest entry
        latest = await loop.run_in_executor(
            None,
            signledger.ledger.get_latest
        )
        
        # Count entry types
        from collections import Counter
        type_counts = Counter()
        
        all_entries = await loop.run_in_executor(
            None,
            signledger.ledger.get_all
        )
        
        for entry in all_entries:
            entry_type = entry.data.get('type', 'unknown')
            type_counts[entry_type] += 1
        
        # Quick integrity check
        if latest:
            result = await signledger.verify_integrity(
                max(0, latest.sequence - 100),
                latest.sequence
            )
            integrity = result.integrity
        else:
            integrity = True
        
        return AuditStats(
            total_entries=latest.sequence + 1 if latest else 0,
            latest_sequence=latest.sequence if latest else None,
            latest_timestamp=latest.timestamp if latest else None,
            entry_types=dict(type_counts),
            integrity_status=integrity
        )
    
    @router.get("/export")
    async def export_entries(
        format: str = Query("json", regex="^(json|csv)$"),
        signledger: FastAPISignLedger = Depends(get_signledger)
    ):
        """Export audit entries."""
        loop = asyncio.get_event_loop()
        entries = await loop.run_in_executor(
            None,
            signledger.ledger.get_all
        )
        
        if format == "json":
            content = json.dumps(
                [e.to_dict() for e in entries],
                indent=2
            )
            
            return Response(
                content=content,
                media_type="application/json",
                headers={
                    "Content-Disposition": "attachment; filename=audit_export.json"
                }
            )
        
        elif format == "csv":
            # Simple CSV export
            import csv
            import io
            
            output = io.StringIO()
            if entries:
                writer = csv.DictWriter(
                    output,
                    fieldnames=['sequence', 'timestamp', 'type', 'user', 'status_code'],
                    extrasaction='ignore'
                )
                writer.writeheader()
                
                for entry in entries:
                    row = entry.to_dict()
                    row.update(row.get('data', {}))
                    writer.writerow(row)
            
            return Response(
                content=output.getvalue(),
                media_type="text/csv",
                headers={
                    "Content-Disposition": "attachment; filename=audit_export.csv"
                }
            )
    
    return router


# WebSocket support for real-time audit streaming
class AuditWebSocketManager:
    """Manage WebSocket connections for real-time audit updates."""
    
    def __init__(self, signledger: FastAPISignLedger):
        self.signledger = signledger
        self.active_connections: List[Any] = []
    
    async def connect(self, websocket):
        """Accept WebSocket connection."""
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket):
        """Remove WebSocket connection."""
        self.active_connections.remove(websocket)
    
    async def send_entry(self, websocket, entry: Entry):
        """Send audit entry to specific connection."""
        await websocket.send_json({
            "type": "audit_entry",
            "entry": entry.to_dict()
        })
    
    async def broadcast_entry(self, entry: Entry):
        """Broadcast new entry to all connections."""
        for connection in self.active_connections:
            try:
                await self.send_entry(connection, entry)
            except:
                # Connection might be closed
                pass


# Helper function to setup SignLedger with FastAPI
def setup_signledger(
    app: FastAPI,
    backend_type: str = 'memory',
    backend_config: Optional[Dict[str, Any]] = None,
    middleware_config: Optional[Dict[str, Any]] = None,
    include_api: bool = True,
    api_prefix: str = "/audit",
    api_dependencies: Optional[List[Depends]] = None
) -> FastAPISignLedger:
    """Complete setup of SignLedger for FastAPI."""
    # Create SignLedger instance
    signledger = FastAPISignLedger()
    
    # Initialize with app
    signledger.init_app(
        app,
        backend_type=backend_type,
        backend_config=backend_config,
        middleware_config=middleware_config
    )
    
    # Add API routes if requested
    if include_api:
        audit_router = create_audit_router(
            prefix=api_prefix,
            dependencies=api_dependencies
        )
        app.include_router(audit_router)
    
    return signledger
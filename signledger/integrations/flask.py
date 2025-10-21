"""Flask integration for SignLedger."""

import json
import logging
import threading
from typing import Any, Dict, Optional, List, Callable
from functools import wraps
from datetime import datetime
import uuid

try:
    from flask import Flask, request, g, current_app, jsonify
    from flask.signals import request_started, request_finished
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False

from ..core.ledger import Ledger
from ..core.entry import Entry
from ..backends.memory import MemoryBackend
from ..backends.sqlite import SQLiteBackend

logger = logging.getLogger(__name__)


class FlaskSignLedger:
    """Flask extension for SignLedger audit logging."""
    
    def __init__(self, app: Optional['Flask'] = None, ledger: Optional[Ledger] = None):
        self.ledger = ledger
        self._app = None
        self._lock = threading.RLock()
        self._request_entries = {}  # Track entries per request
        
        # Configuration
        self.audit_methods = ['POST', 'PUT', 'DELETE', 'PATCH']
        self.exclude_patterns = ['/static/', '/health', '/metrics']
        self.include_request_body = True
        self.include_response_body = False
        self.max_body_size = 10000  # Max size to log
        
        if app is not None:
            self.init_app(app, ledger)
    
    def init_app(self, app: 'Flask', ledger: Optional[Ledger] = None) -> None:
        """Initialize the Flask application."""
        if not HAS_FLASK:
            raise ImportError("Flask is required. Install with: pip install flask")
        
        self._app = app
        
        # Initialize ledger if not provided
        if ledger:
            self.ledger = ledger
        elif not self.ledger:
            # Create default ledger from config
            self.ledger = self._create_ledger_from_config(app.config)
        
        # Store reference in app extensions
        app.extensions['signledger'] = self
        
        # Load configuration
        self._load_config(app.config)
        
        # Register handlers
        app.before_request(self._before_request)
        app.after_request(self._after_request)
        app.teardown_appcontext(self._teardown)
        
        # Add to app context
        app.signledger = self
    
    def _create_ledger_from_config(self, config: dict) -> Ledger:
        """Create ledger from Flask config."""
        backend_type = config.get('PYLEDGER_BACKEND', 'memory')
        ledger_name = config.get('PYLEDGER_NAME', 'flask_audit')
        
        if backend_type == 'sqlite':
            db_path = config.get('PYLEDGER_SQLITE_PATH', 'flask_audit.db')
            backend = SQLiteBackend(database_path=db_path)
        else:
            backend = MemoryBackend()
        
        return Ledger(name=ledger_name, storage=backend)
    
    def _load_config(self, config: dict) -> None:
        """Load configuration from Flask config."""
        self.audit_methods = config.get('PYLEDGER_AUDIT_METHODS', self.audit_methods)
        self.exclude_patterns = config.get('PYLEDGER_EXCLUDE_PATTERNS', self.exclude_patterns)
        self.include_request_body = config.get('PYLEDGER_INCLUDE_REQUEST_BODY', self.include_request_body)
        self.include_response_body = config.get('PYLEDGER_INCLUDE_RESPONSE_BODY', self.include_response_body)
        self.max_body_size = config.get('PYLEDGER_MAX_BODY_SIZE', self.max_body_size)
    
    def _before_request(self) -> None:
        """Handler called before each request."""
        # Generate request ID
        g.request_id = str(uuid.uuid4())
        g.audit_start_time = datetime.utcnow()
        
        # Check if we should audit this request
        if self._should_audit_request():
            # Store initial request data
            g.audit_data = {
                'request_id': g.request_id,
                'type': 'http_request',
                'method': request.method,
                'path': request.path,
                'endpoint': request.endpoint,
                'remote_addr': request.remote_addr,
                'user_agent': request.headers.get('User-Agent'),
                'timestamp': g.audit_start_time.isoformat(),
            }
            
            # Add user info if available
            if hasattr(g, 'user') and g.user:
                g.audit_data['user'] = str(g.user)
            
            # Add request body if configured
            if self.include_request_body and request.data:
                try:
                    if request.is_json:
                        g.audit_data['request_body'] = request.get_json()
                    else:
                        body = request.data[:self.max_body_size]
                        g.audit_data['request_body'] = body.decode('utf-8', errors='ignore')
                except Exception as e:
                    logger.debug(f"Failed to capture request body: {e}")
    
    def _after_request(self, response):
        """Handler called after each request."""
        if hasattr(g, 'audit_data') and g.audit_data:
            # Add response data
            g.audit_data['status_code'] = response.status_code
            g.audit_data['duration_ms'] = int((datetime.utcnow() - g.audit_start_time).total_seconds() * 1000)
            
            # Add response body if configured
            if self.include_response_body and response.data:
                try:
                    if response.is_json:
                        g.audit_data['response_body'] = response.get_json()
                    else:
                        body = response.data[:self.max_body_size]
                        g.audit_data['response_body'] = body.decode('utf-8', errors='ignore')
                except Exception as e:
                    logger.debug(f"Failed to capture response body: {e}")
            
            # Create audit entry
            try:
                entry = self.ledger.append(g.audit_data)
                
                # Add audit header to response
                response.headers['X-Audit-ID'] = g.request_id
                response.headers['X-Audit-Sequence'] = str(entry.sequence)
                
                # Track for request context
                self._request_entries[g.request_id] = entry
                
            except Exception as e:
                logger.error(f"Failed to create audit entry: {e}")
        
        return response
    
    def _teardown(self, exception: Optional[Exception] = None) -> None:
        """Clean up request context."""
        if hasattr(g, 'request_id'):
            # Clean up tracked entries after some time
            # In production, you might want a background task for this
            self._request_entries.pop(g.request_id, None)
    
    def _should_audit_request(self) -> bool:
        """Check if current request should be audited."""
        # Check method
        if request.method not in self.audit_methods:
            return False
        
        # Check excluded paths
        for pattern in self.exclude_patterns:
            if pattern in request.path:
                return False
        
        return True
    
    def audit_event(self, event_type: str, data: Dict[str, Any], **metadata) -> Entry:
        """Manually create an audit entry."""
        audit_data = {
            'type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': data,
        }
        
        # Add request context if available
        if hasattr(g, 'request_id'):
            audit_data['request_id'] = g.request_id
        
        if hasattr(g, 'user') and g.user:
            audit_data['user'] = str(g.user)
        
        # Add metadata
        if metadata:
            audit_data['metadata'] = metadata
        
        return self.ledger.append(audit_data)
    
    def get_request_audit(self, request_id: str) -> Optional[Entry]:
        """Get audit entry for a request ID."""
        return self._request_entries.get(request_id)
    
    def search_audit(self, **criteria) -> List[Entry]:
        """Search audit entries."""
        return self.ledger.search(criteria)
    
    def verify_integrity(self, start: int = 0, end: Optional[int] = None) -> Tuple[List[int], List[int]]:
        """Verify ledger integrity."""
        if end is None:
            latest = self.ledger.get_latest()
            end = latest.sequence if latest else 0
        
        return self.ledger.verify_range(start, end)


# Decorators
def audit_action(action_type: str, include_args: bool = True, include_result: bool = False):
    """Decorator to audit function calls."""
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not HAS_FLASK:
                return f(*args, **kwargs)
            
            signledger = current_app.extensions.get('signledger')
            if not signledger:
                return f(*args, **kwargs)
            
            # Prepare audit data
            audit_data = {
                'function': f.__name__,
                'module': f.__module__,
            }
            
            if include_args:
                # Safely serialize args/kwargs
                try:
                    audit_data['args'] = str(args)[:1000]
                    audit_data['kwargs'] = str(kwargs)[:1000]
                except:
                    pass
            
            # Execute function
            error = None
            result = None
            
            try:
                result = f(*args, **kwargs)
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
                signledger.audit_event(action_type, audit_data)
            
            return result
        
        return wrapped
    return decorator


def require_audit_trail(f: Callable) -> Callable:
    """Decorator to ensure audit trail exists for sensitive operations."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not HAS_FLASK:
            raise RuntimeError("Flask is required for audit trail")
        
        signledger = current_app.extensions.get('signledger')
        if not signledger:
            raise RuntimeError("SignLedger not initialized")
        
        # Verify ledger integrity before proceeding
        latest = signledger.ledger.get_latest()
        if latest:
            # Quick integrity check of last few entries
            valid, invalid = signledger.verify_integrity(
                max(0, latest.sequence - 10),
                latest.sequence
            )
            
            if invalid:
                raise RuntimeError("Audit trail integrity compromised")
        
        return f(*args, **kwargs)
    
    return wrapped


# Blueprint for audit API
def create_audit_blueprint(url_prefix: str = '/audit', require_auth: Optional[Callable] = None):
    """Create Flask blueprint for audit API."""
    if not HAS_FLASK:
        raise ImportError("Flask is required")
    
    from flask import Blueprint
    
    bp = Blueprint('signledger_audit', __name__, url_prefix=url_prefix)
    
    # Apply auth decorator if provided
    if require_auth:
        bp.before_request(require_auth)
    
    @bp.route('/entries', methods=['GET'])
    def get_entries():
        """Get audit entries with pagination."""
        signledger = current_app.extensions.get('signledger')
        if not signledger:
            return jsonify({'error': 'SignLedger not initialized'}), 500
        
        # Get query parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        
        # Get all entries (in production, implement proper pagination)
        all_entries = signledger.ledger.get_all()
        
        # Paginate
        start = (page - 1) * per_page
        end = start + per_page
        entries = all_entries[start:end]
        
        return jsonify({
            'entries': [e.to_dict() for e in entries],
            'total': len(all_entries),
            'page': page,
            'per_page': per_page,
            'total_pages': (len(all_entries) + per_page - 1) // per_page
        })
    
    @bp.route('/entries/<int:sequence>', methods=['GET'])
    def get_entry(sequence: int):
        """Get specific audit entry."""
        signledger = current_app.extensions.get('signledger')
        if not signledger:
            return jsonify({'error': 'SignLedger not initialized'}), 500
        
        entry = signledger.ledger.get(sequence)
        if not entry:
            return jsonify({'error': 'Entry not found'}), 404
        
        return jsonify(entry.to_dict())
    
    @bp.route('/search', methods=['POST'])
    def search_entries():
        """Search audit entries."""
        signledger = current_app.extensions.get('signledger')
        if not signledger:
            return jsonify({'error': 'SignLedger not initialized'}), 500
        
        criteria = request.get_json() or {}
        
        try:
            entries = signledger.search_audit(**criteria)
            return jsonify({
                'entries': [e.to_dict() for e in entries],
                'count': len(entries)
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 400
    
    @bp.route('/verify', methods=['POST'])
    def verify_integrity():
        """Verify audit trail integrity."""
        signledger = current_app.extensions.get('signledger')
        if not signledger:
            return jsonify({'error': 'SignLedger not initialized'}), 500
        
        data = request.get_json() or {}
        start = data.get('start', 0)
        end = data.get('end')
        
        valid, invalid = signledger.verify_integrity(start, end)
        
        return jsonify({
            'valid_count': len(valid),
            'invalid_count': len(invalid),
            'invalid_sequences': invalid[:100],  # Limit to first 100
            'integrity': len(invalid) == 0
        })
    
    @bp.route('/export', methods=['GET'])
    def export_entries():
        """Export audit entries."""
        signledger = current_app.extensions.get('signledger')
        if not signledger:
            return jsonify({'error': 'SignLedger not initialized'}), 500
        
        # Get format parameter
        format_type = request.args.get('format', 'json')
        
        entries = signledger.ledger.get_all()
        
        if format_type == 'json':
            response = jsonify([e.to_dict() for e in entries])
            response.headers['Content-Disposition'] = 'attachment; filename=audit_export.json'
            return response
        else:
            return jsonify({'error': f'Unsupported format: {format_type}'}), 400
    
    @bp.route('/stats', methods=['GET'])
    def get_stats():
        """Get audit statistics."""
        signledger = current_app.extensions.get('signledger')
        if not signledger:
            return jsonify({'error': 'SignLedger not initialized'}), 500
        
        latest = signledger.ledger.get_latest()
        
        stats = {
            'total_entries': latest.sequence + 1 if latest else 0,
            'latest_sequence': latest.sequence if latest else None,
            'latest_timestamp': latest.timestamp if latest else None,
        }
        
        # Get entry type breakdown
        from collections import Counter
        type_counts = Counter()
        
        for entry in signledger.ledger.get_all():
            entry_type = entry.data.get('type', 'unknown')
            type_counts[entry_type] += 1
        
        stats['entry_types'] = dict(type_counts)
        
        return jsonify(stats)
    
    return bp


# CLI commands
def register_cli_commands(app: 'Flask') -> None:
    """Register Flask CLI commands for SignLedger."""
    if not HAS_FLASK:
        return
    
    @app.cli.group()
    def signledger():
        """SignLedger audit management commands."""
        pass
    
    @signledger.command()
    def verify():
        """Verify audit trail integrity."""
        signledger_ext = app.extensions.get('signledger')
        if not signledger_ext:
            print("SignLedger not initialized")
            return
        
        print("Verifying audit trail integrity...")
        
        latest = signledger_ext.ledger.get_latest()
        if not latest:
            print("No entries to verify")
            return
        
        valid, invalid = signledger_ext.verify_integrity(0, latest.sequence)
        
        print(f"Total entries: {latest.sequence + 1}")
        print(f"Valid entries: {len(valid)}")
        print(f"Invalid entries: {len(invalid)}")
        
        if invalid:
            print("\nInvalid entries (first 10):")
            for seq in invalid[:10]:
                print(f"  - Sequence {seq}")
        else:
            print("\n✓ All entries are valid!")
    
    @signledger.command()
    def stats():
        """Show audit statistics."""
        signledger_ext = app.extensions.get('signledger')
        if not signledger_ext:
            print("SignLedger not initialized")
            return
        
        latest = signledger_ext.ledger.get_latest()
        
        if not latest:
            print("No audit entries")
            return
        
        print(f"Total entries: {latest.sequence + 1}")
        print(f"Latest entry: {latest.timestamp}")
        
        # Get type breakdown
        from collections import Counter
        type_counts = Counter()
        
        for entry in signledger_ext.ledger.get_all():
            entry_type = entry.data.get('type', 'unknown')
            type_counts[entry_type] += 1
        
        print("\nEntry types:")
        for entry_type, count in type_counts.most_common():
            print(f"  {entry_type}: {count}")
    
    @signledger.command()
    def export(output: str = 'audit_export.json'):
        """Export audit entries."""
        signledger_ext = app.extensions.get('signledger')
        if not signledger_ext:
            print("SignLedger not initialized")
            return
        
        entries = signledger_ext.ledger.get_all()
        
        with open(output, 'w') as f:
            json.dump([e.to_dict() for e in entries], f, indent=2)
        
        print(f"Exported {len(entries)} entries to {output}")
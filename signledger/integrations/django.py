"""Django integration for SignLedger."""

import json
import logging
from typing import Any, Dict, Optional, List, Type
from datetime import datetime

try:
    from django.db import models
    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.utils import timezone
    from django.core.serializers.json import DjangoJSONEncoder
    from django.dispatch import Signal
    HAS_DJANGO = True
except ImportError:
    HAS_DJANGO = False
    models = None

from ..core.ledger import Ledger
from ..core.entry import Entry
from ..backends.base import StorageBackend

logger = logging.getLogger(__name__)

# Signals
if HAS_DJANGO:
    entry_created = Signal()  # sender=model_instance, entry=Entry
    entry_verified = Signal()  # sender=model_instance, entry=Entry, valid=bool


class DjangoStorageBackend(StorageBackend):
    """Django ORM-based storage backend for SignLedger."""
    
    def __init__(self, model_class: Optional[Type['models.Model']] = None, **kwargs):
        if not HAS_DJANGO:
            raise ImportError("Django is required. Install with: pip install django")
        
        super().__init__(**kwargs)
        self.model_class = model_class or self._get_default_model()
    
    def _get_default_model(self) -> Type['models.Model']:
        """Get or create default audit log model."""
        from django.apps import apps
        
        try:
            return apps.get_model('signledger', 'AuditEntry')
        except LookupError:
            # Model doesn't exist, will be created by migrations
            return None
    
    def append(self, entry_data: Dict[str, Any]) -> int:
        """Append entry to Django model."""
        if not self.model_class:
            raise RuntimeError("Model class not set. Run migrations first.")
        
        # Create model instance
        instance = self.model_class(
            sequence=entry_data['sequence'],
            timestamp=entry_data['timestamp'],
            data=entry_data['data'],
            hash=entry_data['hash'],
            previous_hash=entry_data.get('previous_hash'),
            metadata=entry_data.get('metadata', {}),
            signature=entry_data.get('signature')
        )
        instance.save()
        
        # Send signal
        entry_created.send(
            sender=instance.__class__,
            instance=instance,
            entry=Entry.from_dict(entry_data)
        )
        
        return instance.sequence
    
    def get(self, sequence: int) -> Optional[Dict[str, Any]]:
        """Get entry by sequence number."""
        if not self.model_class:
            return None
        
        try:
            instance = self.model_class.objects.get(sequence=sequence)
            return self._model_to_dict(instance)
        except self.model_class.DoesNotExist:
            return None
    
    def get_latest(self) -> Optional[Dict[str, Any]]:
        """Get the latest entry."""
        if not self.model_class:
            return None
        
        instance = self.model_class.objects.order_by('-sequence').first()
        if instance:
            return self._model_to_dict(instance)
        return None
    
    def get_range(self, start: int, end: int) -> List[Dict[str, Any]]:
        """Get entries in sequence range."""
        if not self.model_class:
            return []
        
        instances = self.model_class.objects.filter(
            sequence__gte=start,
            sequence__lte=end
        ).order_by('sequence')
        
        return [self._model_to_dict(instance) for instance in instances]
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Get all entries."""
        if not self.model_class:
            return []
        
        instances = self.model_class.objects.order_by('sequence')
        return [self._model_to_dict(instance) for instance in instances]
    
    def search(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search entries by criteria."""
        if not self.model_class:
            return []
        
        queryset = self.model_class.objects.all()
        
        # Apply filters
        if 'data' in criteria:
            # Search in JSON data field
            for key, value in criteria['data'].items():
                queryset = queryset.filter(data__contains={key: value})
        
        if 'start_time' in criteria:
            queryset = queryset.filter(timestamp__gte=criteria['start_time'])
        
        if 'end_time' in criteria:
            queryset = queryset.filter(timestamp__lte=criteria['end_time'])
        
        if 'metadata' in criteria:
            for key, value in criteria['metadata'].items():
                queryset = queryset.filter(metadata__contains={key: value})
        
        return [self._model_to_dict(instance) for instance in queryset.order_by('sequence')]
    
    def _model_to_dict(self, instance: 'models.Model') -> Dict[str, Any]:
        """Convert model instance to dict."""
        return {
            'sequence': instance.sequence,
            'timestamp': instance.timestamp.isoformat() if hasattr(instance.timestamp, 'isoformat') else instance.timestamp,
            'data': instance.data,
            'hash': instance.hash,
            'previous_hash': instance.previous_hash,
            'metadata': instance.metadata or {},
            'signature': instance.signature,
        }
    
    def close(self) -> None:
        """Close Django backend (no-op)."""
        pass


# Django Models
if HAS_DJANGO:
    class AbstractAuditEntry(models.Model):
        """Abstract base model for audit entries."""
        
        sequence = models.BigIntegerField(unique=True, db_index=True)
        timestamp = models.DateTimeField(db_index=True)
        data = models.JSONField(encoder=DjangoJSONEncoder)
        hash = models.CharField(max_length=64, db_index=True)
        previous_hash = models.CharField(max_length=64, null=True, blank=True)
        metadata = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
        signature = models.TextField(null=True, blank=True)
        
        # Optional fields for Django integration
        created_by = models.ForeignKey(
            settings.AUTH_USER_MODEL,
            on_delete=models.SET_NULL,
            null=True,
            blank=True,
            related_name='%(class)s_entries'
        )
        created_at = models.DateTimeField(auto_now_add=True)
        
        class Meta:
            abstract = True
            ordering = ['sequence']
            indexes = [
                models.Index(fields=['timestamp']),
                models.Index(fields=['hash']),
                models.Index(fields=['created_by', 'timestamp']),
            ]
        
        def __str__(self):
            return f"Entry #{self.sequence} - {self.timestamp}"
        
        def to_entry(self) -> Entry:
            """Convert to SignLedger Entry."""
            return Entry.from_dict({
                'sequence': self.sequence,
                'timestamp': self.timestamp,
                'data': self.data,
                'hash': self.hash,
                'previous_hash': self.previous_hash,
                'metadata': self.metadata,
                'signature': self.signature,
            })
        
        def verify(self) -> bool:
            """Verify this entry's integrity."""
            entry = self.to_entry()
            # This would need access to the hash chain
            # Typically done through the ledger instance
            return True  # Placeholder


    class AuditEntry(AbstractAuditEntry):
        """Concrete audit entry model."""
        pass


# Middleware
class AuditMiddleware:
    """Django middleware for automatic audit logging."""
    
    def __init__(self, get_response):
        self.get_response = get_response
        self.ledger = None
        self._init_ledger()
    
    def _init_ledger(self):
        """Initialize ledger from Django settings."""
        if hasattr(settings, 'PYLEDGER_CONFIG'):
            config = settings.PYLEDGER_CONFIG
            backend = DjangoStorageBackend(
                model_class=config.get('model_class', AuditEntry)
            )
            
            self.ledger = Ledger(
                name=config.get('name', 'django_audit'),
                storage=backend
            )
            
            # Configure what to audit
            self.audit_methods = config.get('audit_methods', ['POST', 'PUT', 'DELETE', 'PATCH'])
            self.exclude_paths = config.get('exclude_paths', ['/admin/', '/static/', '/media/'])
            self.include_request_data = config.get('include_request_data', True)
            self.include_response_data = config.get('include_response_data', False)
    
    def __call__(self, request):
        if not self.ledger or not self._should_audit(request):
            return self.get_response(request)
        
        # Capture request data
        audit_data = {
            'type': 'http_request',
            'method': request.method,
            'path': request.path,
            'user': str(request.user) if request.user.is_authenticated else 'anonymous',
            'ip_address': self._get_client_ip(request),
            'timestamp': timezone.now().isoformat(),
        }
        
        if self.include_request_data and request.body:
            try:
                audit_data['request_data'] = json.loads(request.body)
            except:
                audit_data['request_data'] = request.body[:1000].decode('utf-8', errors='ignore')
        
        # Process request
        response = self.get_response(request)
        
        # Add response data
        audit_data['status_code'] = response.status_code
        
        if self.include_response_data and hasattr(response, 'content'):
            try:
                audit_data['response_data'] = json.loads(response.content)
            except:
                pass
        
        # Create audit entry
        try:
            entry = self.ledger.append(audit_data)
            
            # Add entry ID to response headers
            response['X-Audit-Entry-ID'] = str(entry.sequence)
            
        except Exception as e:
            logger.error(f"Failed to create audit entry: {e}")
        
        return response
    
    def _should_audit(self, request) -> bool:
        """Check if request should be audited."""
        # Check method
        if request.method not in self.audit_methods:
            return False
        
        # Check excluded paths
        for path in self.exclude_paths:
            if request.path.startswith(path):
                return False
        
        return True
    
    def _get_client_ip(self, request) -> str:
        """Get client IP address."""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip


# Management commands
def create_management_commands():
    """Create Django management commands for SignLedger."""
    if not HAS_DJANGO:
        return
    
    from django.core.management.base import BaseCommand
    
    class Command(BaseCommand):
        help = 'SignLedger audit log management'
        
        def add_arguments(self, parser):
            parser.add_argument(
                'action',
                choices=['verify', 'export', 'stats'],
                help='Action to perform'
            )
            parser.add_argument(
                '--start',
                type=int,
                help='Start sequence number'
            )
            parser.add_argument(
                '--end',
                type=int,
                help='End sequence number'
            )
            parser.add_argument(
                '--output',
                help='Output file for export'
            )
        
        def handle(self, *args, **options):
            # Initialize ledger
            from django.apps import apps
            model = apps.get_model('signledger', 'AuditEntry')
            backend = DjangoStorageBackend(model_class=model)
            ledger = Ledger(name='django_audit', storage=backend)
            
            action = options['action']
            
            if action == 'verify':
                self._verify_ledger(ledger, options)
            elif action == 'export':
                self._export_ledger(ledger, options)
            elif action == 'stats':
                self._show_stats(ledger)
        
        def _verify_ledger(self, ledger, options):
            """Verify ledger integrity."""
            start = options.get('start', 0)
            end = options.get('end')
            
            if end is None:
                latest = ledger.get_latest()
                end = latest.sequence if latest else 0
            
            self.stdout.write(f"Verifying entries {start} to {end}...")
            
            valid, invalid = ledger.verify_range(start, end)
            
            self.stdout.write(
                self.style.SUCCESS(f"Valid entries: {len(valid)}")
            )
            
            if invalid:
                self.stdout.write(
                    self.style.ERROR(f"Invalid entries: {len(invalid)}")
                )
                for seq in invalid[:10]:  # Show first 10
                    self.stdout.write(f"  - Sequence {seq}")
            else:
                self.stdout.write(
                    self.style.SUCCESS("All entries are valid!")
                )
        
        def _export_ledger(self, ledger, options):
            """Export ledger entries."""
            output_file = options.get('output', 'audit_export.json')
            
            entries = ledger.get_all()
            
            with open(output_file, 'w') as f:
                json.dump(
                    [e.to_dict() for e in entries],
                    f,
                    indent=2,
                    cls=DjangoJSONEncoder
                )
            
            self.stdout.write(
                self.style.SUCCESS(f"Exported {len(entries)} entries to {output_file}")
            )
        
        def _show_stats(self, ledger):
            """Show ledger statistics."""
            latest = ledger.get_latest()
            
            if not latest:
                self.stdout.write("No entries in ledger")
                return
            
            total = latest.sequence + 1
            
            # Get date range
            first = ledger.get(0)
            
            self.stdout.write(f"Total entries: {total}")
            self.stdout.write(f"First entry: {first.timestamp if first else 'N/A'}")
            self.stdout.write(f"Latest entry: {latest.timestamp}")
            
            # Show entry types if available
            from collections import Counter
            types = Counter()
            
            for entry in ledger.get_all():
                entry_type = entry.data.get('type', 'unknown')
                types[entry_type] += 1
            
            self.stdout.write("\nEntry types:")
            for entry_type, count in types.most_common():
                self.stdout.write(f"  {entry_type}: {count}")
    
    return Command


# Template tags
def create_template_tags():
    """Create Django template tags for SignLedger."""
    if not HAS_DJANGO:
        return {}
    
    from django import template
    
    register = template.Library()
    
    @register.simple_tag
    def audit_entry_count():
        """Get total audit entry count."""
        from django.apps import apps
        try:
            model = apps.get_model('signledger', 'AuditEntry')
            return model.objects.count()
        except:
            return 0
    
    @register.simple_tag
    def user_audit_entries(user):
        """Get audit entries for a user."""
        from django.apps import apps
        try:
            model = apps.get_model('signledger', 'AuditEntry')
            return model.objects.filter(created_by=user).order_by('-timestamp')[:10]
        except:
            return []
    
    @register.filter
    def verify_entry(entry):
        """Verify an audit entry."""
        try:
            return entry.verify()
        except:
            return False
    
    return {
        'audit_entry_count': audit_entry_count,
        'user_audit_entries': user_audit_entries,
        'verify_entry': verify_entry,
    }
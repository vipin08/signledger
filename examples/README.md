# SignLedger Examples

This directory contains comprehensive real-world examples demonstrating SignLedger usage across different industries and use cases.

## Quick Start

```bash
# Install SignLedger from PyPI
pip install signledger

# Run any example
python examples/flask_banking_app.py
```

## Examples Overview

### 1. Flask Banking Application

**File**: `flask_banking_app.py`

A production-ready RESTful API for banking operations with complete audit trail.

**Features**:
- Account creation and management
- Money transfers with validation
- Transaction history
- Audit trail with cryptographic signatures
- Compliance reporting
- Integrity verification API

**Run**:
```bash
python examples/flask_banking_app.py
```

**Test**:
```bash
# In another terminal
python examples/test_flask_app.py
```

**API Endpoints**:
- `POST /api/accounts` - Create account
- `GET /api/accounts/<id>` - Get account details
- `POST /api/transactions/transfer` - Transfer money
- `GET /api/transactions/<account_id>` - Get transactions
- `GET /api/audit/verify` - Verify ledger integrity
- `GET /api/audit/compliance-report` - Generate compliance report

---

### 2. Healthcare HIPAA Compliance

**File**: `real_world/healthcare_hipaa_compliance.py`

HIPAA-compliant audit logging for healthcare applications tracking Protected Health Information (PHI) access.

**Features**:
- PHI access logging with signatures
- User authentication tracking
- Data modification audit trail
- Patient data export logs
- HIPAA compliance reporting
- Complete patient access history

**Run**:
```bash
python examples/real_world/healthcare_hipaa_compliance.py
```

**Use Cases**:
- Electronic Health Records (EHR) systems
- Medical practice management software
- Hospital information systems
- Healthcare data exchanges

**Compliance Coverage**:
- 45 CFR 164.308(a)(1)(ii)(D) - Access audit logs
- 45 CFR 164.312(a)(2)(i) - User authentication
- 45 CFR 164.508 - Authorization tracking

---

### 3. E-Commerce Order Tracking

**File**: `real_world/ecommerce_order_tracking.py`

Complete order lifecycle tracking for e-commerce platforms.

**Features**:
- Order placement and status tracking
- Payment processing logs
- Inventory management audit
- Shipment tracking
- Returns and refunds logging
- Customer activity tracking
- Sales reporting

**Run**:
```bash
python examples/real_world/ecommerce_order_tracking.py
```

**Use Cases**:
- Online retail platforms
- Marketplace applications
- Dropshipping systems
- Subscription services

**Tracked Events**:
- `ORDER_CREATED` - New order placement
- `PAYMENT_PROCESSED` - Payment confirmation
- `INVENTORY_UPDATE` - Stock changes
- `ORDER_STATUS_CHANGE` - Order lifecycle
- `SHIPMENT_CREATED` - Shipping initiated
- `SHIPMENT_STATUS` - Tracking updates
- `RETURN_REQUEST` - Return requests
- `REFUND_PROCESSED` - Refund completion
- `CUSTOMER_ACTIVITY` - User actions

---

## Common Patterns

### Basic Usage

```python
from signledger import Ledger
from signledger.backends import SQLiteBackend

# Create ledger
ledger = Ledger(backend=SQLiteBackend("audit.db"))

# Add entry
entry = ledger.append({
    "action": "user_login",
    "user_id": "user123",
    "ip": "192.168.1.1"
})

# Verify integrity
is_valid = ledger.verify_integrity()

# Query entries
entries = ledger.query(
    lambda e: e.data.get("user_id") == "user123"
)

# Close ledger
ledger.close()
```

### With Cryptographic Signatures

```python
from signledger.crypto.signatures import RSASigner

# Initialize signer
signer = RSASigner(key_size=2048)

# Create wrapper for string-to-bytes conversion
def sign_wrapper(data_str):
    if isinstance(data_str, str):
        data_str = data_str.encode('utf-8')
    return signer.sign(data_str)

# Append signed entry
entry = ledger.append(
    {"transaction": "critical_operation"},
    sign=True,
    signer=sign_wrapper
)

# Verify signature
is_valid = signer.verify(
    entry.hash.encode('utf-8'),
    entry.signature
)
```

### Context Manager

```python
with Ledger(backend=SQLiteBackend("audit.db")) as ledger:
    ledger.append({"event": "process_started"})
    # Process data
    ledger.append({"event": "process_completed"})
# Ledger automatically closed
```

## Integration Examples

### Django Middleware

```python
from signledger import Ledger
from signledger.backends import PostgreSQLBackend

class AuditMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self.ledger = Ledger(
            backend=PostgreSQLBackend(
                host="localhost",
                database="audit_db"
            )
        )

    def __call__(self, request):
        # Log request
        self.ledger.append({
            "type": "http_request",
            "method": request.method,
            "path": request.path,
            "user": str(request.user)
        })

        response = self.get_response(request)
        return response
```

### Flask Decorator

```python
from functools import wraps
from flask import request

def audit_log(event_type):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Log before execution
            ledger.append({
                "event": event_type,
                "endpoint": request.endpoint,
                "user": get_current_user()
            })
            return f(*args, **kwargs)
        return wrapper
    return decorator

@app.route('/api/critical-operation')
@audit_log("critical_operation")
def critical_operation():
    # Your code here
    pass
```

### FastAPI Dependency

```python
from fastapi import Depends
from signledger import Ledger

async def get_ledger():
    ledger = Ledger(backend=SQLiteBackend("audit.db"))
    try:
        yield ledger
    finally:
        ledger.close()

@app.post("/api/action")
async def perform_action(
    ledger: Ledger = Depends(get_ledger)
):
    entry = await ledger.append_async({
        "action": "api_call",
        "timestamp": datetime.utcnow().isoformat()
    })
    return {"entry_id": entry.id}
```

## Performance Tips

1. **Use Batch Operations** for high-throughput scenarios:
   ```python
   from signledger.core.batch import BatchProcessor

   batch = BatchProcessor(ledger, batch_size=1000)
   for i in range(10000):
       batch.add_entry({"transaction": i})
   batch.flush()
   ```

2. **Enable Compression** for large-scale deployments:
   ```python
   from signledger.compression import Compressor

   ledger = Ledger(
       backend=SQLiteBackend("audit.db"),
       compression=Compressor(algorithm="zstd")
   )
   ```

3. **Use Connection Pooling** for concurrent access:
   ```python
   from signledger.pool import ConnectionPool

   pool = ConnectionPool(
       backend_class=PostgreSQLBackend,
       max_connections=10,
       host="localhost",
       database="audit_db"
   )
   ```

## Testing

All examples include comprehensive testing:

```bash
# Run all tests
python -m pytest examples/

# Run specific test
python examples/test_flask_app.py
```

## Database Files

Examples create SQLite database files in the current directory:
- `banking_audit.db` - Flask banking app
- `hipaa_demo.db` - Healthcare compliance
- `ecommerce_demo.db` - E-commerce tracking

These can be deleted after testing.

## Production Deployment

For production use:

1. **Use PostgreSQL or MongoDB** instead of SQLite
2. **Enable cryptographic signatures** for critical events
3. **Set up automated integrity verification**
4. **Configure proper backup strategies**
5. **Implement access controls** for audit logs
6. **Set up monitoring and alerting**

## Support

- GitHub: https://github.com/vipin08/signledger
- PyPI: https://pypi.org/project/signledger/
- Issues: https://github.com/vipin08/signledger/issues

## License

MIT License - See LICENSE file for details

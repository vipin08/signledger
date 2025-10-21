"""
Real-World Flask Banking Application with SignLedger Audit Trail

This application demonstrates:
- Immutable audit logging for financial transactions
- Account management with full audit history
- Transaction verification and compliance reporting
- RESTful API with comprehensive logging
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from flask import Flask, request, jsonify
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from signledger import Ledger
from signledger.backends import SQLiteBackend
from signledger.crypto.signatures import RSASigner

app = Flask(__name__)

# Initialize SignLedger with SQLite backend
ledger = Ledger(
    backend=SQLiteBackend("banking_audit.db"),
    enable_signatures=True,
    auto_verify=True,
    verify_interval=3600  # Verify every hour
)

# Initialize RSA signer for critical transactions
signer = RSASigner(key_size=2048)

# In-memory database (for demo purposes)
accounts = {}
transactions = []


class Account:
    """Simple account model"""

    def __init__(self, account_id, owner, balance=0.0):
        self.account_id = account_id
        self.owner = owner
        self.balance = Decimal(str(balance))
        self.created_at = datetime.now(timezone.utc)


# Helper functions
def log_audit_entry(event_type, data, sign=False):
    """Log an entry to the audit ledger"""
    audit_data = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **data
    }

    if sign:
        entry = ledger.append(
            data=audit_data,
            metadata={"critical": True, "compliance": "SOX"},
            sign=True,
            signer=signer.sign
        )
    else:
        entry = ledger.append(data=audit_data)

    return entry


def get_account(account_id):
    """Get account by ID"""
    return accounts.get(account_id)


# API Routes

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    stats = ledger.get_stats()

    return jsonify({
        "status": "healthy",
        "ledger": {
            "total_entries": stats.total_entries,
            "integrity": stats.integrity_verified,
            "last_verification": stats.last_verification_time.isoformat() if stats.last_verification_time else None
        }
    })


@app.route('/api/accounts', methods=['POST'])
def create_account():
    """Create a new bank account"""
    data = request.json

    if not data or 'owner' not in data:
        return jsonify({"error": "Owner name is required"}), 400

    account_id = str(uuid.uuid4())
    initial_balance = float(data.get('initial_balance', 0.0))

    if initial_balance < 0:
        return jsonify({"error": "Initial balance cannot be negative"}), 400

    # Create account
    account = Account(account_id, data['owner'], initial_balance)
    accounts[account_id] = account

    # Log to audit trail
    log_audit_entry("ACCOUNT_CREATED", {
        "account_id": account_id,
        "owner": data['owner'],
        "initial_balance": float(initial_balance),
        "ip_address": request.remote_addr
    })

    return jsonify({
        "account_id": account_id,
        "owner": account.owner,
        "balance": float(account.balance),
        "created_at": account.created_at.isoformat()
    }), 201


@app.route('/api/accounts/<account_id>', methods=['GET'])
def get_account_details(account_id):
    """Get account details"""
    account = get_account(account_id)

    if not account:
        return jsonify({"error": "Account not found"}), 404

    # Log access
    log_audit_entry("ACCOUNT_ACCESSED", {
        "account_id": account_id,
        "accessed_by": request.remote_addr
    })

    return jsonify({
        "account_id": account.account_id,
        "owner": account.owner,
        "balance": float(account.balance),
        "created_at": account.created_at.isoformat()
    })


@app.route('/api/transactions/transfer', methods=['POST'])
def transfer_money():
    """Transfer money between accounts"""
    data = request.json

    # Validate input
    required_fields = ['from_account', 'to_account', 'amount']
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    from_account_id = data['from_account']
    to_account_id = data['to_account']
    amount = Decimal(str(data['amount']))

    if amount <= 0:
        return jsonify({"error": "Amount must be positive"}), 400

    # Get accounts
    from_account = get_account(from_account_id)
    to_account = get_account(to_account_id)

    if not from_account:
        return jsonify({"error": "Source account not found"}), 404

    if not to_account:
        return jsonify({"error": "Destination account not found"}), 404

    # Check balance
    if from_account.balance < amount:
        # Log failed attempt
        log_audit_entry("TRANSFER_FAILED", {
            "from_account": from_account_id,
            "to_account": to_account_id,
            "amount": float(amount),
            "reason": "insufficient_funds",
            "ip_address": request.remote_addr
        })

        return jsonify({"error": "Insufficient funds"}), 400

    # Perform transfer
    transaction_id = str(uuid.uuid4())
    from_account.balance -= amount
    to_account.balance += amount

    transaction_data = {
        "transaction_id": transaction_id,
        "from_account": from_account_id,
        "from_owner": from_account.owner,
        "to_account": to_account_id,
        "to_owner": to_account.owner,
        "amount": float(amount),
        "description": data.get('description', ''),
        "ip_address": request.remote_addr
    }

    transactions.append(transaction_data)

    # Log to audit trail with signature (critical transaction)
    entry = log_audit_entry("TRANSFER_COMPLETED", transaction_data, sign=True)

    return jsonify({
        "transaction_id": transaction_id,
        "from_account": from_account_id,
        "to_account": to_account_id,
        "amount": float(amount),
        "timestamp": entry.timestamp.isoformat(),
        "audit_entry_id": entry.id,
        "audit_hash": entry.hash,
        "signed": True
    }), 201


@app.route('/api/transactions/<account_id>', methods=['GET'])
def get_account_transactions(account_id):
    """Get all transactions for an account"""
    account = get_account(account_id)

    if not account:
        return jsonify({"error": "Account not found"}), 404

    # Get transactions from audit ledger
    account_txns = ledger.query(
        lambda e: (
            e.data.get("event_type") == "TRANSFER_COMPLETED" and
            (e.data.get("from_account") == account_id or e.data.get("to_account") == account_id)
        )
    )

    # Log query
    log_audit_entry("TRANSACTION_QUERY", {
        "account_id": account_id,
        "results_count": len(account_txns),
        "ip_address": request.remote_addr
    })

    return jsonify({
        "account_id": account_id,
        "transactions": [
            {
                "transaction_id": e.data.get("transaction_id"),
                "from_account": e.data.get("from_account"),
                "to_account": e.data.get("to_account"),
                "amount": e.data.get("amount"),
                "timestamp": e.timestamp.isoformat(),
                "audit_hash": e.hash,
                "signed": e.signature is not None
            }
            for e in account_txns
        ]
    })


@app.route('/api/audit/verify', methods=['GET'])
def verify_ledger_integrity():
    """Verify the entire ledger integrity"""
    try:
        is_valid = ledger.verify_integrity()
        stats = ledger.get_stats()

        # Log verification
        log_audit_entry("INTEGRITY_VERIFICATION", {
            "result": "VALID" if is_valid else "INVALID",
            "total_entries": stats.total_entries,
            "ip_address": request.remote_addr
        })

        return jsonify({
            "integrity": "VALID" if is_valid else "INVALID",
            "total_entries": stats.total_entries,
            "first_entry": stats.first_entry_time.isoformat() if stats.first_entry_time else None,
            "last_entry": stats.last_entry_time.isoformat() if stats.last_entry_time else None,
            "algorithm": stats.hash_algorithm
        })

    except Exception as e:
        return jsonify({
            "error": "Integrity verification failed",
            "detail": str(e)
        }), 500


@app.route('/api/audit/entries', methods=['GET'])
def get_audit_entries():
    """Get recent audit entries"""
    limit = int(request.args.get('limit', 100))
    event_type = request.args.get('event_type')

    if event_type:
        # Filter by event type
        entries = ledger.query(
            lambda e: e.data.get("event_type") == event_type,
            limit=limit
        )
    else:
        # Get all recent entries
        entries = list(ledger.get_entries(limit=limit))

    return jsonify({
        "total": len(entries),
        "entries": [
            {
                "id": e.id,
                "timestamp": e.timestamp.isoformat(),
                "event_type": e.data.get("event_type"),
                "data": e.data,
                "hash": e.hash,
                "previous_hash": e.previous_hash,
                "signed": e.signature is not None
            }
            for e in entries
        ]
    })


@app.route('/api/audit/compliance-report', methods=['GET'])
def compliance_report():
    """Generate compliance report"""
    stats = ledger.get_stats()

    # Count events by type
    all_entries = list(ledger.get_entries())
    event_counts = {}

    for entry in all_entries:
        event_type = entry.data.get("event_type", "UNKNOWN")
        event_counts[event_type] = event_counts.get(event_type, 0) + 1

    # Count signed transactions
    signed_count = sum(1 for e in all_entries if e.signature is not None)

    # Get failed transactions
    failed_transfers = ledger.query(
        lambda e: e.data.get("event_type") == "TRANSFER_FAILED"
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_audit_entries": stats.total_entries,
        "first_entry": stats.first_entry_time.isoformat() if stats.first_entry_time else None,
        "last_entry": stats.last_entry_time.isoformat() if stats.last_entry_time else None,
        "integrity_status": "VALID" if stats.integrity_verified else "UNKNOWN",
        "events_by_type": event_counts,
        "signed_transactions": signed_count,
        "failed_transactions": len(failed_transfers),
        "total_accounts": len(accounts),
        "hash_algorithm": stats.hash_algorithm
    }

    # Log report generation
    log_audit_entry("COMPLIANCE_REPORT_GENERATED", {
        "ip_address": request.remote_addr
    })

    return jsonify(report)


@app.route('/api/accounts/<account_id>/audit-trail', methods=['GET'])
def account_audit_trail(account_id):
    """Get complete audit trail for an account"""
    account = get_account(account_id)

    if not account:
        return jsonify({"error": "Account not found"}), 404

    # Get all entries related to this account
    account_entries = ledger.query(
        lambda e: (
            e.data.get("account_id") == account_id or
            e.data.get("from_account") == account_id or
            e.data.get("to_account") == account_id
        )
    )

    # Log audit trail access
    log_audit_entry("AUDIT_TRAIL_ACCESSED", {
        "account_id": account_id,
        "entries_count": len(account_entries),
        "ip_address": request.remote_addr
    })

    return jsonify({
        "account_id": account_id,
        "owner": account.owner,
        "current_balance": float(account.balance),
        "audit_trail": [
            {
                "entry_id": e.id,
                "timestamp": e.timestamp.isoformat(),
                "event_type": e.data.get("event_type"),
                "data": e.data,
                "hash": e.hash,
                "signed": e.signature is not None,
                "verified": ledger.verify_entry(e.id)
            }
            for e in account_entries
        ]
    })


@app.before_request
def log_request():
    """Log all API requests"""
    if request.endpoint and not request.endpoint.startswith('static'):
        log_audit_entry("API_REQUEST", {
            "method": request.method,
            "endpoint": request.endpoint,
            "path": request.path,
            "ip_address": request.remote_addr,
            "user_agent": request.user_agent.string[:200] if request.user_agent else None
        })


@app.after_request
def log_response(response):
    """Log API responses"""
    if request.endpoint and not request.endpoint.startswith('static'):
        log_audit_entry("API_RESPONSE", {
            "method": request.method,
            "endpoint": request.endpoint,
            "status_code": response.status_code,
            "ip_address": request.remote_addr
        })

    return response


if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("🏦 Banking Application with SignLedger Audit Trail")
    print("=" * 70)
    print("\nAvailable endpoints:")
    print("  POST   /api/accounts                    - Create account")
    print("  GET    /api/accounts/<id>               - Get account details")
    print("  POST   /api/transactions/transfer       - Transfer money")
    print("  GET    /api/transactions/<account_id>   - Get account transactions")
    print("  GET    /api/audit/verify                - Verify ledger integrity")
    print("  GET    /api/audit/entries               - Get audit entries")
    print("  GET    /api/audit/compliance-report     - Generate compliance report")
    print("  GET    /api/accounts/<id>/audit-trail   - Get account audit trail")
    print("  GET    /api/health                      - Health check")
    print("\nServer starting on http://127.0.0.1:5000")
    print("=" * 70 + "\n")

    try:
        app.run(debug=True, host='0.0.0.0', port=5000)
    finally:
        # Close ledger on shutdown
        ledger.close()
        print("\n✅ Ledger closed successfully")

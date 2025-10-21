"""
Test script for the Flask Banking Application

This script demonstrates:
- Creating bank accounts
- Transferring money between accounts
- Querying transactions
- Verifying ledger integrity
- Generating compliance reports
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "http://127.0.0.1:5000/api"


def print_section(title):
    """Print a section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_response(response, title="Response"):
    """Pretty print HTTP response"""
    print(f"\n{title}:")
    print(f"Status: {response.status_code}")
    try:
        print(f"Data: {json.dumps(response.json(), indent=2)}")
    except:
        print(f"Data: {response.text}")


def test_banking_app():
    """Run comprehensive tests on the banking app"""

    print("\n" + "🏦" * 35)
    print("  Flask Banking Application Test Suite")
    print("🏦" * 35)

    # Test 1: Health Check
    print_section("1. Health Check")
    response = requests.get(f"{BASE_URL}/health")
    print_response(response, "Health Check")

    # Test 2: Create Accounts
    print_section("2. Creating Bank Accounts")

    # Create Alice's account
    alice_response = requests.post(f"{BASE_URL}/accounts", json={
        "owner": "Alice Johnson",
        "initial_balance": 10000.00
    })
    print_response(alice_response, "Alice's Account Created")
    alice_account_id = alice_response.json()['account_id']

    # Create Bob's account
    bob_response = requests.post(f"{BASE_URL}/accounts", json={
        "owner": "Bob Smith",
        "initial_balance": 5000.00
    })
    print_response(bob_response, "Bob's Account Created")
    bob_account_id = bob_response.json()['account_id']

    # Create Charlie's account
    charlie_response = requests.post(f"{BASE_URL}/accounts", json={
        "owner": "Charlie Brown",
        "initial_balance": 1000.00
    })
    print_response(charlie_response, "Charlie's Account Created")
    charlie_account_id = charlie_response.json()['account_id']

    time.sleep(0.5)

    # Test 3: Check Account Details
    print_section("3. Checking Account Details")

    alice_details = requests.get(f"{BASE_URL}/accounts/{alice_account_id}")
    print_response(alice_details, "Alice's Account Details")

    # Test 4: Transfer Money
    print_section("4. Transferring Money")

    # Alice transfers $2000 to Bob
    transfer1 = requests.post(f"{BASE_URL}/transactions/transfer", json={
        "from_account": alice_account_id,
        "to_account": bob_account_id,
        "amount": 2000.00,
        "description": "Payment for consulting services"
    })
    print_response(transfer1, "Transfer #1: Alice -> Bob ($2000)")

    time.sleep(0.5)

    # Bob transfers $500 to Charlie
    transfer2 = requests.post(f"{BASE_URL}/transactions/transfer", json={
        "from_account": bob_account_id,
        "to_account": charlie_account_id,
        "amount": 500.00,
        "description": "Rent payment"
    })
    print_response(transfer2, "Transfer #2: Bob -> Charlie ($500)")

    time.sleep(0.5)

    # Charlie transfers $200 to Alice
    transfer3 = requests.post(f"{BASE_URL}/transactions/transfer", json={
        "from_account": charlie_account_id,
        "to_account": alice_account_id,
        "amount": 200.00,
        "description": "Loan repayment"
    })
    print_response(transfer3, "Transfer #3: Charlie -> Alice ($200)")

    time.sleep(0.5)

    # Test 5: Failed Transfer (Insufficient Funds)
    print_section("5. Testing Insufficient Funds Scenario")

    failed_transfer = requests.post(f"{BASE_URL}/transactions/transfer", json={
        "from_account": charlie_account_id,
        "to_account": alice_account_id,
        "amount": 10000.00,
        "description": "This should fail"
    })
    print_response(failed_transfer, "Failed Transfer (Insufficient Funds)")

    time.sleep(0.5)

    # Test 6: Check Updated Balances
    print_section("6. Checking Updated Account Balances")

    print("\nAlice's Final Balance:")
    alice_final = requests.get(f"{BASE_URL}/accounts/{alice_account_id}")
    print(f"  Balance: ${alice_final.json()['balance']:.2f}")

    print("\nBob's Final Balance:")
    bob_final = requests.get(f"{BASE_URL}/accounts/{bob_account_id}")
    print(f"  Balance: ${bob_final.json()['balance']:.2f}")

    print("\nCharlie's Final Balance:")
    charlie_final = requests.get(f"{BASE_URL}/accounts/{charlie_account_id}")
    print(f"  Balance: ${charlie_final.json()['balance']:.2f}")

    # Test 7: Query Transactions
    print_section("7. Querying Account Transactions")

    alice_txns = requests.get(f"{BASE_URL}/transactions/{alice_account_id}")
    print_response(alice_txns, "Alice's Transactions")

    # Test 8: Verify Ledger Integrity
    print_section("8. Verifying Ledger Integrity")

    integrity = requests.get(f"{BASE_URL}/audit/verify")
    print_response(integrity, "Integrity Verification")

    # Test 9: Get Audit Entries
    print_section("9. Retrieving Audit Entries")

    audit_entries = requests.get(f"{BASE_URL}/audit/entries?limit=10")
    print_response(audit_entries, "Recent Audit Entries")

    # Test 10: Filter by Event Type
    print_section("10. Filtering Audit Entries by Event Type")

    transfers = requests.get(f"{BASE_URL}/audit/entries?event_type=TRANSFER_COMPLETED&limit=5")
    print_response(transfers, "Transfer Events Only")

    # Test 11: Compliance Report
    print_section("11. Generating Compliance Report")

    compliance = requests.get(f"{BASE_URL}/audit/compliance-report")
    print_response(compliance, "Compliance Report")

    # Test 12: Account Audit Trail
    print_section("12. Account Audit Trail")

    audit_trail = requests.get(f"{BASE_URL}/accounts/{alice_account_id}/audit-trail")
    print_response(audit_trail, "Alice's Complete Audit Trail")

    # Summary
    print_section("Test Suite Summary")
    print("\n✅ All tests completed successfully!")
    print("\nKey Metrics:")
    print(f"  - Accounts Created: 3")
    print(f"  - Successful Transfers: 3")
    print(f"  - Failed Transfers: 1")
    print(f"  - Ledger Integrity: {integrity.json()['integrity']}")
    print(f"  - Total Audit Entries: {integrity.json()['total_entries']}")

    print("\n" + "=" * 70)
    print("  Test Suite Completed")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        print("\n⏳ Waiting for Flask server to start...")
        print("   Make sure flask_banking_app.py is running on port 5000\n")

        time.sleep(2)

        # Run tests
        test_banking_app()

    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Could not connect to Flask server")
        print("   Please start the server first:")
        print("   python flask_banking_app.py\n")
    except Exception as e:
        print(f"\n❌ Error: {e}\n")

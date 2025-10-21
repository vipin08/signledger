"""
Real-World Example: E-Commerce Order Tracking with SignLedger

This example demonstrates using SignLedger for complete order lifecycle tracking
in an e-commerce platform, providing full transparency and audit trails.

Features:
- Order placement and status tracking
- Inventory management audit trail
- Payment processing logs
- Shipment tracking
- Returns and refunds logging
- Customer activity tracking
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from signledger import Ledger
from signledger.backends import SQLiteBackend
from datetime import datetime, timezone
from decimal import Decimal


class ECommerceAuditLogger:
    """E-commerce order and activity audit logger"""

    def __init__(self, database_path="ecommerce_audit.db"):
        self.ledger = Ledger(
            backend=SQLiteBackend(database_path),
            auto_verify=True,
            verify_interval=1800  # Verify every 30 minutes
        )

    def log_order_created(self, order_id, customer_id, items, total_amount,
                          payment_method, shipping_address):
        """Log new order creation"""
        entry = self.ledger.append({
            "event_type": "ORDER_CREATED",
            "order_id": order_id,
            "customer_id": customer_id,
            "items": items,
            "total_amount": float(total_amount),
            "payment_method": payment_method,
            "shipping_address": shipping_address,
            "order_status": "pending",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_payment_processed(self, order_id, amount, payment_method,
                              transaction_id, status):
        """Log payment processing"""
        entry = self.ledger.append({
            "event_type": "PAYMENT_PROCESSED",
            "order_id": order_id,
            "amount": float(amount),
            "payment_method": payment_method,
            "transaction_id": transaction_id,
            "status": status,  # success, failed, pending
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_inventory_update(self, product_id, product_name, old_quantity,
                             new_quantity, reason, updated_by):
        """Log inventory changes"""
        entry = self.ledger.append({
            "event_type": "INVENTORY_UPDATE",
            "product_id": product_id,
            "product_name": product_name,
            "old_quantity": old_quantity,
            "new_quantity": new_quantity,
            "change": new_quantity - old_quantity,
            "reason": reason,
            "updated_by": updated_by,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_order_status_change(self, order_id, old_status, new_status,
                                 changed_by, notes=""):
        """Log order status changes"""
        entry = self.ledger.append({
            "event_type": "ORDER_STATUS_CHANGE",
            "order_id": order_id,
            "old_status": old_status,
            "new_status": new_status,
            "changed_by": changed_by,
            "notes": notes,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_shipment_created(self, order_id, tracking_number, carrier,
                             estimated_delivery, items_shipped):
        """Log shipment creation"""
        entry = self.ledger.append({
            "event_type": "SHIPMENT_CREATED",
            "order_id": order_id,
            "tracking_number": tracking_number,
            "carrier": carrier,
            "estimated_delivery": estimated_delivery,
            "items_shipped": items_shipped,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_shipment_status(self, tracking_number, status, location, notes=""):
        """Log shipment status updates"""
        entry = self.ledger.append({
            "event_type": "SHIPMENT_STATUS",
            "tracking_number": tracking_number,
            "status": status,
            "location": location,
            "notes": notes,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_return_request(self, order_id, customer_id, items, reason,
                           refund_amount):
        """Log return/refund requests"""
        entry = self.ledger.append({
            "event_type": "RETURN_REQUEST",
            "order_id": order_id,
            "customer_id": customer_id,
            "items": items,
            "reason": reason,
            "refund_amount": float(refund_amount),
            "status": "pending",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_refund_processed(self, order_id, amount, refund_method,
                             transaction_id):
        """Log refund processing"""
        entry = self.ledger.append({
            "event_type": "REFUND_PROCESSED",
            "order_id": order_id,
            "amount": float(amount),
            "refund_method": refund_method,
            "transaction_id": transaction_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def log_customer_activity(self, customer_id, activity_type, details):
        """Log customer activities"""
        entry = self.ledger.append({
            "event_type": "CUSTOMER_ACTIVITY",
            "customer_id": customer_id,
            "activity_type": activity_type,
            "details": details,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        return entry

    def get_order_history(self, order_id):
        """Get complete history for an order"""
        entries = self.ledger.query(
            lambda e: e.data.get("order_id") == order_id
        )
        return sorted(entries, key=lambda e: e.timestamp)

    def get_customer_orders(self, customer_id):
        """Get all orders for a customer"""
        entries = self.ledger.query(
            lambda e: e.data.get("customer_id") == customer_id and
                     e.data.get("event_type") == "ORDER_CREATED"
        )
        return sorted(entries, key=lambda e: e.timestamp, reverse=True)

    def get_inventory_history(self, product_id):
        """Get inventory change history for a product"""
        entries = self.ledger.query(
            lambda e: e.data.get("product_id") == product_id and
                     e.data.get("event_type") == "INVENTORY_UPDATE"
        )
        return sorted(entries, key=lambda e: e.timestamp)

    def generate_sales_report(self, start_date=None, end_date=None):
        """Generate sales report"""
        entries = list(self.ledger.get_entries(
            start_time=start_date,
            end_time=end_date
        ))

        report = {
            "report_generated": datetime.now(timezone.utc).isoformat(),
            "total_orders": 0,
            "total_revenue": 0.0,
            "total_refunds": 0.0,
            "orders_by_status": {},
            "payment_methods": {},
            "shipments": 0,
            "returns": 0
        }

        for entry in entries:
            event_type = entry.data.get("event_type")

            if event_type == "ORDER_CREATED":
                report["total_orders"] += 1
                report["total_revenue"] += entry.data.get("total_amount", 0)
                status = entry.data.get("order_status", "unknown")
                report["orders_by_status"][status] = \
                    report["orders_by_status"].get(status, 0) + 1

            elif event_type == "PAYMENT_PROCESSED":
                method = entry.data.get("payment_method", "unknown")
                report["payment_methods"][method] = \
                    report["payment_methods"].get(method, 0) + 1

            elif event_type == "SHIPMENT_CREATED":
                report["shipments"] += 1

            elif event_type == "RETURN_REQUEST":
                report["returns"] += 1

            elif event_type == "REFUND_PROCESSED":
                report["total_refunds"] += entry.data.get("amount", 0)

        report["net_revenue"] = report["total_revenue"] - report["total_refunds"]
        return report

    def close(self):
        """Close the ledger"""
        self.ledger.close()


def main():
    """Demonstrate e-commerce order tracking"""

    print("\n" + "="*70)
    print("  E-Commerce Order Tracking Demo")
    print("="*70 + "\n")

    logger = ECommerceAuditLogger("ecommerce_demo.db")

    # Scenario 1: Customer places order
    print("1. Customer places order...")
    order_entry = logger.log_order_created(
        order_id="ORD-2024-001",
        customer_id="CUST-12345",
        items=[
            {"product_id": "PROD-101", "name": "Laptop", "quantity": 1, "price": 999.99},
            {"product_id": "PROD-202", "name": "Mouse", "quantity": 2, "price": 29.99}
        ],
        total_amount=Decimal("1059.97"),
        payment_method="credit_card",
        shipping_address={
            "street": "123 Main St",
            "city": "San Francisco",
            "state": "CA",
            "zip": "94105"
        }
    )
    print(f"   ✅ Order created: {order_entry.id}")

    # Scenario 2: Process payment
    print("\n2. Processing payment...")
    payment_entry = logger.log_payment_processed(
        order_id="ORD-2024-001",
        amount=Decimal("1059.97"),
        payment_method="credit_card",
        transaction_id="TXN-987654321",
        status="success"
    )
    print(f"   ✅ Payment processed: {payment_entry.id}")

    # Scenario 3: Update inventory
    print("\n3. Updating inventory...")
    logger.log_inventory_update(
        product_id="PROD-101",
        product_name="Laptop",
        old_quantity=50,
        new_quantity=49,
        reason="order_fulfillment",
        updated_by="system"
    )
    logger.log_inventory_update(
        product_id="PROD-202",
        product_name="Mouse",
        old_quantity=200,
        new_quantity=198,
        reason="order_fulfillment",
        updated_by="system"
    )
    print("   ✅ Inventory updated")

    # Scenario 4: Update order status
    print("\n4. Updating order status to 'processing'...")
    status_entry = logger.log_order_status_change(
        order_id="ORD-2024-001",
        old_status="pending",
        new_status="processing",
        changed_by="system",
        notes="Payment confirmed, preparing for shipment"
    )
    print(f"   ✅ Status updated: {status_entry.id}")

    # Scenario 5: Create shipment
    print("\n5. Creating shipment...")
    shipment_entry = logger.log_shipment_created(
        order_id="ORD-2024-001",
        tracking_number="TRACK-123456789",
        carrier="FedEx",
        estimated_delivery="2024-10-25",
        items_shipped=["PROD-101", "PROD-202"]
    )
    print(f"   ✅ Shipment created: {shipment_entry.id}")

    # Scenario 6: Update shipment status
    print("\n6. Tracking shipment updates...")
    logger.log_shipment_status(
        tracking_number="TRACK-123456789",
        status="picked_up",
        location="San Francisco, CA",
        notes="Package picked up from warehouse"
    )
    logger.log_shipment_status(
        tracking_number="TRACK-123456789",
        status="in_transit",
        location="Oakland, CA",
        notes="In transit to destination"
    )
    logger.log_shipment_status(
        tracking_number="TRACK-123456789",
        status="delivered",
        location="123 Main St, San Francisco, CA",
        notes="Delivered and signed for"
    )
    print("   ✅ Shipment tracking updated")

    # Scenario 7: Another order with different outcome
    print("\n7. Processing second order...")
    logger.log_order_created(
        order_id="ORD-2024-002",
        customer_id="CUST-67890",
        items=[
            {"product_id": "PROD-303", "name": "Keyboard", "quantity": 1, "price": 79.99}
        ],
        total_amount=Decimal("79.99"),
        payment_method="paypal",
        shipping_address={
            "street": "456 Oak Ave",
            "city": "Los Angeles",
            "state": "CA",
            "zip": "90001"
        }
    )
    print("   ✅ Second order created")

    # Scenario 8: Customer requests return
    print("\n8. Processing return request...")
    return_entry = logger.log_return_request(
        order_id="ORD-2024-002",
        customer_id="CUST-67890",
        items=["PROD-303"],
        reason="changed_mind",
        refund_amount=Decimal("79.99")
    )
    print(f"   ✅ Return request logged: {return_entry.id}")

    # Scenario 9: Process refund
    print("\n9. Processing refund...")
    refund_entry = logger.log_refund_processed(
        order_id="ORD-2024-002",
        amount=Decimal("79.99"),
        refund_method="paypal",
        transaction_id="REFUND-555666777"
    )
    print(f"   ✅ Refund processed: {refund_entry.id}")

    # Scenario 10: Log customer activities
    print("\n10. Logging customer activities...")
    logger.log_customer_activity(
        customer_id="CUST-12345",
        activity_type="product_view",
        details={"product_id": "PROD-404", "product_name": "Monitor"}
    )
    logger.log_customer_activity(
        customer_id="CUST-12345",
        activity_type="wishlist_add",
        details={"product_id": "PROD-404"}
    )
    print("   ✅ Customer activities logged")

    # Get order history
    print("\n" + "="*70)
    print("  Order History (ORD-2024-001)")
    print("="*70 + "\n")

    history = logger.get_order_history("ORD-2024-001")
    for i, entry in enumerate(history, 1):
        print(f"{i}. {entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"   Event: {entry.data.get('event_type')}")
        if entry.data.get('event_type') == 'ORDER_STATUS_CHANGE':
            print(f"   Status: {entry.data.get('old_status')} → {entry.data.get('new_status')}")
        elif entry.data.get('event_type') == 'SHIPMENT_STATUS':
            print(f"   Status: {entry.data.get('status')} at {entry.data.get('location')}")
        print(f"   Hash: {entry.hash[:40]}...")
        print()

    # Generate sales report
    print("="*70)
    print("  Sales Report")
    print("="*70 + "\n")

    report = logger.generate_sales_report()
    print(f"Total Orders: {report['total_orders']}")
    print(f"Total Revenue: ${report['total_revenue']:.2f}")
    print(f"Total Refunds: ${report['total_refunds']:.2f}")
    print(f"Net Revenue: ${report['net_revenue']:.2f}")
    print(f"Shipments: {report['shipments']}")
    print(f"Returns: {report['returns']}")
    print("\nPayment Methods:")
    for method, count in report['payment_methods'].items():
        print(f"  - {method}: {count}")

    # Verify integrity
    print("\n" + "="*70)
    print("  Integrity Verification")
    print("="*70 + "\n")

    is_valid = logger.ledger.verify_integrity()
    stats = logger.ledger.get_stats()

    print(f"✅ Integrity Status: {'VALID' if is_valid else 'INVALID'}")
    print(f"✅ Total Audit Entries: {stats.total_entries}")
    print(f"✅ Hash Algorithm: {stats.hash_algorithm}")

    print("\n" + "="*70)
    print("  ✅ E-Commerce Demo Complete")
    print("="*70)
    print("\nComplete audit trail created for:")
    print("  • Order placement and fulfillment")
    print("  • Payment processing")
    print("  • Inventory management")
    print("  • Shipment tracking")
    print("  • Returns and refunds")
    print("  • Customer activities")
    print("\n" + "="*70 + "\n")

    logger.close()


if __name__ == "__main__":
    main()

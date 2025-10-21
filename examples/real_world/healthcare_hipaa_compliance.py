"""
Real-World Example: Healthcare HIPAA Compliance Audit Logging

This example demonstrates using SignLedger for HIPAA-compliant audit trails
in a healthcare application, tracking all access to Protected Health Information (PHI).

Features:
- Patient record access logging
- User authentication tracking
- Data modification audit trail
- Compliance reporting
- Tamper-proof logs with cryptographic signatures
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from signledger import Ledger
from signledger.backends import SQLiteBackend
from signledger.crypto.signatures import RSASigner
from datetime import datetime, timezone
import json


class HIPAAComplianceLogger:
    """HIPAA-compliant audit logger using SignLedger"""

    def __init__(self, database_path="hipaa_audit.db"):
        # Initialize ledger with SQLite for persistence
        self.ledger = Ledger(
            backend=SQLiteBackend(database_path),
            enable_signatures=True,
            auto_verify=True,
            verify_interval=3600  # Verify every hour
        )

        # Initialize RSA signer for critical events
        self.signer = RSASigner(key_size=2048)

    def _sign_wrapper(self, data_str):
        """Wrapper to convert string hash to bytes for RSA signing"""
        if isinstance(data_str, str):
            data_str = data_str.encode('utf-8')
        return self.signer.sign(data_str)

    def log_phi_access(self, patient_id, accessed_by, access_type,
                       data_accessed, purpose, facility):
        """Log access to Protected Health Information (PHI)"""
        entry = self.ledger.append(
            data={
                "event_type": "PHI_ACCESS",
                "patient_id": patient_id,
                "accessed_by": accessed_by,
                "access_type": access_type,  # READ, WRITE, DELETE
                "data_accessed": data_accessed,
                "purpose": purpose,  # treatment, payment, operations
                "facility": facility,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            metadata={
                "compliance": "HIPAA",
                "regulation": "45 CFR 164.308(a)(1)(ii)(D)",
                "critical": True
            },
            sign=True,
            signer=self._sign_wrapper
        )
        return entry

    def log_user_authentication(self, user_id, user_role, action,
                                 ip_address, success):
        """Log user authentication events"""
        entry = self.ledger.append(
            data={
                "event_type": "AUTHENTICATION",
                "user_id": user_id,
                "user_role": user_role,
                "action": action,  # LOGIN, LOGOUT, FAILED_LOGIN
                "ip_address": ip_address,
                "success": success,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            metadata={
                "compliance": "HIPAA",
                "security_rule": "164.312(a)(2)(i)"
            }
        )
        return entry

    def log_data_modification(self, patient_id, modified_by, field_name,
                              old_value, new_value, reason):
        """Log modifications to patient data"""
        entry = self.ledger.append(
            data={
                "event_type": "DATA_MODIFICATION",
                "patient_id": patient_id,
                "modified_by": modified_by,
                "field_name": field_name,
                "old_value": old_value,
                "new_value": new_value,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            metadata={
                "compliance": "HIPAA",
                "requires_review": True
            },
            sign=True,
            signer=self._sign_wrapper
        )
        return entry

    def log_data_export(self, patient_id, exported_by, export_format,
                        recipient, authorization_number):
        """Log PHI data exports"""
        entry = self.ledger.append(
            data={
                "event_type": "PHI_EXPORT",
                "patient_id": patient_id,
                "exported_by": exported_by,
                "export_format": export_format,
                "recipient": recipient,
                "authorization_number": authorization_number,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            metadata={
                "compliance": "HIPAA",
                "privacy_rule": "164.508",
                "critical": True
            },
            sign=True,
            signer=self._sign_wrapper
        )
        return entry

    def generate_compliance_report(self, start_date=None, end_date=None):
        """Generate HIPAA compliance audit report"""
        entries = list(self.ledger.get_entries(
            start_time=start_date,
            end_time=end_date
        ))

        report = {
            "report_generated": datetime.now(timezone.utc).isoformat(),
            "period_start": start_date.isoformat() if start_date else "inception",
            "period_end": end_date.isoformat() if end_date else "now",
            "total_events": len(entries),
            "events_by_type": {},
            "phi_access_count": 0,
            "authentication_failures": 0,
            "data_modifications": 0,
            "data_exports": 0,
            "integrity_verified": False
        }

        # Analyze entries
        for entry in entries:
            event_type = entry.data.get("event_type", "UNKNOWN")
            report["events_by_type"][event_type] = \
                report["events_by_type"].get(event_type, 0) + 1

            if event_type == "PHI_ACCESS":
                report["phi_access_count"] += 1
            elif event_type == "AUTHENTICATION" and not entry.data.get("success"):
                report["authentication_failures"] += 1
            elif event_type == "DATA_MODIFICATION":
                report["data_modifications"] += 1
            elif event_type == "PHI_EXPORT":
                report["data_exports"] += 1

        # Verify integrity
        try:
            report["integrity_verified"] = self.ledger.verify_integrity()
        except Exception as e:
            report["integrity_error"] = str(e)

        return report

    def get_patient_access_history(self, patient_id):
        """Get complete access history for a patient"""
        entries = self.ledger.query(
            lambda e: e.data.get("patient_id") == patient_id
        )

        return [{
            "timestamp": e.timestamp.isoformat(),
            "event_type": e.data.get("event_type"),
            "accessed_by": e.data.get("accessed_by") or e.data.get("modified_by") or e.data.get("exported_by"),
            "action": e.data.get("access_type") or e.data.get("action"),
            "details": e.data,
            "signed": e.signature is not None,
            "hash": e.hash
        } for e in entries]

    def close(self):
        """Close the ledger"""
        self.ledger.close()


def main():
    """Demonstrate HIPAA compliance logging"""

    print("\n" + "="*70)
    print("  Healthcare HIPAA Compliance Audit Logging Demo")
    print("="*70 + "\n")

    # Initialize logger
    logger = HIPAAComplianceLogger("hipaa_demo.db")

    # Scenario 1: Doctor logs in
    print("1. Doctor authentication...")
    entry = logger.log_user_authentication(
        user_id="DR_SMITH_001",
        user_role="physician",
        action="LOGIN",
        ip_address="192.168.1.100",
        success=True
    )
    print(f"   ✅ Logged: {entry.id}")

    # Scenario 2: Doctor accesses patient record
    print("\n2. Accessing patient medical record...")
    entry = logger.log_phi_access(
        patient_id="PATIENT_12345",
        accessed_by="DR_SMITH_001",
        access_type="READ",
        data_accessed=["medical_history", "prescriptions", "lab_results"],
        purpose="treatment",
        facility="General Hospital"
    )
    print(f"   ✅ PHI Access logged (signed): {entry.id}")
    print(f"   ✅ Signature: {entry.signature[:40]}...")

    # Scenario 3: Nurse updates patient vital signs
    print("\n3. Updating patient data...")
    entry = logger.log_data_modification(
        patient_id="PATIENT_12345",
        modified_by="NURSE_JONES_042",
        field_name="vital_signs.blood_pressure",
        old_value="120/80",
        new_value="125/82",
        reason="routine_checkup"
    )
    print(f"   ✅ Data modification logged (signed): {entry.id}")

    # Scenario 4: Failed login attempt
    print("\n4. Failed authentication attempt...")
    entry = logger.log_user_authentication(
        user_id="UNKNOWN_USER",
        user_role="unknown",
        action="FAILED_LOGIN",
        ip_address="203.0.113.42",
        success=False
    )
    print(f"   ✅ Failed login logged: {entry.id}")

    # Scenario 5: Export patient data for transfer
    print("\n5. Exporting patient data for transfer...")
    entry = logger.log_data_export(
        patient_id="PATIENT_12345",
        exported_by="DR_SMITH_001",
        export_format="HL7_FHIR",
        recipient="Specialist Clinic",
        authorization_number="AUTH_2024_7890"
    )
    print(f"   ✅ PHI Export logged (signed): {entry.id}")

    # Scenario 6: Additional PHI accesses
    print("\n6. Simulating additional activities...")
    logger.log_phi_access(
        patient_id="PATIENT_67890",
        accessed_by="NURSE_JONES_042",
        access_type="WRITE",
        data_accessed=["medications"],
        purpose="treatment",
        facility="General Hospital"
    )
    logger.log_phi_access(
        patient_id="PATIENT_12345",
        accessed_by="LAB_TECH_033",
        access_type="READ",
        data_accessed=["lab_results"],
        purpose="operations",
        facility="General Hospital Lab"
    )
    print("   ✅ Additional events logged")

    # Generate compliance report
    print("\n" + "="*70)
    print("  HIPAA Compliance Report")
    print("="*70)

    report = logger.generate_compliance_report()
    print(f"\nReport Generated: {report['report_generated']}")
    print(f"Total Events: {report['total_events']}")
    print(f"PHI Access Events: {report['phi_access_count']}")
    print(f"Data Modifications: {report['data_modifications']}")
    print(f"Data Exports: {report['data_exports']}")
    print(f"Authentication Failures: {report['authentication_failures']}")
    print(f"\nLedger Integrity: {'✅ VERIFIED' if report['integrity_verified'] else '❌ FAILED'}")

    print("\nEvents by Type:")
    for event_type, count in report['events_by_type'].items():
        print(f"  - {event_type}: {count}")

    # Get patient access history
    print("\n" + "="*70)
    print("  Patient Access History (PATIENT_12345)")
    print("="*70 + "\n")

    history = logger.get_patient_access_history("PATIENT_12345")
    for i, event in enumerate(history, 1):
        print(f"{i}. {event['timestamp']}")
        print(f"   Event: {event['event_type']}")
        print(f"   By: {event['accessed_by']}")
        print(f"   Signed: {'✅' if event['signed'] else '❌'}")
        print(f"   Hash: {event['hash'][:40]}...")
        print()

    # Verify integrity
    print("="*70)
    print("  Final Integrity Check")
    print("="*70)

    is_valid = logger.ledger.verify_integrity()
    stats = logger.ledger.get_stats()

    print(f"\n✅ Integrity Status: {'VALID' if is_valid else 'INVALID'}")
    print(f"✅ Total Audit Entries: {stats.total_entries}")
    print(f"✅ Hash Algorithm: {stats.hash_algorithm}")
    print(f"✅ First Entry: {stats.first_entry_time}")
    print(f"✅ Last Entry: {stats.last_entry_time}")

    print("\n" + "="*70)
    print("  ✅ HIPAA Compliance Demo Complete")
    print("="*70)
    print("\nAll patient data access has been logged with:")
    print("  • Tamper-proof hash chain")
    print("  • Cryptographic signatures on critical events")
    print("  • Complete audit trail for compliance")
    print("  • Integrity verification")
    print("\n" + "="*70 + "\n")

    # Cleanup
    logger.close()


if __name__ == "__main__":
    main()

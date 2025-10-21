#!/usr/bin/env python
"""Command-line interface for SignLedger."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from signledger import Ledger, __version__
from signledger.backends.base import InMemoryBackend


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="SignLedger - Blockchain-Inspired Audit Log Library",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"SignLedger {__version__}",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Verify command
    verify_parser = subparsers.add_parser("verify", help="Verify ledger integrity")
    verify_parser.add_argument("-b", "--backend", default="memory", help="Backend type")
    verify_parser.add_argument("-c", "--connection", help="Backend connection string")
    
    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show ledger statistics")
    stats_parser.add_argument("-b", "--backend", default="memory", help="Backend type")
    stats_parser.add_argument("-c", "--connection", help="Backend connection string")
    
    # Export command
    export_parser = subparsers.add_parser("export", help="Export ledger entries")
    export_parser.add_argument("-b", "--backend", default="memory", help="Backend type")
    export_parser.add_argument("-c", "--connection", help="Backend connection string")
    export_parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    export_parser.add_argument("-f", "--format", default="json", choices=["json", "csv"], help="Export format")
    
    # Demo command
    demo_parser = subparsers.add_parser("demo", help="Run interactive demo")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.command == "verify":
            # Create ledger with specified backend
            backend = _create_backend(args.backend, args.connection)
            ledger = Ledger(backend=backend, auto_verify=False)
            
            print("Verifying ledger integrity...")
            try:
                ledger.verify_integrity()
                print("✓ Ledger integrity verified successfully")
                return 0
            except Exception as e:
                print(f"✗ Integrity verification failed: {e}")
                return 1
                
        elif args.command == "stats":
            backend = _create_backend(args.backend, args.connection)
            ledger = Ledger(backend=backend, auto_verify=False)
            
            stats = ledger.get_stats()
            print("Ledger Statistics:")
            print(f"  Total entries: {stats.total_entries}")
            print(f"  Hash algorithm: {stats.hash_algorithm}")
            print(f"  Storage size: {stats.total_size_bytes:,} bytes")
            if stats.first_entry_time:
                print(f"  First entry: {stats.first_entry_time.isoformat()}")
            if stats.last_entry_time:
                print(f"  Last entry: {stats.last_entry_time.isoformat()}")
                
        elif args.command == "export":
            backend = _create_backend(args.backend, args.connection)
            ledger = Ledger(backend=backend, auto_verify=False)
            
            entries = list(ledger.get_entries())
            
            if args.format == "json":
                output = json.dumps([e.to_dict() for e in entries], indent=2)
            else:  # CSV
                import csv
                import io
                output_buffer = io.StringIO()
                if entries:
                    fieldnames = ["id", "timestamp", "hash", "previous_hash", "data"]
                    writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
                    writer.writeheader()
                    for entry in entries:
                        row = entry.to_dict()
                        row["data"] = json.dumps(row["data"])
                        writer.writerow({k: row.get(k) for k in fieldnames})
                output = output_buffer.getvalue()
            
            if args.output:
                Path(args.output).write_text(output)
                print(f"Exported {len(entries)} entries to {args.output}")
            else:
                print(output)
                
        elif args.command == "demo":
            print("SignLedger Interactive Demo")
            print("=" * 50)
            
            # Create demo ledger
            ledger = Ledger(auto_verify=False)
            
            # Add some entries
            print("\nAdding demo entries...")
            entries = []
            
            entry1 = ledger.append({
                "event": "user_login",
                "user": "alice",
                "ip": "192.168.1.100"
            })
            entries.append(entry1)
            print(f"✓ Added entry: {entry1.id[:8]}... (user_login)")
            
            entry2 = ledger.append({
                "event": "file_upload",
                "user": "alice",
                "filename": "report.pdf",
                "size": 1024000
            })
            entries.append(entry2)
            print(f"✓ Added entry: {entry2.id[:8]}... (file_upload)")
            
            entry3 = ledger.append({
                "event": "user_logout",
                "user": "alice",
                "duration": 3600
            })
            entries.append(entry3)
            print(f"✓ Added entry: {entry3.id[:8]}... (user_logout)")
            
            # Show chain
            print("\nHash Chain:")
            for i, entry in enumerate(entries):
                print(f"  Entry {i+1}: {entry.hash[:16]}...")
                if entry.previous_hash:
                    print(f"    └─> Previous: {entry.previous_hash[:16]}...")
            
            # Verify
            print("\nVerifying integrity...")
            ledger.verify_integrity()
            print("✓ Integrity verified!")
            
            # Stats
            stats = ledger.get_stats()
            print(f"\nLedger contains {stats.total_entries} entries")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    
    return 0


def _create_backend(backend_type: str, connection: str = None):
    """Create backend instance based on type."""
    if backend_type == "memory":
        return InMemoryBackend()
    else:
        raise ValueError(f"Unsupported backend type: {backend_type}")


if __name__ == "__main__":
    sys.exit(main())
#!/usr/bin/env python3
"""
Extract Missing Data PMIDs from Enhanced Database

This script extracts PMIDs that failed specifically due to missing DOI/VIP/PII
data from the enhanced database with TXERROR capture.

Usage:
    python extract_missing_data_pmids_enhanced.py --output missing_data_pmids_enhanced.txt
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict

# Add FindIt_coverage_v2 to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'FindIt_coverage_v2' / 'bin'))

from database import DatabaseManager, setup_logger

def extract_missing_data_pmids(db_manager: DatabaseManager) -> Dict[str, List[str]]:
    """Extract PMIDs with missing data from enhanced database."""
    
    results = {
        'missing_doi': [],
        'missing_vip': [],
        'missing_pii': [],
        'missing_any': [],
        'txerror_containing_missing': []
    }
    
    with db_manager.get_connection() as conn:
        with db_manager.get_cursor(conn) as cursor:
            
            # Direct flags
            cursor.execute("SELECT pmid FROM pmid_results WHERE missing_doi = TRUE")
            results['missing_doi'] = [row['pmid'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT pmid FROM pmid_results WHERE missing_vip = TRUE")
            results['missing_vip'] = [row['pmid'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT pmid FROM pmid_results WHERE missing_pii = TRUE") 
            results['missing_pii'] = [row['pmid'] for row in cursor.fetchall()]
            
            # Any missing data flag
            cursor.execute("""
                SELECT pmid FROM pmid_results 
                WHERE missing_doi = TRUE OR missing_vip = TRUE OR missing_pii = TRUE
            """)
            results['missing_any'] = [row['pmid'] for row in cursor.fetchall()]
            
            # TXERROR messages containing missing keywords
            cursor.execute("""
                SELECT DISTINCT pmid 
                FROM pmid_results 
                WHERE txerror_messages IS NOT NULL 
                  AND (
                    array_to_string(txerror_messages, ' ') ILIKE '%missing%'
                    OR array_to_string(txerror_messages, ' ') ILIKE '%doi required%'
                    OR array_to_string(txerror_messages, ' ') ILIKE '%vip%'
                    OR array_to_string(txerror_messages, ' ') ILIKE '%pii%'
                  )
            """)
            results['txerror_containing_missing'] = [row['pmid'] for row in cursor.fetchall()]
    
    return results

def show_missing_data_summary(db_manager: DatabaseManager):
    """Show comprehensive missing data summary."""
    
    with db_manager.get_connection() as conn:
        with db_manager.get_cursor(conn) as cursor:
            
            print("📊 Enhanced Missing Data Analysis")
            print("=" * 50)
            
            # Overall stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_pmids,
                    COUNT(CASE WHEN txerror_messages IS NOT NULL THEN 1 END) as with_txerror,
                    COUNT(CASE WHEN missing_doi = TRUE THEN 1 END) as missing_doi,
                    COUNT(CASE WHEN missing_vip = TRUE THEN 1 END) as missing_vip,
                    COUNT(CASE WHEN missing_pii = TRUE THEN 1 END) as missing_pii,
                    COUNT(CASE WHEN failure_type = 'missing_data' THEN 1 END) as missing_data_failures
                FROM pmid_results
            """)
            
            stats = cursor.fetchone()
            print(f"Total PMIDs: {stats['total_pmids']:,}")
            print(f"With TXERROR data: {stats['with_txerror']:,}")
            print(f"Missing DOI flagged: {stats['missing_doi']:,}")
            print(f"Missing VIP flagged: {stats['missing_vip']:,}")
            print(f"Missing PII flagged: {stats['missing_pii']:,}")
            print(f"Missing data failures: {stats['missing_data_failures']:,}")
            print()
            
            # Specific TXERROR patterns
            cursor.execute("""
                SELECT 
                    'DOI required' as pattern,
                    COUNT(*) as count
                FROM pmid_results 
                WHERE array_to_string(txerror_messages, ' ') ILIKE '%doi required%'
                UNION ALL
                SELECT 
                    'Missing DOI' as pattern,
                    COUNT(*) as count
                FROM pmid_results 
                WHERE array_to_string(txerror_messages, ' ') ILIKE '%missing: doi%'
                UNION ALL
                SELECT 
                    'PII required' as pattern,
                    COUNT(*) as count
                FROM pmid_results 
                WHERE array_to_string(txerror_messages, ' ') ILIKE '%pii%required%'
                UNION ALL
                SELECT 
                    'VIP missing' as pattern,
                    COUNT(*) as count
                FROM pmid_results 
                WHERE array_to_string(txerror_messages, ' ') ILIKE '%vip%'
                ORDER BY count DESC
            """)
            
            patterns = cursor.fetchall()
            print("TXERROR Pattern Analysis:")
            for row in patterns:
                if row['count'] > 0:
                    print(f"  {row['pattern']}: {row['count']:,}")
            print()
            
            # Sample detailed messages
            cursor.execute("""
                SELECT pmid, txerror_messages[1] as message, failure_type
                FROM pmid_results 
                WHERE array_to_string(txerror_messages, ' ') ILIKE '%missing%'
                LIMIT 10
            """)
            
            samples = cursor.fetchall()
            if samples:
                print("Sample MISSING-related TXERROR messages:")
                for row in samples:
                    msg = row['message'][:100] + "..." if len(row['message']) > 100 else row['message']
                    print(f"  PMID {row['pmid']}: {msg}")
                print()

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Extract missing data PMIDs from enhanced database')
    parser.add_argument('--output', '-o', default='missing_data_pmids_enhanced.txt',
                       help='Output file for PMIDs (default: missing_data_pmids_enhanced.txt)')
    parser.add_argument('--show-summary-only', action='store_true',
                       help='Only show summary without extracting PMIDs')
    parser.add_argument('--include-txerror-matches', action='store_true',
                       help='Include PMIDs with TXERROR messages containing missing keywords')
    
    args = parser.parse_args()
    
    try:
        # Initialize database
        db_manager = DatabaseManager()
        
        # Show summary
        show_missing_data_summary(db_manager)
        
        if not args.show_summary_only:
            # Extract missing data PMIDs
            missing_data = extract_missing_data_pmids(db_manager)
            
            # Choose which PMIDs to output
            if args.include_txerror_matches:
                # Use broader TXERROR-based matching
                output_pmids = missing_data['txerror_containing_missing']
                source_desc = "TXERROR messages containing missing data keywords"
            else:
                # Use specific flags
                output_pmids = missing_data['missing_any']
                source_desc = "specific missing data flags"
            
            if output_pmids:
                # Write to file
                with open(args.output, 'w') as f:
                    for pmid in sorted(set(output_pmids)):
                        f.write(f"{pmid}\n")
                
                print(f"✅ Extracted {len(output_pmids)} PMIDs with missing data")
                print(f"📁 Source: {source_desc}")
                print(f"💾 Saved to: {args.output}")
                print()
                
                # Show breakdown
                print("Breakdown by type:")
                print(f"  Missing DOI only: {len(missing_data['missing_doi'])}")
                print(f"  Missing VIP only: {len(missing_data['missing_vip'])}")
                print(f"  Missing PII only: {len(missing_data['missing_pii'])}")
                print(f"  Any missing flag: {len(missing_data['missing_any'])}")
                print(f"  TXERROR matches: {len(missing_data['txerror_containing_missing'])}")
            else:
                print("❌ No PMIDs found with missing data")
                print("💡 Try running more enhanced analysis to capture TXERROR data")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
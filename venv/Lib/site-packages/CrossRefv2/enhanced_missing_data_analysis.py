#!/usr/bin/env python3
"""
Enhanced Missing Data PMIDs CrossRefv2 Analysis

Analysis specifically for the PMIDs identified through enhanced TXERROR capture
that failed due to missing DOI/VIP/PII data.
"""

import sys
import os
import csv
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metapub import PubMedFetcher
from CrossRefv2 import EnhancedCrossRefFetcher
import requests
from Levenshtein import ratio

logging.basicConfig(level=logging.WARNING)

@dataclass
class AnalysisResult:
    pmid: str
    category: str  # 'has_doi', 'missing_doi', 'error'
    status: str    # 'discovered', 'not_found', 'already_has_doi', 'error'
    original_doi: Optional[str] = None
    discovered_doi: Optional[str] = None
    confidence: float = 0.0
    title_similarity: float = 0.0
    match_quality: str = "unknown"
    original_title: str = ""
    original_journal: str = ""
    original_year: str = ""
    crossref_title: str = ""
    crossref_journal: str = ""
    validation_notes: str = ""
    txerror_type: str = ""  # Original failure type from enhanced capture

def assess_match_quality(result: AnalysisResult) -> Tuple[str, str]:
    """Assess match quality and provide validation notes"""
    
    if result.confidence < 0.7:
        return "poor", "Low confidence score"
    
    notes = []
    quality = "excellent"
    
    # Check title similarity
    if result.title_similarity < 0.7:
        quality = "poor"
        notes.append(f"Low title similarity ({result.title_similarity:.3f})")
    elif result.title_similarity < 0.8:
        if quality == "excellent":
            quality = "good"
        notes.append(f"Moderate title similarity ({result.title_similarity:.3f})")
    
    # Check journal name similarity (basic check)
    if result.original_journal and result.crossref_journal:
        orig_journal = result.original_journal.lower().replace(".", "").replace(" ", "")
        cross_journal = result.crossref_journal.lower().replace(".", "").replace(" ", "")
        
        if orig_journal not in cross_journal and cross_journal not in orig_journal:
            if ratio(orig_journal, cross_journal) < 0.6:
                if quality != "poor":
                    quality = "questionable"
                notes.append("Journal mismatch")
    
    # Overall assessment
    if result.confidence >= 0.9 and result.title_similarity >= 0.9 and quality != "poor":
        quality = "excellent"
        if not notes:
            notes.append("Good match")
    elif quality == "poor":
        notes.append("Very low title similarity suggests different article")
    
    validation_notes = "; ".join(notes) if notes else "Good match"
    
    return quality, validation_notes

def analyze_pmid(pmid: str, pubmed: PubMedFetcher, crossref: EnhancedCrossRefFetcher, 
                txerror_type: str = "") -> AnalysisResult:
    """Analyze a single PMID"""
    
    try:
        # Get PubMed article
        article = pubmed.article_by_pmid(pmid)
        if not article:
            return AnalysisResult(
                pmid=pmid,
                category="error", 
                status="error",
                txerror_type=txerror_type,
                validation_notes="Could not fetch PubMed article"
            )
        
        # Check if already has DOI
        original_doi = getattr(article, 'doi', None)
        if original_doi:
            return AnalysisResult(
                pmid=pmid,
                category="has_doi",
                status="already_has_doi",
                original_doi=original_doi,
                discovered_doi=original_doi,
                confidence=1.0,
                title_similarity=1.0,
                match_quality="perfect",
                original_title=getattr(article, 'title', ''),
                original_journal=getattr(article, 'journal', ''),
                original_year=str(getattr(article, 'year', '')),
                txerror_type=txerror_type,
                validation_notes="Already has DOI in PubMed"
            )
        
        # Try to discover DOI via CrossRefv2
        result_doi = crossref.find_best_doi(article)
        result = None
        if result_doi:
            # Get the best candidate for details
            candidates = crossref.search_multiple_strategies(article)
            if candidates:
                best_candidate = max(candidates, key=lambda c: c.confidence)
                result = best_candidate
        
        if not result_doi:
            return AnalysisResult(
                pmid=pmid,
                category="missing_doi",
                status="not_found",
                original_title=getattr(article, 'title', ''),
                original_journal=getattr(article, 'journal', ''),
                original_year=str(getattr(article, 'year', '')),
                txerror_type=txerror_type,
                validation_notes="No DOI found via CrossRefv2"
            )
        
        # Calculate title similarity
        orig_title = getattr(article, 'title', '')
        crossref_title = getattr(result, 'title', '') if result else ''
        title_sim = ratio(orig_title.lower(), crossref_title.lower()) if orig_title and crossref_title else 0.0
        
        # Create analysis result
        analysis_result = AnalysisResult(
            pmid=pmid,
            category="missing_doi",
            status="discovered",
            discovered_doi=result_doi,
            confidence=result.confidence if result else 0.8,
            title_similarity=title_sim,
            original_title=orig_title,
            original_journal=getattr(article, 'journal', ''),
            original_year=str(getattr(article, 'year', '')),
            crossref_title=crossref_title,
            crossref_journal=getattr(result, 'journal_name', '') if result else '',
            txerror_type=txerror_type
        )
        
        # Assess quality
        quality, notes = assess_match_quality(analysis_result)
        analysis_result.match_quality = quality
        analysis_result.validation_notes = notes
        
        return analysis_result
        
    except Exception as e:
        return AnalysisResult(
            pmid=pmid,
            category="error",
            status="error",
            txerror_type=txerror_type,
            validation_notes=f"Analysis error: {str(e)}"
        )

def load_enhanced_missing_data_pmids() -> Dict[str, str]:
    """Load PMIDs with their TXERROR types from the enhanced database."""
    
    # Add FindIt_coverage_v2 to path
    from pathlib import Path
    parent_dir = Path(__file__).parent.parent
    sys.path.insert(0, str(parent_dir / 'FindIt_coverage_v2' / 'bin'))
    
    from database import DatabaseManager
    
    db_manager = DatabaseManager()
    pmid_txerror_map = {}
    
    with db_manager.get_connection() as conn:
        with db_manager.get_cursor(conn) as cursor:
            
            cursor.execute("""
                SELECT pmid, failure_type, txerror_messages[1] as first_txerror
                FROM pmid_results 
                WHERE failure_type = 'missing_data'
                   OR array_to_string(txerror_messages, ' ') ILIKE '%missing%'
                   OR array_to_string(txerror_messages, ' ') ILIKE '%doi required%'
                   OR array_to_string(txerror_messages, ' ') ILIKE '%vip%'
                   OR array_to_string(txerror_messages, ' ') ILIKE '%pii%'
                ORDER BY pmid
            """)
            
            for row in cursor.fetchall():
                pmid = row['pmid']
                failure_type = row['failure_type'] or 'unknown'
                
                # Determine specific failure type
                first_txerror = row['first_txerror'] or ''
                if 'doi required' in first_txerror.lower():
                    txerror_type = 'missing_doi'
                elif 'pii' in first_txerror.lower():
                    txerror_type = 'missing_pii'
                elif 'vip' in first_txerror.lower():
                    txerror_type = 'missing_vip'
                else:
                    txerror_type = failure_type
                
                pmid_txerror_map[pmid] = txerror_type
    
    return pmid_txerror_map

def main():
    """Main analysis function"""
    
    # Load enhanced missing data PMIDs
    pmid_txerror_map = load_enhanced_missing_data_pmids()
    pmids = list(pmid_txerror_map.keys())
    
    print("Enhanced Missing Data PMIDs CrossRefv2 Analysis")
    print("=" * 60)
    print(f"Analyzing {len(pmids)} PMIDs identified through enhanced TXERROR capture")
    print("These PMIDs failed specifically due to missing DOI/VIP/PII data")
    print()
    
    # Initialize clients
    pubmed = PubMedFetcher()
    crossref = EnhancedCrossRefFetcher()
    
    results = []
    
    # Analyze each PMID
    for i, pmid in enumerate(pmids, 1):
        txerror_type = pmid_txerror_map.get(pmid, 'unknown')
        print(f"[{i}/{len(pmids)}] Analyzing PMID {pmid} (TXERROR: {txerror_type})")
        
        result = analyze_pmid(pmid, pubmed, crossref, txerror_type)
        results.append(result)
        
        # Show status
        if result.status == "already_has_doi":
            print(f"    ✓ Already has DOI: {result.original_doi}")
        elif result.status == "discovered":
            print(f"    DOI: {result.discovered_doi}")
            print(f"    Confidence: {result.confidence:.3f}")
            print(f"    Title similarity: {result.title_similarity:.3f}")
            print(f"    Match quality: {result.match_quality}")
        elif result.status == "not_found":
            print(f"    ✗ No DOI found")
        else:
            print(f"    ⚠ Error: {result.validation_notes}")
        print()
    
    # Generate summary
    print("=" * 60)
    print("ENHANCED ANALYSIS SUMMARY")
    print("=" * 60)
    
    total = len(results)
    has_doi = len([r for r in results if r.category == "has_doi"])
    missing_doi = len([r for r in results if r.category == "missing_doi"])
    errors = len([r for r in results if r.category == "error"])
    
    print(f"Total PMIDs analyzed: {total}")
    print(f"Already have DOI: {has_doi} ({has_doi/total*100:.1f}%)")
    print(f"Missing DOI: {missing_doi} ({missing_doi/total*100:.1f}%)")
    print(f"Processing errors: {errors} ({errors/total*100:.1f}%)")
    print()
    
    # Missing DOI analysis
    missing_doi_results = [r for r in results if r.category == "missing_doi"]
    if missing_doi_results:
        discovered = len([r for r in missing_doi_results if r.status == "discovered"])
        not_found = len([r for r in missing_doi_results if r.status == "not_found"])
        
        print(f"CrossRefv2 Performance on Enhanced Missing-DOI Cases:")
        print(f"  Discovery Rate: {discovered/len(missing_doi_results)*100:.1f}% ({discovered}/{len(missing_doi_results)})")
        
        if discovered > 0:
            discovered_results = [r for r in missing_doi_results if r.status == "discovered"]
            excellent = len([r for r in discovered_results if r.match_quality == "excellent"])
            good = len([r for r in discovered_results if r.match_quality == "good"])
            questionable = len([r for r in discovered_results if r.match_quality == "questionable"])
            poor = len([r for r in discovered_results if r.match_quality == "poor"])
            
            print(f"  Match Quality Distribution:")
            if excellent > 0:
                print(f"    Excellent: {excellent} ({excellent/discovered*100:.1f}%)")
            if good > 0:
                print(f"    Good: {good} ({good/discovered*100:.1f}%)")
            if questionable > 0:
                print(f"    Questionable: {questionable} ({questionable/discovered*100:.1f}%)")
            if poor > 0:
                print(f"    Poor: {poor} ({poor/discovered*100:.1f}%)")
            
            high_quality = excellent + good
            print(f"  High-Quality Matches: {high_quality} ({high_quality/discovered*100:.1f}%)")
            print(f"  Overall Effectiveness: {high_quality/len(missing_doi_results)*100:.1f}%")
    
    # TXERROR type breakdown
    print()
    print("Breakdown by Original TXERROR Type:")
    txerror_types = {}
    for result in results:
        txtype = result.txerror_type
        if txtype not in txerror_types:
            txerror_types[txtype] = {'total': 0, 'discovered': 0, 'excellent': 0}
        txerror_types[txtype]['total'] += 1
        if result.status == 'discovered':
            txerror_types[txtype]['discovered'] += 1
            if result.match_quality == 'excellent':
                txerror_types[txtype]['excellent'] += 1
    
    for txtype, stats in txerror_types.items():
        discovery_rate = stats['discovered'] / stats['total'] * 100 if stats['total'] > 0 else 0
        excellence_rate = stats['excellent'] / stats['discovered'] * 100 if stats['discovered'] > 0 else 0
        print(f"  {txtype}: {stats['total']} total, {discovery_rate:.1f}% discovery, {excellence_rate:.1f}% excellent")
    
    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"enhanced_missing_data_crossref_analysis_{timestamp}.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'pmid', 'category', 'status', 'txerror_type', 'original_doi', 'discovered_doi',
            'confidence', 'title_similarity', 'match_quality',
            'original_title', 'original_journal', 'original_year',
            'crossref_title', 'crossref_journal', 'validation_notes'
        ])
        
        for result in results:
            writer.writerow([
                result.pmid, result.category, result.status, result.txerror_type, result.original_doi,
                result.discovered_doi, result.confidence, result.title_similarity,
                result.match_quality, result.original_title, result.original_journal,
                result.original_year, result.crossref_title, result.crossref_journal,
                result.validation_notes
            ])
    
    print(f"\nDetailed results saved to: {output_file}")

if __name__ == "__main__":
    main()
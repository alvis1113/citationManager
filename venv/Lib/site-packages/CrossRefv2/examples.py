#!/usr/bin/env python3
"""
Examples and demos for CrossRef v2 enhanced DOI discovery

This module provides example usage and demonstration scripts for the
enhanced CrossRef DOI discovery system.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from CrossRefv2 import EnhancedCrossRefFetcher, find_doi_for_pmid, batch_find_dois, validate_doi_discovery
from metapub import PubMedFetcher
import logging

# Configure logging for demos
logging.basicConfig(level=logging.INFO)


def demo_single_pmid():
    """Demonstrate DOI discovery for a single PMID"""
    print("CrossRef v2 Demo: Single PMID DOI Discovery")
    print("=" * 50)
    
    # Example PMID that lacks DOI in PubMed
    pmid = "11745998"
    
    print(f"Finding DOI for PMID {pmid}...")
    doi = find_doi_for_pmid(pmid)
    
    if doi:
        print(f"✓ Found DOI: {doi}")
    else:
        print("✗ No DOI found")
    
    print()


def demo_batch_discovery():
    """Demonstrate batch DOI discovery"""
    print("CrossRef v2 Demo: Batch DOI Discovery")
    print("=" * 50)
    
    # Sample of problematic PMIDs from unsourceables.txt
    problem_pmids = [
        "11745998",  # Am J Med Genet - missing DOI
        "13817571",  # Pediatrics - missing DOI  
        "14299791",  # Mayo Clin Proc - missing DOI
        "11823443",  # Hum Mol Genet - has DOI (for comparison)
    ]
    
    print(f"Processing {len(problem_pmids)} PMIDs...")
    results = batch_find_dois(problem_pmids, min_confidence=0.7)
    
    print("\nResults:")
    for pmid, doi in results.items():
        if doi:
            print(f"  PMID {pmid}: ✓ {doi}")
        else:
            print(f"  PMID {pmid}: ✗ No DOI found")
    
    success_count = len([doi for doi in results.values() if doi])
    print(f"\nSuccess rate: {success_count}/{len(problem_pmids)} ({success_count/len(problem_pmids)*100:.1f}%)")
    print()


def demo_detailed_analysis():
    """Demonstrate detailed candidate analysis"""
    print("CrossRef v2 Demo: Detailed Candidate Analysis")
    print("=" * 50)
    
    # Example PMID for detailed analysis
    pmid = "11745998"
    
    fetch = PubMedFetcher()
    finder = EnhancedCrossRefFetcher()
    
    print(f"Analyzing PMID {pmid}...")
    
    try:
        pma = fetch.article_by_pmid(pmid)
        print(f"Article: {pma.title[:60]}...")
        print(f"Journal: {pma.journal} ({pma.year})")
        print(f"Volume/Issue/Page: {pma.volume}/{pma.issue}/{pma.first_page}")
        print(f"PubMed DOI: {pma.doi or 'None'}")
        print()
        
        candidates = finder.search_multiple_strategies(pma)
        
        print(f"Found {len(candidates)} candidates:")
        print("-" * 50)
        
        for i, candidate in enumerate(candidates[:5], 1):  # Show top 5
            print(f"{i}. DOI: {candidate.doi}")
            print(f"   Title: {candidate.title[:50]}...")
            print(f"   Confidence: {candidate.confidence:.3f}")
            print(f"   Title similarity: {candidate.title_similarity:.3f}")
            print(f"   Year match: {candidate.year_match}")
            print(f"   Volume match: {candidate.volume_match}")
            print(f"   Page match: {candidate.page_match}")
            print(f"   CrossRef score: {candidate.score}")
            print()
            
    except Exception as e:
        print(f"Error: {e}")
    
    print()


def demo_validation():
    """Demonstrate validation on test set"""
    print("CrossRef v2 Demo: Validation on Test Set")
    print("=" * 50)
    
    # Test set of problematic PMIDs
    test_pmids = [
        "11745998", "11746017", "11746019", "11754051", "11770813",
        "13817571", "14299791", "14527360", "13863929", "14343445"
    ]
    
    print(f"Validating on {len(test_pmids)} test PMIDs...")
    results = validate_doi_discovery(test_pmids)
    
    print(f"\nValidation Results:")
    print(f"Total tested: {results['total_tested']}")
    print(f"Already had DOI: {results['already_had_doi']}")
    print(f"Successful discoveries: {results['successful']}")
    print(f"Failed discoveries: {results['failed']}")
    print(f"Discovery success rate: {results['success_rate']:.1%}")
    
    print(f"\nDetailed results:")
    for pmid, details in results['details'].items():
        status = details['status']
        if status == 'success':
            print(f"  PMID {pmid}: ✓ Discovered {details['discovered_doi']}")
        elif status == 'already_has_doi':
            print(f"  PMID {pmid}: = Already had {details['existing_doi']}")
        else:
            print(f"  PMID {pmid}: ✗ {status}")
    
    print()


def demo_findit_integration():
    """Demonstrate integration with FindIt"""
    print("CrossRef v2 Demo: FindIt Integration")
    print("=" * 50)
    
    pmid = "11745998"
    
    fetch = PubMedFetcher()
    finder = EnhancedCrossRefFetcher()
    
    try:
        # Get original PMA
        pma = fetch.article_by_pmid(pmid)
        print(f"Original PMID {pmid}:")
        print(f"  Journal: {pma.journal}")
        print(f"  DOI: {pma.doi or 'None'}")
        
        # Discover DOI
        if not pma.doi:
            discovered_doi = finder.find_best_doi(pma, min_confidence=0.7)
            if discovered_doi:
                print(f"  Discovered DOI: {discovered_doi}")
                
                # Test with DOI slide dance
                from metapub.findit.dances.generic import the_doi_slide
                
                # Create enhanced PMA wrapper
                class EnhancedPMA:
                    def __init__(self, pma, doi):
                        self._pma = pma
                        self._doi = doi
                    def __getattr__(self, name):
                        if name == 'doi':
                            return self._doi
                        return getattr(self._pma, name)
                
                enhanced_pma = EnhancedPMA(pma, discovered_doi)
                
                try:
                    url = the_doi_slide(enhanced_pma, verify=False)
                    print(f"  ✓ Generated PDF URL: {url}")
                except Exception as e:
                    print(f"  ✗ URL generation failed: {e}")
            else:
                print("  ✗ No DOI discovered")
        else:
            print("  = Article already has DOI")
            
    except Exception as e:
        print(f"Error: {e}")
    
    print()


def run_all_demos():
    """Run all demonstration functions"""
    print("CrossRef v2 Enhanced DOI Discovery - Complete Demo")
    print("=" * 60)
    print()
    
    demo_single_pmid()
    demo_batch_discovery()
    demo_detailed_analysis()
    demo_validation()
    demo_findit_integration()
    
    print("All demos completed!")


if __name__ == "__main__":
    run_all_demos()
"""
Utility functions for CrossRef v2 enhanced DOI discovery

This module provides convenience functions and helpers for the enhanced
CrossRef DOI discovery system.
"""

from typing import Optional, List, Dict
from .enhanced_fetcher import EnhancedCrossRefFetcher, DOICandidate


def find_doi_for_pmid(pmid: str, min_confidence: float = 0.7, email: str = "metapub@research.org") -> Optional[str]:
    """
    Convenience function to find DOI for a single PMID
    
    Args:
        pmid: PubMed ID to search for
        min_confidence: Minimum confidence threshold (0.0-1.0)
        email: Email for CrossRef API requests
        
    Returns:
        DOI string if found with sufficient confidence, None otherwise
        
    Example:
        >>> from CrossRefv2 import find_doi_for_pmid
        >>> doi = find_doi_for_pmid("11745998")
        >>> print(doi)  # "10.1002/ajmg.1535"
    """
    from metapub import PubMedFetcher
    
    fetch = PubMedFetcher()
    finder = EnhancedCrossRefFetcher(email=email)
    
    try:
        pma = fetch.article_by_pmid(pmid)
        return finder.find_best_doi(pma, min_confidence=min_confidence)
    except Exception as e:
        print(f"Error finding DOI for PMID {pmid}: {e}")
        return None


def batch_find_dois(pmids: List[str], min_confidence: float = 0.7, email: str = "metapub@research.org") -> Dict[str, Optional[str]]:
    """
    Find DOIs for multiple PMIDs in batch
    
    Args:
        pmids: List of PubMed IDs to search for
        min_confidence: Minimum confidence threshold (0.0-1.0)
        email: Email for CrossRef API requests
        
    Returns:
        Dictionary mapping PMID -> DOI (or None if not found)
        
    Example:
        >>> from CrossRefv2 import batch_find_dois
        >>> results = batch_find_dois(["11745998", "13817571"])
        >>> print(results)  # {"11745998": "10.1002/ajmg.1535", "13817571": "10.1542/peds.24.5.786"}
    """
    from metapub import PubMedFetcher
    
    fetch = PubMedFetcher()
    finder = EnhancedCrossRefFetcher(email=email)
    
    results = {}
    
    for pmid in pmids:
        try:
            pma = fetch.article_by_pmid(pmid)
            doi = finder.find_best_doi(pma, min_confidence=min_confidence)
            results[pmid] = doi
        except Exception as e:
            print(f"Error processing PMID {pmid}: {e}")
            results[pmid] = None
            
    return results


def get_detailed_candidates(pmid: str, email: str = "metapub@research.org") -> List[DOICandidate]:
    """
    Get detailed candidate information for a PMID (for debugging/analysis)
    
    Args:
        pmid: PubMed ID to search for
        email: Email for CrossRef API requests
        
    Returns:
        List of DOICandidate objects with detailed match information
        
    Example:
        >>> from CrossRefv2 import get_detailed_candidates
        >>> candidates = get_detailed_candidates("11745998")
        >>> for c in candidates[:3]:  # Show top 3
        ...     print(f"DOI: {c.doi}, Confidence: {c.confidence:.3f}")
    """
    from metapub import PubMedFetcher
    
    fetch = PubMedFetcher()
    finder = EnhancedCrossRefFetcher(email=email)
    
    try:
        pma = fetch.article_by_pmid(pmid)
        return finder.search_multiple_strategies(pma)
    except Exception as e:
        print(f"Error getting candidates for PMID {pmid}: {e}")
        return []


def validate_doi_discovery(pmids: List[str], email: str = "metapub@research.org") -> Dict[str, Dict]:
    """
    Validate DOI discovery performance on a set of PMIDs
    
    Args:
        pmids: List of PubMed IDs to test
        email: Email for CrossRef API requests
        
    Returns:
        Dictionary with validation statistics and per-PMID results
        
    Example:
        >>> from CrossRefv2 import validate_doi_discovery
        >>> results = validate_doi_discovery(["11745998", "13817571"])
        >>> print(f"Success rate: {results['success_rate']:.1%}")
    """
    from metapub import PubMedFetcher
    
    fetch = PubMedFetcher()
    finder = EnhancedCrossRefFetcher(email=email)
    
    results = {
        'total_tested': len(pmids),
        'successful': 0,
        'failed': 0,
        'already_had_doi': 0,
        'details': {}
    }
    
    for pmid in pmids:
        try:
            pma = fetch.article_by_pmid(pmid)
            
            if pma.doi:
                results['already_had_doi'] += 1
                results['details'][pmid] = {
                    'status': 'already_has_doi',
                    'existing_doi': pma.doi,
                    'discovered_doi': None
                }
            else:
                discovered_doi = finder.find_best_doi(pma, min_confidence=0.7)
                
                if discovered_doi:
                    results['successful'] += 1
                    results['details'][pmid] = {
                        'status': 'success',
                        'existing_doi': None,
                        'discovered_doi': discovered_doi
                    }
                else:
                    results['failed'] += 1
                    results['details'][pmid] = {
                        'status': 'failed',
                        'existing_doi': None,
                        'discovered_doi': None
                    }
                    
        except Exception as e:
            results['failed'] += 1
            results['details'][pmid] = {
                'status': 'error',
                'error': str(e),
                'existing_doi': None,
                'discovered_doi': None
            }
    
    # Calculate success rate
    discoverable = results['total_tested'] - results['already_had_doi']
    if discoverable > 0:
        results['success_rate'] = results['successful'] / discoverable
    else:
        results['success_rate'] = 0.0
        
    return results
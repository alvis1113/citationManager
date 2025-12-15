"""
CrossRef v2 - Enhanced DOI Discovery System

This module provides an enhanced CrossRef API implementation with improved
DOI discovery capabilities for articles missing DOIs in PubMed metadata.

Key improvements over the original CrossRefFetcher:
- Multiple search strategies for better coverage
- Year-based filtering for historical articles  
- Volume/Issue/Page searches for VIP articles
- Enhanced confidence scoring and validation
- Fuzzy title matching using Levenshtein distance

Classes:
    DOICandidate: Represents a potential DOI match with confidence metrics
    EnhancedCrossRefFetcher: Main class for enhanced DOI discovery
    
Functions:
    find_doi_for_pmid: Convenience function for single PMID DOI discovery
"""

from .enhanced_fetcher import EnhancedCrossRefFetcher, DOICandidate
from .utils import find_doi_for_pmid, batch_find_dois, validate_doi_discovery, get_detailed_candidates

__version__ = "2.0.0"
__author__ = "metapub enhanced by Claude Code"

__all__ = [
    'EnhancedCrossRefFetcher',
    'DOICandidate', 
    'find_doi_for_pmid',
    'batch_find_dois',
    'validate_doi_discovery', 
    'get_detailed_candidates'
]
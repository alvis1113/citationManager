"""
Enhanced CrossRef DOI Discovery System

This module implements an improved DOI discovery strategy using multiple
CrossRef API approaches to find DOIs for papers missing from PubMed metadata.

Key improvements over current metapub implementation:
1. Multiple search strategies with different parameter combinations
2. Year-based filtering to improve historical article matching
3. Volume/issue/page-based searches for VIP articles
4. Better author name formatting and matching
5. Enhanced result scoring and validation
6. More comprehensive search result analysis
"""

import requests
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
import Levenshtein
import re

# Set up logging
log = logging.getLogger('CrossRefv2')

@dataclass
class DOICandidate:
    """Represents a potential DOI match from CrossRef"""
    doi: str
    title: str
    score: float
    title_similarity: float
    year_match: bool
    volume_match: bool
    page_match: bool
    confidence: float

class EnhancedCrossRefFetcher:
    """Enhanced DOI discovery using multiple CrossRef strategies"""
    
    def __init__(self, email="metapub@research.org"):
        self.base_url = "https://api.crossref.org/works"
        self.headers = {'User-Agent': f'Enhanced CrossRef Fetcher/2.0 (mailto:{email})'}
        self.session = requests.Session()
        
    def search_multiple_strategies(self, pma) -> List[DOICandidate]:
        """Try multiple search strategies and combine results"""
        
        all_candidates = []
        
        # Strategy 1: Title + Journal + Year filter
        candidates = self._search_title_journal_year(pma)
        all_candidates.extend(candidates)
        
        # Strategy 2: Title + Author + Year filter  
        candidates = self._search_title_author_year(pma)
        all_candidates.extend(candidates)
        
        # Strategy 3: Volume/Issue/Page search for VIP articles
        if pma.volume and pma.first_page:
            candidates = self._search_volume_issue_page(pma)
            all_candidates.extend(candidates)
            
        # Strategy 4: Author + Journal + Year (for cases with common titles)
        candidates = self._search_author_journal_year(pma)
        all_candidates.extend(candidates)
        
        # Strategy 5: Partial title search (for very long titles)
        if len(pma.title) > 100:
            candidates = self._search_partial_title(pma)
            all_candidates.extend(candidates)
            
        # Deduplicate and score candidates
        return self._deduplicate_and_score(all_candidates, pma)
    
    def _search_title_journal_year(self, pma) -> List[DOICandidate]:
        """Search by title + journal with year filtering"""
        if not pma.title or not pma.journal:
            return []
            
        params = {
            'query.bibliographic': pma.title,
            'query.container-title': pma.journal,
            'rows': 20
        }
        
        # Add year filtering if available
        if pma.year:
            year = int(pma.year)
            params['filter'] = f'from-pub-date:{year-1},until-pub-date:{year+1}'
            
        return self._execute_search(params, pma, "title_journal_year")
    
    def _search_title_author_year(self, pma) -> List[DOICandidate]:
        """Search by title + first author with year filtering"""
        if not pma.title or not pma.author1_lastfm:
            return []
            
        # Try different author formats
        author_formats = [
            pma.author1_lastfm,  # "LastName F"
            pma.author1_lastfm.replace(' ', ''),  # "LastNameF"
            pma.author1_lastfm.split()[0] if ' ' in pma.author1_lastfm else pma.author1_lastfm  # Just "LastName"
        ]
        
        all_candidates = []
        for author_format in author_formats:
            params = {
                'query.bibliographic': pma.title,
                'query.author': author_format,
                'rows': 15
            }
            
            if pma.year:
                year = int(pma.year)
                params['filter'] = f'from-pub-date:{year-1},until-pub-date:{year+1}'
                
            candidates = self._execute_search(params, pma, f"title_author_year_{author_format}")
            all_candidates.extend(candidates)
            
        return all_candidates
    
    def _search_volume_issue_page(self, pma) -> List[DOICandidate]:
        """Search using volume/issue/page information for VIP articles"""
        if not (pma.journal and pma.volume and pma.first_page):
            return []
            
        # Create bibliographic query with vol/issue/page
        bib_parts = [pma.journal]
        if pma.volume:
            bib_parts.append(f"volume {pma.volume}")
        if pma.issue:
            bib_parts.append(f"issue {pma.issue}")
        if pma.first_page:
            bib_parts.append(f"page {pma.first_page}")
            
        bib_query = " ".join(bib_parts)
        
        params = {
            'query.bibliographic': bib_query,
            'rows': 15
        }
        
        if pma.year:
            year = int(pma.year)
            params['filter'] = f'from-pub-date:{year-1},until-pub-date:{year+1}'
            
        return self._execute_search(params, pma, "volume_issue_page")
    
    def _search_author_journal_year(self, pma) -> List[DOICandidate]:
        """Search by author + journal + year (useful for common titles)"""
        if not (pma.author1_lastfm and pma.journal):
            return []
            
        params = {
            'query.author': pma.author1_lastfm,
            'query.container-title': pma.journal,
            'rows': 15
        }
        
        if pma.year:
            year = int(pma.year)
            params['filter'] = f'from-pub-date:{year-1},until-pub-date:{year+1}'
            
        return self._execute_search(params, pma, "author_journal_year")
    
    def _search_partial_title(self, pma) -> List[DOICandidate]:
        """Search using first part of very long titles"""
        if not pma.title or len(pma.title) <= 100:
            return []
            
        # Use first significant part of title (up to first period or 80 chars)
        partial_title = pma.title
        if '.' in partial_title:
            partial_title = partial_title.split('.')[0]
        else:
            partial_title = partial_title[:80]
            
        params = {
            'query.bibliographic': partial_title,
            'query.container-title': pma.journal,
            'rows': 15
        }
        
        if pma.year:
            year = int(pma.year)
            params['filter'] = f'from-pub-date:{year-1},until-pub-date:{year+1}'
            
        return self._execute_search(params, pma, "partial_title")
    
    def _execute_search(self, params: Dict, pma, strategy: str) -> List[DOICandidate]:
        """Execute CrossRef search and convert results to candidates"""
        try:
            response = self.session.get(self.base_url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            candidates = []
            total_results = data['message']['total-results']
            log.debug(f"Strategy '{strategy}': {total_results} results")
            
            for item in data['message']['items']:
                candidate = self._create_candidate(item, pma, strategy)
                if candidate:
                    candidates.append(candidate)
                    
            return candidates
            
        except Exception as e:
            log.warning(f"Search strategy '{strategy}' failed: {e}")
            return []
    
    def _create_candidate(self, item: Dict, pma, strategy: str) -> Optional[DOICandidate]:
        """Create a DOI candidate from CrossRef search result"""
        doi = item.get('DOI')
        if not doi:
            return None
            
        title_list = item.get('title', [])
        title = title_list[0] if title_list else ""
        score = item.get('score', 0)
        
        # Calculate title similarity
        title_similarity = 0.0
        if title and pma.title:
            title_similarity = Levenshtein.ratio(pma.title.lower(), title.lower())
        
        # Check year match
        year_match = False
        if pma.year and item.get('issued'):
            try:
                issued_year = item['issued']['date-parts'][0][0]
                year_match = abs(int(pma.year) - issued_year) <= 1
            except (KeyError, IndexError, ValueError):
                pass
        
        # Check volume match
        volume_match = False
        if pma.volume and item.get('volume'):
            volume_match = str(pma.volume).strip() == str(item['volume']).strip()
        
        # Check page match
        page_match = False
        if pma.first_page and item.get('page'):
            item_first_page = item['page'].split('-')[0] if '-' in item['page'] else item['page']
            page_match = str(pma.first_page).strip() == item_first_page.strip()
        
        # Calculate confidence score
        confidence = self._calculate_confidence(
            title_similarity, year_match, volume_match, page_match, score, strategy
        )
        
        return DOICandidate(
            doi=doi,
            title=title,
            score=score,
            title_similarity=title_similarity,
            year_match=year_match,
            volume_match=volume_match,
            page_match=page_match,
            confidence=confidence
        )
    
    def _calculate_confidence(self, title_sim: float, year_match: bool, 
                            volume_match: bool, page_match: bool, 
                            score: float, strategy: str) -> float:
        """Calculate confidence score for a candidate"""
        confidence = 0.0
        
        # Title similarity is most important
        confidence += title_sim * 0.4
        
        # Year match is important for disambiguation  
        if year_match:
            confidence += 0.25
            
        # Volume/page matches are strong indicators
        if volume_match:
            confidence += 0.15
        if page_match:
            confidence += 0.15
            
        # CrossRef relevance score (normalized)
        normalized_score = min(score / 100.0, 0.2)  # Cap at 0.2
        confidence += normalized_score
        
        # Strategy bonus (some strategies are more reliable)
        strategy_bonus = {
            'title_journal_year': 0.05,
            'volume_issue_page': 0.1,  # VIP searches are very specific
            'partial_title': -0.05      # Partial titles are less reliable
        }
        confidence += strategy_bonus.get(strategy, 0.0)
        
        return min(confidence, 1.0)
    
    def _deduplicate_and_score(self, candidates: List[DOICandidate], pma) -> List[DOICandidate]:
        """Remove duplicate DOIs and sort by confidence"""
        seen_dois = set()
        unique_candidates = []
        
        for candidate in candidates:
            if candidate.doi not in seen_dois:
                seen_dois.add(candidate.doi)
                unique_candidates.append(candidate)
        
        # Sort by confidence score (highest first)
        unique_candidates.sort(key=lambda x: x.confidence, reverse=True)
        
        return unique_candidates
    
    def find_best_doi(self, pma, min_confidence: float = 0.7) -> Optional[str]:
        """Find the best DOI match for a PubMedArticle"""
        
        # Skip if article already has DOI
        if pma.doi:
            return pma.doi
            
        log.info(f"Searching for DOI: PMID {pma.pmid} - {pma.title[:50]}...")
        
        candidates = self.search_multiple_strategies(pma)
        
        if not candidates:
            log.info(f"No DOI candidates found for PMID {pma.pmid}")
            return None
            
        best_candidate = candidates[0]
        
        log.info(f"Best candidate for PMID {pma.pmid}:")
        log.info(f"  DOI: {best_candidate.doi}")
        log.info(f"  Confidence: {best_candidate.confidence:.3f}")
        log.info(f"  Title similarity: {best_candidate.title_similarity:.3f}")
        log.info(f"  Year match: {best_candidate.year_match}")
        log.info(f"  Volume match: {best_candidate.volume_match}")
        log.info(f"  Page match: {best_candidate.page_match}")
        
        if best_candidate.confidence >= min_confidence:
            log.info(f"✓ High confidence DOI found: {best_candidate.doi}")
            return best_candidate.doi
        else:
            log.info(f"✗ Low confidence ({best_candidate.confidence:.3f} < {min_confidence})")
            return None
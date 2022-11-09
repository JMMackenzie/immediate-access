#pragma once

#ifdef VARIABLE_BLOCK
#include "variable_postings_cursor.hpp"
#else
#include "postings_cursor.hpp"
#endif

#include "ranking.hpp"
#include "topk_queue.hpp"
#include "query.hpp"

// Inspired by PISA's implementations
//
size_t boolean_conjunction(std::vector<postings_cursor>& cursors) {

  if (cursors.size() == 0) {
    return 0;
  }

  std::vector<uint32_t> results;

  std::vector<postings_cursor*> ordered_cursors;
  ordered_cursors.reserve(cursors.size());
  for (auto& curs : cursors) {
    ordered_cursors.push_back(&curs);
  }

  // Order short to long
  std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](postings_cursor* l, postings_cursor* r) {
    return l->doc_freq() < r->doc_freq();
  });

  uint32_t candidate = ordered_cursors[0]->docid();
  size_t i = 1;
  
  while (candidate != END_CHAIN) {
    for(; i < ordered_cursors.size(); ++i) {
      ordered_cursors[i]->next_geq(candidate);
      
      if (ordered_cursors[i]->docid() != candidate) {
        i = 0;
        break;
      }
    }

    if (i == ordered_cursors.size()) {
      results.push_back(candidate);
    }
        
    ordered_cursors[0]->next(); 
    candidate = ordered_cursors[0]->docid();
    i = 1;
 
  }
  return results.size();
}

size_t profile_boolean_conjunction(std::vector<postings_cursor>& cursors) {

  if (cursors.size() == 0) {
    return 0;
  }

  std::vector<uint32_t> results;

  std::vector<postings_cursor*> ordered_cursors;
  ordered_cursors.reserve(cursors.size());
  for (auto& curs : cursors) {
    ordered_cursors.push_back(&curs);
  }

  // Order short to long
  std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](postings_cursor* l, postings_cursor* r) {
    return l->doc_freq() < r->doc_freq();
  });

  uint32_t candidate = ordered_cursors[0]->docid();
  size_t i = 1;

  std::vector<size_t> geq_count(ordered_cursors.size());

  while (candidate != END_CHAIN) {
    
    for(; i < ordered_cursors.size(); ++i) {
      
      ordered_cursors[i]->next_geq(candidate);
      geq_count[i]++;

      if (ordered_cursors[i]->docid() != candidate) {
        i = 0;
        break;
      }
    }

    if (i == ordered_cursors.size()) {
      results.push_back(candidate);
    }
    
    geq_count[0]++;
    ordered_cursors[0]->next();
    candidate = ordered_cursors[0]->docid();
    i = 1;

  }

  std::cout << "------\n";
  for(size_t i = 0; i < ordered_cursors.size(); ++i) {
    std::cout << "[" << i << "] -> " << ordered_cursors[i]->term() << "  df= " << ordered_cursors[i]->doc_freq() << "  next_geq_count= " << geq_count[i] << "\n";
  }


  return results.size();
}


// Heavily based on PISA's algos
size_t boolean_disjunction(std::vector<postings_cursor>& cursors) {

  if (cursors.size() == 0) {
    return 0;
  }

  size_t results = 0;
  
  uint32_t candidate = 
    std::min_element(cursors.begin(), cursors.end(), [](auto const& l, auto const& r) {
        return l.docid() < r.docid();
    })->docid();
  
  while (candidate != END_CHAIN) {
    results += 1;
    uint32_t next_doc = END_CHAIN;
    for(size_t i = 0; i < cursors.size(); ++i) {
      if (cursors[i].docid() == candidate) {
        cursors[i].next();
      }
      if (cursors[i].docid() < next_doc) {
        next_doc = cursors[i].docid();
      }
    }
    candidate = next_doc;
  }
 
  return results;
}

// Heavily based on PISA's algos
size_t ranked_disjunction(std::vector<postings_cursor>& cursors, tfidf_ranker& ranker, topk_queue& results) {

  if (cursors.size() == 0) {
    return 0;
  }

  uint32_t candidate = 
    std::min_element(cursors.begin(), cursors.end(), [](auto const& l, auto const& r) {
        return l.docid() < r.docid();
    })->docid();
  
  while (candidate != END_CHAIN) {
    float score = 0;
    uint32_t next_doc = END_CHAIN;
    for(size_t i = 0; i < cursors.size(); ++i) {
      if (cursors[i].docid() == candidate) {
        score += ranker.tf_weight(cursors[i].freq()) * ranker.idf_weight(cursors[i].doc_freq());
        cursors[i].next();
      }
      if (cursors[i].docid() < next_doc) {
        next_doc = cursors[i].docid();
      }
    }
    results.insert(score, candidate);
    candidate = next_doc;
  }
  results.finalize(); 
  return results.size();
}



#pragma once

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <limits>
#include <chrono>
#include <memory>
#include <functional>
#include <unordered_set>
#include <map>
#include <numeric>
#include <type_traits>
#include <cmath>

#define VARIABLE_BLOCK

// A placeholder for the end/null/etc
const uint32_t END_CHAIN = std::numeric_limits<uint32_t>::max();

// A value corresponding to the desired size of the hash table w.r.t vocab
// That is, the hash table will be HASH_VOCAB_SIZE * |V| entries
const size_t HASH_VOCAB_SIZE = 2;

// A guesstimate of the number of chars (bytes) in a word
const size_t AVERAGE_WORD_BYTES = 8;

inline double get_time_usecs() {
  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

struct term_position {
    std::string m_term;
    std::vector<uint32_t> m_positions;
};

// A full document for convenience
// A document is simply a textual identifier, and a list of term/positions
struct plain_document {
  std::string m_text_id;
  std::vector<term_position> m_terms;
  size_t m_length;
  size_t m_unique_terms;

  size_t length() const {
    return m_length;
  }

  size_t postings() const {
    return m_unique_terms;
  }

  uint32_t term_frequency(std::string term) {
    for (size_t i = 0; i < m_terms.size(); ++i) {
      // Do we have the term?
      if (m_terms[i].m_term == term) {
        return m_terms[i].m_positions.size();
      }
    }
    return 0;
  }
};


struct plain_collection {
  std::vector<plain_document> m_documents;
  size_t m_total_terms = 0;
  size_t m_unique_terms = 0;
  size_t m_total_postings = 0;

  // This is the number of documents
  size_t size() const {
    return m_documents.size(); 
  }

  // This is the sum of distinct terms in each document across the collection
  size_t postings() const {
    return m_total_postings;
  }

  // This is the total number of words in the collection
  size_t terms() const {
    return m_total_terms;
  }

  // This is the size of the vocab across the collection
  size_t unique_terms() const {
    return m_unique_terms;
  }

};

// Reads a document collection from a file and returns it
// File format: <string_id> <term_1> <term_2> ...
plain_collection read_full_collection(std::ifstream& in) {
  std::unordered_set<std::string> all_terms;
  plain_collection collection;
  std::vector<plain_document> documents;
  std::string line;
  // For each doc
  while (std::getline(in, line)) {
    plain_document in_doc;
    std::map<std::string, std::vector<uint32_t>> term_to_pos;
    std::istringstream line_data(line);
    line_data >> in_doc.m_text_id;
    std::string term;
    uint32_t position = 1; // Index from 1
    while (line_data >> term) {
      term_to_pos[term].push_back(position);
      all_terms.insert(term);
      position++;
    }
    in_doc.m_unique_terms = term_to_pos.size();
    in_doc.m_length = position - 1;
    collection.m_total_postings += term_to_pos.size();
    collection.m_total_terms += position - 1;
    // Flatten the map back into term_positions
    in_doc.m_terms.resize(term_to_pos.size());
    size_t idx = 0;
    for (auto & element : term_to_pos) {
      in_doc.m_terms[idx].m_term = element.first;
      in_doc.m_terms[idx].m_positions = element.second;
      ++idx;
    }
    collection.m_documents.push_back(in_doc);
  }
  collection.m_unique_terms = all_terms.size();
  return collection;
}



// PISA's do_not_optimize_away
namespace detail {
    template <typename T>
    struct do_not_optimize_away_needs_indirect {
        using Decayed = typename std::decay<T>::type;
        constexpr static bool value = !std::is_trivially_copyable<Decayed>::value
            || sizeof(Decayed) > sizeof(long) || std::is_pointer<Decayed>::value;
    };
}  // namespace detail

template <typename T>
auto do_not_optimize_away(const T& datum) ->
    typename std::enable_if<!detail::do_not_optimize_away_needs_indirect<T>::value>::type
{
    asm volatile("" ::"r"(datum));
}

template <typename T>
auto do_not_optimize_away(const T& datum) ->
    typename std::enable_if<detail::do_not_optimize_away_needs_indirect<T>::value>::type
{
    asm volatile("" ::"m"(datum) : "memory");
}

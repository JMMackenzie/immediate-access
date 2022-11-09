#pragma once

#include "util.hpp"
#include "compress.hpp"
#include "query.hpp"
#include "variable_immediate_index.hpp"

// Same as the default postings cursor but it can handle
// variable sized blocks
class postings_cursor {

 public:
  // Give the cursor a const reference to the index
  postings_cursor(immediate_index& index, std::string term) : 
                                            m_index(index),
                                            m_term(term),
                                            m_head_block(END_CHAIN),
                                            m_tail_block(END_CHAIN), 
                                            m_doc_freq(END_CHAIN),
                                            m_current_block(END_CHAIN), 
                                            m_current_offset(END_CHAIN),
                                            m_gap_accumulator(0), 
                                            m_current_docid(0),
                                            m_current_tf(0),
                                            m_block_count(0) {
    
    // Find the entry location in the hash table
    uint32_t entry_hash = m_index.found_or_empty_offset(term);
    m_current_block = m_index.get_offset(entry_hash);
    if (m_current_block == END_CHAIN) {
      std::cerr << "Warning: Could not find term [" << term << "]\n";
    } else {
      m_head_block = m_current_block;
      m_tail_block = m_index.tail_block(m_current_block);
      m_doc_freq = m_index.doc_freq(m_current_block);
      m_current_offset = m_index.head_data_offset(m_current_block);
      this->next();
    }
  }

  // Valid cursors head blocks are indexes
  bool valid() const {
    return m_head_block != END_CHAIN;
  }

  uint32_t doc_freq() const {
    return m_doc_freq;
  }

  uint32_t docid() const {
    return m_current_docid; 
  }

  uint32_t freq() const {
    return m_current_tf;
  }

  std::string term() const {
    return m_term;
  }

  void reset() {
    m_current_block = m_head_block;
    m_current_offset = m_index.head_data_offset(m_current_block);
    m_current_docid = 0;
    m_gap_accumulator = 0;
    m_block_count = 0;
    this->next();
  }

  // Access will implicitly move us forward
  void next() {
    // make sure we don't overrun the block, and make sure we have data to read
    if (m_current_offset < (m_index.slab_size(m_block_count)*BLOCK_SIZE) && m_index.has_data(m_current_block, m_current_offset)) {
      // m_current_offset is modified by this call
      auto data = m_index.access(m_current_block, m_current_offset);
      m_current_docid += data.first;
      m_current_tf = data.second;
    } else { // Look for the next block
      m_block_count += 1;
      m_block_count = std::min(m_block_count, MAX_SLAB_IDX);
      auto next_block = m_index.next_block(m_current_block, m_tail_block);
      // We have exhausted the list
      if (next_block == END_CHAIN) {
        m_current_block = END_CHAIN;
        m_current_docid = END_CHAIN;
        return;
      }
      // Update current values
      m_current_block = next_block;
      m_current_offset = TT_PL_OFFSET;
      // this is a new block, so we have a b-gap...
      auto data = m_index.access(m_current_block, m_current_offset);
      m_gap_accumulator += data.first;
      m_current_docid = m_gap_accumulator;
      m_current_tf = data.second;
    }
  }

  // Within-block next_geq
  void advance_to_id(uint32_t target_docid) {
    while (m_current_docid < target_docid) {
      next();
    }
  }

  // Find the first document in the index >= docid
  void next_geq(uint32_t target_docid) {

    // XXX should never happen
    if (target_docid <= m_current_docid)
      return;

    // Assume it's not in this block
    uint32_t current_block = m_current_block;
    uint32_t current_docid = m_gap_accumulator;
    uint32_t prev_block = m_current_block;
    uint32_t prev_docid = m_gap_accumulator;
    size_t block_count = m_block_count;
 
    // Skip ahead until we run over the target or run out of stuff
    while (current_docid < target_docid && current_block != END_CHAIN) {
      // Save these values for when we unwind
      prev_block = current_block;
      prev_docid = current_docid;
      block_count += 1;
      // look ahead now
      current_block = m_index.next_block(current_block, m_tail_block);
      if (current_block != END_CHAIN) {
        size_t temp_offset = TT_PL_OFFSET;
        auto data = m_index.access(current_block, temp_offset);
        current_docid += data.first;
      }
    }

    // We've overran the document and now need to backtrack by one block
    if (current_docid > target_docid || current_block == END_CHAIN) {
      m_current_block = prev_block;
      m_gap_accumulator = prev_docid;
      m_current_docid = prev_docid;
      m_block_count = block_count - 1;
    } 

    // we either hit the target or didn't see it yet
    else {
      m_current_block = current_block;
      m_gap_accumulator = current_docid;
      m_current_docid = current_docid;
      m_block_count = block_count;
    }

    // Need to fix the alignment
    size_t offset = TT_PL_OFFSET;
    if (m_current_block == m_head_block) {
      offset = m_index.head_data_offset(m_current_block);
      auto data = m_index.access(m_current_block, offset);
      m_current_docid = data.first;
      m_current_tf = data.second;
      m_current_offset = offset;
    } else {
      auto data = m_index.access(m_current_block, offset);
      m_current_tf = data.second;
      m_current_offset = offset;
    }
   
    m_block_count = std::min(m_block_count, MAX_SLAB_IDX);
    advance_to_id(target_docid);
  }

  // Members 
  private:
    immediate_index& m_index;
    std::string m_term;
    uint32_t m_head_block;
    uint32_t m_tail_block;
    uint32_t m_doc_freq;
    uint32_t m_current_block;
    size_t m_current_offset;
    uint32_t m_gap_accumulator;
    uint32_t m_current_docid;
    uint32_t m_current_tf;
    uint32_t m_block_count;
};

// Given an index and a query, return a vector of cursors into the index
std::vector<postings_cursor> 
query_to_cursors(immediate_index& index, query in_query) {

  std::vector<postings_cursor> cursors;

  // XXX assumes terms are unique!
  for (auto term : in_query.m_terms) {
    auto cursor = postings_cursor(index, term);
    if (cursor.valid()) {
      cursors.push_back(cursor);
    }
  }
  return cursors;
}

#pragma once
#include <string.h>

#include "util.hpp"
#include "compress.hpp"

// NOTE: In the following head/torso/tail structures, access to the
// m_bytes buffer is computed as an offset from the memory address
// corresponding to m_next_block, the _beginning of the struct_.
// See: https://en.cppreference.com/w/cpp/named_req/StandardLayoutType

// Constants representing the index, access locations, etc
const size_t BLOCK_SIZE = 64;
const size_t HEAD_PL_OFFSET = (4 * sizeof(uint32_t) + 2 * sizeof(uint8_t) + sizeof(uint16_t));
const size_t TT_PL_OFFSET = sizeof(uint32_t);
const size_t HEAD_BYTES = BLOCK_SIZE - HEAD_PL_OFFSET;
const size_t TT_BYTES = BLOCK_SIZE - TT_PL_OFFSET;

// The number of slab sizes we store
const uint32_t MAX_SLAB_IDX = 255;

// The head structure
struct head_block {
 
  uint32_t m_next_block;       // index of next block for this term
  uint32_t m_tail_block;       // index of tail block for this term
  uint32_t m_doc_freq;         // number of postings for this term
  uint32_t m_recent_docid;     // most recently seen docid for this term
  uint16_t m_tail_byte_offset; // next unused byte in the tail block
  uint8_t  m_growth_offset;    // an index into the array of block growth values
  uint8_t  m_word_length;      // number of characters in the term
  uint8_t  m_bytes[HEAD_BYTES]; 
                               // m_word_length chars representing the term 
                               // string, and then variable-byte postings after

  // Initialize a head node; sets the term, updates offsets, etc
  void init(const std::string term, size_t self_index) {
    m_next_block = END_CHAIN;
    m_tail_block = self_index;
    m_doc_freq = 0;
    this->set_term(term);
    m_tail_byte_offset = HEAD_PL_OFFSET + m_word_length;
    m_recent_docid = 0;
    m_growth_offset = 0;
  }

  uint32_t next_block() const {
    return m_next_block;
  }

  void set_next_block(const uint32_t next_block) {
    m_next_block = next_block;
  }

  uint32_t tail_block() const {
    return m_tail_block;
  }

  void set_tail_block(const uint32_t tail_block) {
    m_tail_block = tail_block;
  }

  uint32_t doc_freq() const {
    return m_doc_freq;
  }

  void set_doc_freq(const uint32_t doc_freq) {
    m_doc_freq = doc_freq;
  }

  void increment_doc_freq() {
    m_doc_freq += 1;
  } 

  // Tells us which index to access on the precomputed slab size table
  // Note that we have to sit within [0, 255] as we index using a byte
  void increment_growth_offset() {
    if (m_growth_offset < MAX_SLAB_IDX) {
      m_growth_offset += 1;
    }
  }

  uint32_t growth_offset() const {
    return m_growth_offset;
  }

  // Returns the most recently seen docid during encoding
  uint32_t recent_docid() const {
    return m_recent_docid;
  }

  // Sets the most recently seen docid during encoding
  void set_recent_docid(const uint32_t recent_docid) {
    m_recent_docid = recent_docid;
  }

  // Reads m_word_length chars out of the start of the buffer, converts to a std::string
  std::string get_term() {
    return std::string(reinterpret_cast<const char *>(&m_bytes[0]), m_word_length);
  }

  // Given a new term, we put it in the buffer and store the length
  void set_term(const std::string& term) {
    m_word_length = term.size();
    std::copy(term.begin(), term.end(), std::begin(m_bytes));
  }

  uint16_t tail_byte_offset() const {
    return m_tail_byte_offset;
  }

  // Explicitly sets the offset
  void set_tail_byte_offset(const uint16_t tail_byte_offset) {
    m_tail_byte_offset = tail_byte_offset;
  }

  // Advances the offset by a stride
  void advance_tail_byte_offset(const size_t stride) {
    auto before = m_tail_byte_offset;
    m_tail_byte_offset += stride;
    if (before > m_tail_byte_offset) {
      std::cerr << "Overflow on byte offset\n";
    }
  }

  // Pointer to the memory address of the struct itself
  uint8_t* struct_ptr() {
    return reinterpret_cast<uint8_t *>(&m_next_block);
  }

  // Byte offset to first entry
  size_t data_offset() {
    return HEAD_PL_OFFSET + m_word_length;
  }

  // Returns the first encoded document identifier
  uint32_t first_docid() {
    size_t stride = 0;
    auto decoded_pair = decode_magic(&m_bytes[0] + m_word_length, stride);
    return decoded_pair.first; 
  }

};

// The "middle" blocks of a chain
struct torso_block {
  uint32_t m_next_block;      // index of the next block for this term
  uint8_t  m_bytes[TT_BYTES];
                              // variable-byte postings; first one is the b-gap
  
  // Initialize the block
  void init() {
    m_next_block = END_CHAIN;
  }

  uint32_t next_block() const {
    return m_next_block;
  }

  void set_next_block(const uint32_t next_block) {
    m_next_block = next_block;
  }

  // Pointer to the memory address of the struct itself
  uint8_t* struct_ptr() {
    return reinterpret_cast<uint8_t *>(&m_next_block);
  }

  // Decodes the first d-gap stored in the bytes buffer
  uint32_t first_docid() {
    size_t stride = 0;
    auto decoded_pair = decode_magic(&m_bytes[0], stride);
    return decoded_pair.first; 
  }

};

// The last block of a chain
struct tail_block {
  uint32_t m_first_docid; // The first docid in this block (uncompressed)
  uint8_t  m_bytes[TT_BYTES];
                              // variable-byte postings; first one is the b-gap

  void init(const uint32_t first_docid) {
    m_first_docid = first_docid;
  }

  uint32_t first_docid() const {
    return m_first_docid;
  }

  void set_first_docid(const uint32_t first_docid) {
    m_first_docid = first_docid;
  }

  // Pointer to the memory address of the struct itself
  uint8_t* struct_ptr() {
    return reinterpret_cast<uint8_t *>(&m_first_docid);
  }

};

// All blocks are the same size; make a union for allocating the index
typedef union {
  head_block head;
  torso_block torso;
  tail_block tail;
} index_block;



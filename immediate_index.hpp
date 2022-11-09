#pragma once
#include <string.h>

#include "util.hpp"
#include "compress.hpp"
#include "index_blocks.hpp"
#include "query.hpp"

// The structure of the whole index
class immediate_index {

  // Data structures
  private:
    size_t m_next_empty;
    std::vector<uint32_t> m_term_offsets;
    std::vector<index_block> m_data;

  // Functions
  public:

    // Default
    immediate_index() : m_next_empty(0) {}
    
    // Initialize the index
    immediate_index(size_t no_blocks, size_t no_hash_slots) {
      m_next_empty = 0;
      m_term_offsets.resize(no_hash_slots, END_CHAIN);
      m_data.resize(no_blocks);
    }

    // Writes to disk
    void serialize(std::ofstream& out) {
      // (1) Write total of "in-use" blocks
      out.write(reinterpret_cast<char *>(&m_next_empty), sizeof(size_t));
      // (2) Write the hash table size
      size_t ht_size = m_term_offsets.size();
      out.write(reinterpret_cast<char *>(&ht_size), sizeof(size_t));
      // (3) Write the table itself
      out.write(reinterpret_cast<char *>(&m_term_offsets[0]), sizeof(uint32_t) * ht_size);
      // (4) Write the data
      out.write(reinterpret_cast<char *>(&m_data[0]), m_next_empty * BLOCK_SIZE);
    }

    // Writes to disk but compacts the blocks for each list into a
    // contiguous range
    void serialize_pack(std::ofstream& out) {
      // (1) Write total of "in-use" blocks
      out.write(reinterpret_cast<char *>(&m_next_empty), sizeof(size_t));

      // (2) Write the hash table size
      size_t ht_size = m_term_offsets.size();
      out.write(reinterpret_cast<char *>(&ht_size), sizeof(size_t));

      // (3) Note the hash table start offset; we are going to have to re-write it
      const size_t hash_table_beginning_offset = out.tellp();

      // (4) Write the table itself; this is all done in vain but I am lazy
      out.write(reinterpret_cast<char *>(&m_term_offsets[0]), sizeof(uint32_t) * ht_size);

      // (5) For each hash table entry, follow the chains updating next pointers and
      // writing blocks consecutively; once the end is reached, update the hash
      // table pointer to reflect the new head block offset
      uint32_t next_idx = 0;
      for (size_t i = 0; i < m_term_offsets.size(); ++i) {

        // (1) Check if there is something here
        uint32_t head_block_idx = m_term_offsets[i];
        if (head_block_idx == END_CHAIN) {
          continue;
        }
        // (2) Update the hash table pointer 
        m_term_offsets[i] = next_idx;

        // (3) Get a handle on the tail block
        auto tail_block = m_data[head_block_idx].head.tail_block();

        // (4) Count how many blocks we are going to write
        uint32_t total_blocks_in_chain = 0; // XXX 1 coz we are in the head
        uint32_t block_idx = head_block_idx;
        while (block_idx != tail_block) {
          auto& block = m_data[block_idx];
          block_idx = block.head.next_block();
          total_blocks_in_chain += 1;
        }
        
        // (5) Update the head block tail pointer to reflect the new location
        auto &head_block = m_data[head_block_idx];
        head_block.head.set_tail_block(total_blocks_in_chain + next_idx);

        // (6) We are now clear to walk and write all of the blocks
        block_idx = head_block_idx;
        while (block_idx != tail_block) {
          auto& block = m_data[block_idx];
          uint32_t next_block = block.head.next_block();
          next_idx += 1;
          block.head.set_next_block(next_idx);
          out.write(reinterpret_cast<char *>(&m_data[block_idx]), BLOCK_SIZE);
          block_idx = next_block;
        }
        // Finally, we will write the tail block
        // Note that we need not do any updating on the tail blocks pointers 
        out.write(reinterpret_cast<char *>(&m_data[block_idx]), BLOCK_SIZE);
        next_idx += 1;
      }

      // (6) Now all that hard work is done, we need to go back and re-write the hash table
      out.seekp(hash_table_beginning_offset);
      out.write(reinterpret_cast<char *>(&m_term_offsets[0]), sizeof(uint32_t) * ht_size);
    }

    // Read back into memory
    void load(std::ifstream& in) {
      // (1) Read total of "in-use" blocks
      in.read(reinterpret_cast<char *>(&m_next_empty), sizeof(size_t));
      // (2) Read the hash table size and set it up
      size_t ht_size = 0;
      in.read(reinterpret_cast<char *>(&ht_size), sizeof(size_t));
      m_term_offsets.resize(ht_size);
      // (3) Read the table itself
      in.read(reinterpret_cast<char *>(&m_term_offsets[0]), sizeof(uint32_t) * ht_size);
      // (4) Resize the blocks and then read the data
      m_data.resize(m_next_empty);
      in.read(reinterpret_cast<char *>(&m_data[0]), m_next_empty * BLOCK_SIZE);
    }
    
    // Returns the next slot, or blows up if none are left
    size_t next_free_slot() {
      if (m_next_empty + 1 == m_data.size()) {
          std::cerr << "__ERROR__: Out of slots.\n";
          exit(EXIT_FAILURE);
      }
      return m_next_empty++;
    }

    // Hashes the termid to an offset
    uint32_t termid_to_offset_hash(const uint32_t termid) {
      return termid % m_term_offsets.size();
    }

    // Given an index, return the offset value
    uint32_t get_offset(uint32_t index) {
      return m_term_offsets[index];
    }

    // Hashes a "raw" term string into an entry point
    uint32_t term_to_offset(std::string term) {
      return std::hash<std::string>{}(term) % m_term_offsets.size();
    }

    // Returns the correct entry offset based on stringcompare, or the first
    // empty one. It's up to the caller to check for empty
    uint32_t found_or_empty_offset(std::string term) {
      uint32_t index = term_to_offset(term);
      // Walk the table until we either find the block, or an empty one
      while (m_term_offsets[index] != END_CHAIN) {
        if (term == m_data[m_term_offsets[index]].head.get_term()) {
          return index;
        }
        index = (index+1) % m_term_offsets.size();
      }
      return index; 
    }

    // True if data to read; false if exhausted
    bool has_data(uint32_t block_idx, size_t offset) {
      return ( *(m_data[block_idx].head.struct_ptr() + offset) != '\0'); 
    }

    // Returns a docid/freq pair at a given position
    std::pair<uint32_t, uint32_t> access(uint32_t block_idx, size_t& offset) {
      return decode_magic(m_data[block_idx].head.struct_ptr() + offset, offset);
    }

    // Returns the identifier of the next block
    uint32_t next_block(uint32_t block_idx, const uint32_t tail_idx) const {
      if (block_idx == tail_idx) {
        return END_CHAIN;
      }
      return m_data[block_idx].head.next_block();
    }

    // Returns the tail block index given an index
    // Assumes the index is a head block
    uint32_t tail_block(uint32_t block_idx) const {
      return m_data[block_idx].head.tail_block();
    }

    // Returns the document freq given an index
    // Assumes the index is a head block
    uint32_t doc_freq(uint32_t block_idx) const {
      return m_data[block_idx].head.doc_freq();
    }


    // Returns a head block data offset
    uint64_t head_data_offset(const uint32_t block_idx) {
      return m_data[block_idx].head.data_offset();
    }

    // helper for inserting out of in-memory payload structure
    void insert(const uint32_t docid, const term_position& payload) {
      insert(docid, payload.m_term, payload.m_positions);
    }

    // Insert a posting, a <docid, f_dt> pair
    void insert(const uint32_t docid, const std::string& term, const std::vector<uint32_t>& positions) {

      uint32_t freq = positions.size();

      // Find the entry location in the hash table
      uint32_t entry_hash = found_or_empty_offset(term);
      auto head_block_index = m_term_offsets[entry_hash];

      // If the item is not found, we are working with a new empty head block
      if (head_block_index == END_CHAIN) {
        head_block_index = next_free_slot();
        m_term_offsets[entry_hash] = head_block_index;
        auto& current_block = m_data[head_block_index];
        current_block.head.init(term, head_block_index);
      } 

      // Get a handle on the head block, compute the current gap, increment the ft,
      // and set the recent docid
      auto& head_block = m_data[head_block_index];
      uint32_t doc_gap = docid - head_block.head.recent_docid();
      head_block.head.increment_doc_freq();
      head_block.head.set_recent_docid(docid);

      // Now figure out where to write the new values: we need
      // the block, and the write offset to write new data
      uint32_t current_block_index = head_block.head.tail_block();
      uint8_t  write_offset = head_block.head.tail_byte_offset();

      size_t bytes_required = magic_bytes_required(doc_gap, freq);
   
      // Can the new posting fit?
      if (write_offset + bytes_required <= BLOCK_SIZE) {
          auto& write_block = m_data[current_block_index];
          size_t bytes_written = encode_magic(doc_gap, freq, write_block.tail.struct_ptr() + write_offset);
          head_block.head.advance_tail_byte_offset(bytes_written);
      } else {
          // Grab the next free slot, set it up as a 'tail'
          uint32_t prev_block_index = current_block_index;
          current_block_index = next_free_slot();
          auto& write_block = m_data[current_block_index];
          write_block.tail.init(docid);
          
          // Get a handle on the 'previous' block
          auto& prev_block = m_data[prev_block_index];
          
          // Compute the b-gap to the previous block
          if (prev_block_index == head_block_index) {
              doc_gap = docid; // b-gap in second block is just the docid
          } else {
              doc_gap = docid - prev_block.tail.first_docid();
          }
          
          // Convert the previous block to a 'torso'
          prev_block.torso.set_next_block(current_block_index);

          // Fix up the head pointers
          head_block.head.set_tail_block(current_block_index);
          head_block.head.set_tail_byte_offset(TT_PL_OFFSET);
          write_offset = head_block.head.tail_byte_offset();

          // Write it, assume it will fit now
          size_t bytes_written = encode_magic(doc_gap, freq, write_block.tail.struct_ptr() + write_offset);
          head_block.head.advance_tail_byte_offset(bytes_written); 
      }
    }

    // Insert a positional vector: a <docid, pos<1..n>> pair
    void insert_positions(const uint32_t docid, const term_position& payload) {
      insert_positions(docid, payload.m_term, payload.m_positions);
    }

      // Insert a posting, a <docid, f_dt> pair
    void insert_positions(const uint32_t docid, const std::string& term, const std::vector<uint32_t>& positions) {

      // Find the entry location in the hash table
      uint32_t entry_hash = found_or_empty_offset(term);
      auto head_block_index = m_term_offsets[entry_hash];

      // If the item is not found, we are working with a new empty head block
      if (head_block_index == END_CHAIN) {
        head_block_index = next_free_slot();
        m_term_offsets[entry_hash] = head_block_index;
        auto& current_block = m_data[head_block_index];
        current_block.head.init(term, head_block_index);
      } 

      // Get a handle on the head block, compute the current gap, increment the ft,
      // and set the recent docid
      auto& head_block = m_data[head_block_index];
      uint32_t doc_gap = docid - head_block.head.recent_docid();
      head_block.head.increment_doc_freq();
      // The -1 in next statement is to handle the possibility
      // that the next posting might be to the same doc, musn't
      // let d-gaps (or b-gaps either, nor w-gaps) ever be zero
      head_block.head.set_recent_docid(docid-1);

      uint32_t last_word_pos = 0;
      // Now we'll insert f_d,t positions consecutively
      for (size_t i = 0; i < positions.size(); ++i) {
       
        // Get the current w-gap 
        uint32_t word_gap = positions[i] - last_word_pos;
        last_word_pos = positions[i];
   
        // Now figure out where to write the new values: we need
        // the block, and the write offset to write new data
        uint32_t current_block_index = head_block.head.tail_block();
        uint8_t  write_offset = head_block.head.tail_byte_offset();

        // For positions, encode "backwards" (position first, since pos is smaller usually)
        size_t bytes_required = magic_bytes_required(word_gap, doc_gap);
       
        // Can the new posting fit?
        if (write_offset + bytes_required <= BLOCK_SIZE) {
            auto& write_block = m_data[current_block_index];
            size_t bytes_written = encode_magic(word_gap, doc_gap, write_block.tail.struct_ptr() + write_offset);
            head_block.head.advance_tail_byte_offset(bytes_written);
        } else {
            // Grab the next free slot, set it up as a 'tail'
            uint32_t prev_block_index = current_block_index;
            current_block_index = next_free_slot();
            auto& write_block = m_data[current_block_index];
            // Retain the true first docid associated with this new block
            write_block.tail.init(docid);
            
            // Get a handle on the 'previous' block
            auto& prev_block = m_data[prev_block_index];           
              
            // Compute the b-gap to the previous block
            if (prev_block_index == head_block_index) {
                doc_gap = docid; // b-gap in second block is just the docid
            } else {
                // True docid was stored in the previous tail block, need
                // to accept that this block might have same first docid,
                // and don't want to risk zeros going to encoding routine
                doc_gap = docid - prev_block.tail.first_docid() + 1;
            }
              
            // Convert the previous block to a 'torso'
            prev_block.torso.set_next_block(current_block_index);

            // Fix up the head pointers
            head_block.head.set_tail_block(current_block_index);
            head_block.head.set_tail_byte_offset(TT_PL_OFFSET);
            write_offset = head_block.head.tail_byte_offset();

            // Write it, but as individually encoded variable byte calls. We'll put the b-gap first
            size_t bytes_written = vbyte_encode(doc_gap, write_block.tail.struct_ptr() + write_offset);
            head_block.head.advance_tail_byte_offset(bytes_written);
            bytes_written = vbyte_encode(word_gap, write_block.tail.struct_ptr() + write_offset);
            head_block.head.advance_tail_byte_offset(bytes_written); 
        }
        // If we go round the loop, the next doc_gap will be 1, this next
        // has exactly same effect as
        // doc_gap = docid - head_block.head.recent_docid()
        doc_gap = 1;
      }
    }

    // Report on the index size
    // XXX: Complete the implementation
    void report(size_t total_postings, size_t total_words, size_t vocab_terms, size_t total_docs) {
        size_t MiB = 1024*1024;
        size_t tot_bytes = vocab_terms * 2 * 4 + BLOCK_SIZE * m_next_empty;
        std::string div = "----------------\n";
        std::cerr << div;
        std::cerr << "BLK_SIZE       : " << BLOCK_SIZE << "\n";
        std::cerr << "BLK_HEAD_INIT  : " << HEAD_PL_OFFSET << "\n";
        std::cerr << "BLK_HEAD_PAYL  : " << HEAD_BYTES << "\n";
        std::cerr << "FDT_THRESHOLD  : " << MAGIC_F << "\n";
        std::cerr << div;
        std::cerr << "# total docs   : " << total_docs << "\n";
        std::cerr << "# total words  : " << total_words << "\n";
        std::cerr << "# num postings : " << total_postings << "\n";
        std::cerr << "# unique words : " << vocab_terms << "\n";
        std::cerr << div;
        std::cerr << "# full blocks  : ?\n";
        std::cerr << "# part blocks  : ?\n";
        std::cerr << "# total blocks : " << m_next_empty << "\n";
        std::cerr << div;
        std::cerr << "# hash array   : ?\n";
        std::cerr << "# headers      : ?\n";
        std::cerr << "# d-gaps       : ?\n";
        std::cerr << "# fdt bytes    : ?\n";
        std::cerr << "# waste (full) : ?\n";
        std::cerr << "# waste (part) : ?\n";
        std::cerr << div;
        std::cerr << "# total        : " << tot_bytes << " = " << 1.0*tot_bytes/MiB << " MiB\n";
        std::cerr << div;
        std::cerr << "# overall      : " << 1.0*tot_bytes/total_words << " bytes per input word\n";
        std::cerr << "# overall      : " << 1.0*tot_bytes/total_postings << " bytes per posting\n"; 
        std::cerr << div;
    }

};

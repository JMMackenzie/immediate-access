#include "util.hpp"

#ifdef VARIABLE_BLOCK
#include "variable_immediate_index.hpp"
#include "variable_postings_cursor.hpp"
#else
#include "immediate_index.hpp"
#include "postings_cursor.hpp"
#endif


// CONFIGURE ME!
constexpr bool positions = false;
constexpr bool sort_serialize = true; 
constexpr bool dummy = false;

int main(int argc, const char **argv) {

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " [wsj1|robust|wiki] < /path/to/file\n";
    return EXIT_FAILURE;
  }

  std::cerr << "Positions? " << positions << "\n";
  std::cerr << "Sort before serialize? " << sort_serialize << "\n";
  std::cerr << "Dummy Indexing? " << dummy << "\n";
  std::cerr << "Block Size = " << BLOCK_SIZE << "\n";
  std::cerr << "Magic F = " << MAGIC_F << "\n";


  size_t idx_blocks = 0;
  size_t hash_buckets = 0;

  if (std::string(argv[1]) == "wsj1") {
    idx_blocks = 248602600 / BLOCK_SIZE;
    hash_buckets = 319468;
  } else if (std::string(argv[1]) == "robust") {
    idx_blocks = 1463852840 / BLOCK_SIZE;
    hash_buckets = 1313536;
  } else if (std::string(argv[1]) == "wiki") {
    idx_blocks = 11955330080 / BLOCK_SIZE;
    hash_buckets = 10561650;
  } else {
    std::cerr << "Unknown collection: " << argv[1] << ", cannot guess params...\n";
  }

  std::cerr << "Indexing from stream...\n";
  auto start = get_time_usecs();

  immediate_index my_idx(idx_blocks, hash_buckets);

  // Read the file line-by-line out of stdin
  std::string document;
  std::string _docid;
  std::string term;
  std::unordered_map<std::string, std::vector<uint32_t>> term_to_pos;
  term_to_pos.reserve(1024); // Just a guess; we don't want the table resizing
  uint32_t docid = 1;
  size_t postings_count = 0;
  size_t words_count = 0;
  while (std::getline(std::cin, document)) {
  
    //auto doctime = get_time_usecs();
 
    term_to_pos.clear();
    std::istringstream doc_data(document);
    doc_data >> _docid; // throw away the docid
    uint32_t position = 1; // Index from 1
    while (doc_data >> term) {
      term_to_pos[term].push_back(position);
      position++;
    }
    // We now have the terms and their positions, so we can index
    for (auto & element : term_to_pos) {
      if (dummy) { // Don't index anything, just check the lengths
        size_t vec_size = element.second.size();
        do_not_optimize_away(vec_size);
      } else { // OK, legit indexing here
        if (positions) { 
          my_idx.insert_positions(docid, element.first, element.second);
        } else {
          my_idx.insert(docid, element.first, element.second);
        }
      }
    }
    
    postings_count += term_to_pos.size();
    words_count += position-1;
    docid += 1;

    //std::cout << docid-1 << " " << get_time_usecs() - doctime << "\n";
  }

  auto time_micro = (get_time_usecs() - start);
  std::cerr << "Indexed " << docid-1 << " documents [" << postings_count << " postings] in " << time_micro/1000.0 << " milliseconds...\n";
  std::cerr << "That's about " << time_micro / docid-1 << " micro/doc, or " 
           << time_micro / postings_count << " micro/posting, or "
           << time_micro / words_count << " micro/word\n";

  // Also time the serialization
  if (!dummy) {
    std::ofstream out_idx("/ssd/jmmacke/tmp.idx", std::ios::binary);
    if (sort_serialize) {
      my_idx.serialize_pack(out_idx);
    } else {
      my_idx.serialize(out_idx);
    }
    time_micro = (get_time_usecs() - start);
    std::cerr << "Indexed+Serialized to SSD in " << time_micro/1000.0 << " milliseconds\n";
  }

  std::cerr << "Done.\n";

  return EXIT_SUCCESS;
}


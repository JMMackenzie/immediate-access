#include "util.hpp"

#ifdef VARIABLE_BLOCK
#include "variable_immediate_index.hpp"
#include "variable_postings_cursor.hpp"
#else
#include "immediate_index.hpp"
#include "postings_cursor.hpp"
#endif

int main(int argc, const char **argv) {

  if (argc != 3 && argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <documents> <output_file> [-p]\n"; 
    return EXIT_FAILURE;
  }

  bool positions = false;
  if (argc == 4 && std::string(argv[3]) == "-p") {
    positions = true;
  }

  std::cerr << "Indexing Utility...\n";
  std::cerr << "Data File: " << argv[1] << "\n";
  std::cerr << "Index Positions? " << positions << "\n";

  std::cerr << "Reading the plain collection...\n";
  std::ifstream in_docs(argv[1]);
  std::ofstream out_idx(argv[2], std::ios::binary);
  plain_collection collection = read_full_collection(in_docs);
  std::cerr << "Read " << collection.size() << " documents with a total of "
            << collection.postings() << " postings, " 
            <<  collection.postings() /collection.size() << " postings/doc\n";
  std::cerr << "The vocabulary has " << collection.unique_terms() << " elements\n";


  std::cerr << "Init the instant index...\n";
  // Give it 150% of the estimated size of the raw data
  size_t index_slots = 1.5 * (collection.postings() * AVERAGE_WORD_BYTES) / BLOCK_SIZE;
  std::cerr << "Index Blocks: " << index_slots << "\n";
 
  size_t hash_slots = collection.unique_terms() * HASH_VOCAB_SIZE;
  std::cerr << "Hash Table Size: " << hash_slots << "\n";
 
  immediate_index my_idx(index_slots, hash_slots);
  std::cerr << "Instant Index ready...\n";

  std::cerr << "Adding all documents to the index...\n";
  const bool with_positions = positions;

  // XXX: If testing insert performance, best to avoid the branch in this loop
  auto start = get_time_usecs();
  for (size_t i = 0; i < collection.size(); ++i) {
    auto& doc = collection.m_documents[i];
    // For each term in the doc
    for (size_t j = 0; j < doc.m_terms.size(); ++j) {
      if (with_positions) {
        my_idx.insert_positions(i+1, doc.m_terms[j]);
      } else {
        my_idx.insert(i+1, doc.m_terms[j]);
      }
    }
  }
  auto time_micro = (get_time_usecs() - start);
  std::cerr << "Added " << collection.size() << " documents in " 
            << time_micro/1000 << " milliseconds; " 
            << time_micro / collection.size() << " microseconds/doc.\n";

  my_idx.report(collection.postings(), collection.terms(), collection.unique_terms(), collection.size());
  
  std::cerr << "Serializing index...\n";
  start = get_time_usecs();
  my_idx.serialize(out_idx);
  //my_idx.serialize_pack(out_idx); 
  time_micro = (get_time_usecs() - start);
  std::cerr << "Serialized Index in " << time_micro/1000 << " milliseconds [to SSD or spinning disk?]\n";

  std::cerr << "Done.\n";

  return EXIT_SUCCESS;
}


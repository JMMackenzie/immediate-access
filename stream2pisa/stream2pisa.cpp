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

// Pisa indexes are made up of five files
// Three are binary u32 sequences as follows
// .docs
// [<1 |D|>] [<|l1| <d_1,l1> <d_2,l1> ... <d_3,l1>] ...
// .freqs
// [<|l1| <f_1,l1> <f_2,l1> ...]
// .sizes
// <|D| |doc_1| |doc_2| ...>


struct posting {
  uint32_t docid;
  uint32_t freq;

  posting(uint32_t d, uint32_t f) : docid(d), freq(f) {}

};

struct inverted_index {

  std::map<std::string, std::vector<posting>> index;
  std::vector<uint32_t> doclen;
  std::vector<std::string> docmap;

  void serialize(std::string basename) {

    // Plaintext out
    std::ofstream out_lexicon(basename + ".terms");
    std::ofstream out_docmap(basename + ".documents");
    
    // Binary out
    std::ofstream out_docs(basename + ".docs");
    std::ofstream out_freqs(basename + ".freqs");
    std::ofstream out_sizes(basename + ".sizes");
   
    if (docmap.size() != doclen.size()) {
      std::cerr << "Error: docmap and doclen should have the same size.\n";
      return;
    }

    // Write header
    uint32_t one = 1;
    uint32_t doc_count = docmap.size();
    out_docs.write(reinterpret_cast<char *>(&one), sizeof(uint32_t));
    out_docs.write(reinterpret_cast<char *>(&doc_count), sizeof(uint32_t));
    out_sizes.write(reinterpret_cast<char *>(&doc_count), sizeof(uint32_t));

    for (size_t i = 0; i < docmap.size(); ++i) {
      // Write docmap 
      out_docmap << docmap[i] << "\n";

      uint32_t len = doclen[i];
      // and doclens
      out_sizes.write(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    }

    for (auto it = index.begin(); it != index.end(); ++it) {
  
      // Write lexicon
      out_lexicon << it->first << "\n";

      // Write pl
      auto list = it->second;

      uint32_t count = list.size();
      out_docs.write(reinterpret_cast<char *>(&count), sizeof(uint32_t));
      out_freqs.write(reinterpret_cast<char *>(&count), sizeof(uint32_t));


      for (auto & posting : list) {
        uint32_t docid = posting.docid;
        uint32_t freq = posting.freq;
        out_docs.write(reinterpret_cast<char *>(&docid), sizeof(uint32_t));
        out_freqs.write(reinterpret_cast<char *>(&freq), sizeof(uint32_t));
      }
    }
  }

  void serialize_postings_into_interleaved(std::string basename) {

    std::ofstream out(basename + ".interleaved");
    for (auto it = index.begin(); it != index.end(); ++it) {
      auto list = it->second;
      uint32_t count = list.size();
      bool first = true;
      uint32_t prev_docid = 0;
      for (auto & posting : list) {
        uint32_t dgap = 0;
        uint32_t docid = posting.docid;
        uint32_t freq = posting.freq;
        if (first) {
          dgap = docid;
          first = false;
        } else {
          dgap = docid - prev_docid;
        }
        prev_docid = docid;
        out.write(reinterpret_cast<char *>(&dgap), sizeof(uint32_t));
        out.write(reinterpret_cast<char *>(&freq), sizeof(uint32_t));
      }
    }
  }

};

int main(int argc, const char **argv) {

  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <docstream> <output basename>\n"; 
    return -1;
  }

  inverted_index idx;

  std::ifstream in(argv[1]);
 
  std::string line;
  uint32_t docid = 0;

  // For each doc
  while (std::getline(in, line)) {

    std::string sdocid;
    std::istringstream line_data(line);
    line_data >> sdocid;
    idx.docmap.push_back(sdocid);
    
    std::string term;
    uint32_t len = 0;
    std::map<std::string, uint32_t> local_stats;

    while (line_data >> term) {
      local_stats[term]+=1;
      len++;
    }
    idx.doclen.push_back(len);
   
    // Freq count 
    for (auto & p : local_stats) {
      idx.index[p.first].emplace_back(docid, p.second);
    }
    docid++;
  }

  //idx.serialize(argv[2]);
  idx.serialize_postings_into_interleaved(argv[2]);



}



#include "util.hpp"

#ifdef VARIABLE_BLOCK
#include "variable_immediate_index.hpp"
#include "variable_postings_cursor.hpp"
#else
#include "immediate_index.hpp"
#include "postings_cursor.hpp"
#endif

#include "query.hpp"
#include "query_processing.hpp"

int main(int argc, const char **argv) {

  if (argc != 5 && argc != 6) {
    std::cerr << "Usage: " << argv[0] << " <index> <query_file> <k> <num_docs_in_index> [-v]\n"; 
    return -1;
  }

  bool verbose = false;
  if (argc == 6) { 
    if (std::string(argv[5]) == "-v")
      verbose = true;
    else 
      std::cerr << "Ignoring unknown argument: " << argv[5] << "\n";
  }
 

  std::cerr << "Index File: " << argv[1] << "\n";
  std::cerr << "Query File: " << argv[2] << "\n";
  size_t k = std::atol(argv[3]);
  size_t num_docs = std::atol(argv[4]);
  std::cerr << "k = " << k << "\n";
  std::cerr << "N = " << num_docs << "\n";

  std::cerr << "Reading the index...\n";
  std::ifstream in_idx(argv[1], std::ios::binary);
 
  immediate_index my_idx;
  my_idx.load(in_idx);

  std::cerr << "Reading the query file...\n";
  std::ifstream in_q(argv[2]);
  auto queries = read_queries(in_q);

  std::vector<double> query_times;

  // Ranking structures
  topk_queue heap(k);
  tfidf_ranker ranker(num_docs); 

  // For each query
  for (size_t i = 0; i < queries.size(); ++i) {
   
    // Clear the heap from the last query 
    heap.clear();

    double start = get_time_usecs();
    auto cursors = query_to_cursors(my_idx, queries[i]);
    size_t result_count = ranked_disjunction(cursors, ranker, heap);
    do_not_optimize_away(result_count);
    double stop = get_time_usecs() - start;

    if (result_count > 0) {
        query_times.push_back(stop);

        if (verbose) {
          std::cout << queries[i].m_id << " latency=" << stop << " matches=" << result_count << "\n";
        }
    }
  }

  std::cerr << "Statistics computed over " << query_times.size() << " queries with at least one match.\n";
  
  std::sort(query_times.begin(), query_times.end());
  double average = std::accumulate(query_times.begin(), query_times.end(), double()) / query_times.size();
  double p50 = query_times[query_times.size() / 2];
  double p90 = query_times[90 * query_times.size() / 100];
  double p95 = query_times[95 * query_times.size() / 100];
  double p99 = query_times[99 * query_times.size() / 100];

  std::cerr << "Latency -> Mean: " << average 
            << " Median: " << p50 
            << " p90: " << p90 
            << " p95: " << p95
            << " p99: " << p99 << "\n";

  return 0;
}


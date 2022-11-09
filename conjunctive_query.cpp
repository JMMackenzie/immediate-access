#include "util.hpp"
#include "query.hpp"
#include "query_processing.hpp"

#ifdef VARIABLE_BLOCK
#include "variable_immediate_index.hpp"
#include "variable_postings_cursor.hpp"
#else
#include "immediate_index.hpp"
#include "postings_cursor.hpp"
#endif


int main(int argc, const char **argv) {

  if (argc != 3 && argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <index> <query_file> [-v(v)]\n"; 
    return -1;
  }

  std::cerr << "Index File: " << argv[1] << "\n";
  std::cerr << "Query File: " << argv[2] << "\n";

  bool verbose = false;
  bool very_verbose = false;
  if (argc == 4) { 
    if (std::string(argv[3]) == "-v")
      verbose = true;
    else if (std::string(argv[3]) == "-vv")
      very_verbose = true;
    else 
      std::cerr << "Ignoring unknown argument: " << argv[3] << "\n";
  }
  std::cerr << "Reading the index...\n";
  std::ifstream in_idx(argv[1], std::ios::binary);
 
  immediate_index my_idx;
  my_idx.load(in_idx);

  std::cerr << "Reading the query file...\n";
  std::ifstream in_q(argv[2]);
  auto queries = read_queries(in_q);

  std::vector<double> query_times;
  std::vector<size_t> match_counts;

  for (size_t i = 0; i < queries.size(); ++i) {

    if (very_verbose) {
      auto cursors = query_to_cursors(my_idx, queries[i]);
      //size_t result_count = boolean_conjunction_joel(cursors);
      size_t result_count = profile_boolean_conjunction_alistair(cursors);
      if (result_count > 0) {
        match_counts.push_back(result_count);
      }
      do_not_optimize_away(result_count);
    } else {
      double start = get_time_usecs();
      auto cursors = query_to_cursors(my_idx, queries[i]);
      //size_t result_count = boolean_conjunction_joel(cursors);
      size_t result_count = boolean_conjunction_alistair(cursors);
      do_not_optimize_away(result_count);
      double stop = get_time_usecs() - start;
      // XXX We're only counting queries with matches
      if (result_count > 0) {
        if (verbose) {
          std::cout << queries[i].m_id << " latency=" << stop << " matches=" << result_count << "\n";
        }
        query_times.push_back(stop);
        match_counts.push_back(result_count);
      }
    }
  }

  std::cerr << "Statistics computed over " << match_counts.size() << " queries with at least one match.\n";
  
  if (!verbose && !very_verbose) {
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

  }

  std::sort(match_counts.begin(), match_counts.end());
  double maverage = std::accumulate(match_counts.begin(), match_counts.end(), double()) / match_counts.size();
  size_t mmin = match_counts[0];
  size_t m50 = match_counts[match_counts.size() / 2];
  size_t mmax = match_counts[match_counts.size() - 1];

  
  std::cerr << "Matches -> Mean: " << maverage
            << " min: " << mmin
            << " p50: " << m50
            << " max: " << mmax << "\n";


  return 0;
}


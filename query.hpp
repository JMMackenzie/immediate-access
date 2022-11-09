#pragma once

#include "util.hpp"

// A query is an identifier and a vector of strings
struct query {
  std::string m_id;
  std::vector<std::string> m_terms;

  query(std::string id, std::unordered_set<std::string>& terms) : m_id(id) {
    std::copy(terms.begin(), terms.end(), std::back_inserter(m_terms));
  }

};

// Read queries formatted like <qid> <t1> <t2> ...
std::vector<query> read_queries(std::ifstream &in) {

  size_t tcount = 0;
  std::vector<query> all_queries;
  std::string line;
  
  while (std::getline(in, line)) {
    
    std::istringstream line_data(line);
    std::unordered_set<std::string> terms;
    std::string qid;
    // Eat the first string into the identifier
    line_data >> qid;
    std::string term;
    while (line_data >> term) {
      terms.insert(term);
    }
    tcount += terms.size();
    all_queries.emplace_back(qid, terms);
  }

  std::cerr << "Info: Read " << all_queries.size() 
            << " queries, average length of " << (double) tcount / all_queries.size() << "\n";

  return all_queries;
}

#pragma once

#include "util.hpp"

class tfidf_ranker {

  public:
    explicit tfidf_ranker(const uint32_t num_docs) : m_num_docs(num_docs) {}

    float tf_weight(const uint32_t tf) const {
      return std::log(1 + tf);
    }

    float idf_weight(const uint32_t df) const {
      return std::log(1 + m_num_docs / static_cast<float>(df));
    }

  private:
    uint32_t m_num_docs;

};

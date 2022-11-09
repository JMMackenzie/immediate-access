#pragma once

// A simple top-k min-heap, adapted from PISA
// https://github.com/pisa-engine/pisa/blob/master/include/pisa/topk_queue.hpp

#include <algorithm>

/// Top-k document priority queue.
///
/// Accumulates (document, score) pairs during a retrieval algorithm.
/// This is a min heap; once it is full (contains k elements), any new entry
/// with a score higher than the one on the top of the heap will replace the
/// min element. Because it is a binary heap, the elements are not sorted;
/// use `finalize()` member function to sort it before accessing it with
/// `topk()`.
struct topk_queue {

    // Scores are floats, docids are u32's
    using entry_type = std::pair<float, uint32_t>;

    /// Constructs a top-k priority queue with a threshold set to zero
    explicit topk_queue(size_t k) : m_k(k), m_threshold(0.0f) { 
        m_q.reserve(m_k + 1);
    }
    topk_queue(topk_queue const&) = default;
    topk_queue(topk_queue&&) noexcept = default;
    topk_queue& operator=(topk_queue const&) = default;
    topk_queue& operator=(topk_queue&&) noexcept = default;
    ~topk_queue() = default;

    /// Inserts a heap entry.
    ///
    /// Attempts to inserts an entry with the given score and docid. If the score
    /// is below the threshold, the entry will **not** be inserted, and `false`
    /// will be returned. Otherwise, the entry will be inserted, and `true` returned.
    /// If the heap is full, the entry with the lowest value will be removed, i.e.,
    /// the heap will maintain its size.
    auto insert(float score, uint32_t docid = 0) -> bool
    {
        if (not would_enter(score)) {
            return false;
        }
        m_q.emplace_back(score, docid);
        if (m_q.size() <= m_k) {
            std::push_heap(m_q.begin(), m_q.end(), min_heap_order);
            if (m_q.size() == m_k) {
                m_threshold = m_q.front().first;
            }
        } else {
            std::pop_heap(m_q.begin(), m_q.end(), min_heap_order);
            m_q.pop_back();
            m_threshold = m_q.front().first;
        }
        return true;
    }

    /// Checks if an entry with the given score would be inserted to the queue, according
    /// to the current threshold.
    bool would_enter(float score) const { return score > m_threshold; }

    /// Sorts the results in the heap container in the descending score order.
    ///
    /// After calling this function, the heap should be no longer modified, as
    /// the heap order will not be preserved.
    void finalize()
    {
        std::sort_heap(m_q.begin(), m_q.end(), min_heap_order);
        size_t size = std::lower_bound(
                          m_q.begin(),
                          m_q.end(),
                          0,
                          [](std::pair<float, uint32_t> l, float r) { return l.first > r; })
            - m_q.begin();
        m_q.resize(size);
    }

    /// Returns a reference to the contents of the heap.
    ///
    /// This is intended to be used after calling `finalize()` first, which will sort
    /// the results in order of descending scores.
    [[nodiscard]] std::vector<entry_type> const& topk() const noexcept { return m_q; }

    /// Returns the maximum of `true_threshold()` and `initial_threshold()`.
    [[nodiscard]] auto threshold() const noexcept -> float
    {
        return m_threshold;
    }


    /// Empties the queue and resets the threshold to 0 (or the given value).
    void clear() noexcept
    {
        m_q.clear();
        m_threshold = 0.0f;
    }

    /// Will dump a TREC-like output to stdout
    void dump_to_stream(std::string qid) {
      for (size_t i = 0; i < m_q.size(); ++i) {
        std::cout << qid << " Q0 " << m_q[i].second << " " << i << " " << m_q[i].first << " mm-instant\n";
      }
    }

    /// The maximum number of entries that can fit in the queue.
    [[nodiscard]] auto capacity() const noexcept -> std::size_t { return m_k; }

    /// The current number of entries in the queue.
    [[nodiscard]] auto size() const noexcept -> std::size_t { return m_q.size(); }

  private:
    [[nodiscard]] constexpr static auto
    min_heap_order(entry_type const& lhs, entry_type const& rhs) noexcept -> bool
    {
        return lhs.first > rhs.first;
    }

    size_t m_k;
    float m_threshold;
    std::vector<entry_type> m_q;
};


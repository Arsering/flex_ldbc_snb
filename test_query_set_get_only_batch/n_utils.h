#ifndef APPS_UTILS_H_
#define APPS_UTILS_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

#define BATCH_SIZE 1
  // #define BATCH_SIZE_RATIO 4.5

  inline void get_1d_friends(const ReadTransaction &txn, label_t person_label,
                             label_t knows_labels, vid_t root,
                             std::vector<vid_t> &friends)
  {
    friends.clear();

#if OV
    const auto &oe = txn.GetOutgoingEdges<Date>(person_label, root, person_label,
                                                knows_labels);
    for (auto &e : oe)
    {
      friends.push_back(e.neighbor);
    }
#else
    auto oe = txn.GetOutgoingEdges<Date>(person_label, root, person_label,
                                         knows_labels);
    for (; oe.is_valid(); oe.next())
    {
      auto v = oe.get_neighbor();
      friends.push_back(oe.get_neighbor());
    }
#endif
#if OV
    const auto &ie = txn.GetIncomingEdges<Date>(person_label, root, person_label,
                                                knows_labels);
    for (auto &e : ie)
    {
      friends.push_back(e.neighbor);
    }
#else
    auto ie = txn.GetIncomingEdges<Date>(person_label, root, person_label,
                                         knows_labels);
    for (; ie.is_valid(); ie.next())
    {
      auto v = ie.get_neighbor();
      friends.push_back(ie.get_neighbor());
    }
#endif
  }

  inline void get_2d_friends(const ReadTransaction &txn, label_t person_label,
                             label_t knows_label, vid_t root, vid_t person_num,
                             std::vector<bool> &friends)
  {
    auto person_knows_person_out =
        txn.GetOutgoingGraphView<Date>(person_label, person_label, knows_label);
    auto person_knows_person_in =
        txn.GetIncomingGraphView<Date>(person_label, person_label, knows_label);

    std::vector<uint32_t> neighbors;
#if OV
    const auto &ie = person_knows_person_in.get_edges(root);
    for (auto &e : ie)
    {
      neighbors.push_back(e.neighbor);
    }
    const auto &oe = person_knows_person_out.get_edges(root);
    for (auto &e : oe)
    {
      neighbors.push_back(e.neighbor);
    }
#else

    for (auto ie = person_knows_person_in.get_edges(root); ie.is_valid(); ie.next())
    {
      neighbors.push_back(ie.get_neighbor());
    }

    for (auto oe = person_knows_person_out.get_edges(root); oe.is_valid(); oe.next())
    {

      neighbors.push_back(oe.get_neighbor());
    }
#endif
    friends.clear();
    friends.resize(person_num, false);
    if (neighbors.empty())
    {
      return;
    }
    for (auto v : neighbors)
    {
      friends[v] = true;
#if OV
      const auto &ie2 = person_knows_person_in.get_edges(v);
      for (auto &e : ie2)
      {
        friends[e.neighbor] = true;
      }
      const auto &oe2 = person_knows_person_out.get_edges(v);
      for (auto &e : oe2)
      {
        friends[e.neighbor] = true;
      }
#else
      for (auto ie = person_knows_person_in.get_edges(v); ie.is_valid(); ie.next())
      {
        friends[ie.get_neighbor()] = true;
      }
      for (auto oe = person_knows_person_out.get_edges(v); oe.is_valid(); oe.next())
      {
        friends[oe.get_neighbor()] = true;
      }
#endif
    }
    friends[root] = false;
  }

  template <typename FUNC_T>
  void iterate_1d_2d_neighbors(const ReadTransaction &txn, label_t person_label,
                               label_t knows_label, vid_t root,
                               const FUNC_T &func, vid_t person_num)
  {
    std::vector<bool> bitset(person_num, false);
    bitset[root] = true;

    auto person_knows_person_out =
        txn.GetOutgoingGraphView<Date>(person_label, person_label, knows_label);
    auto person_knows_person_in =
        txn.GetIncomingGraphView<Date>(person_label, person_label, knows_label);
    for (auto ie = person_knows_person_in.get_edges(root); ie.is_valid(); ie.next())
    {
      auto v = ie.get_neighbor();
      for (auto ie2 = person_knows_person_in.get_edges(v); ie2.is_valid(); ie2.next())
      {
        auto u = ie2.get_neighbor();
        if (!bitset[u])
        {
          bitset[u] = true;
          func(u);
        }
      }

      for (auto oe2 = person_knows_person_out.get_edges(v); oe2.is_valid(); oe2.next())
      {
        auto u = oe2.get_neighbor();
        if (!bitset[u])
        {
          bitset[u] = true;
          func(u);
        }
      }
      if (!bitset[v])
      {
        bitset[v] = true;
        func(v);
      }
    }

    for (auto oe = person_knows_person_out.get_edges(root); oe.is_valid(); oe.next())
    {
      auto v = oe.get_neighbor();
      for (auto ie2 = person_knows_person_in.get_edges(v); ie2.is_valid(); ie2.next())
      {
        auto u = ie2.get_neighbor();
        if (!bitset[u])
        {
          bitset[u] = true;
          func(u);
        }
      }
      for (auto oe2 = person_knows_person_out.get_edges(v); oe2.is_valid(); oe2.next())
      {
        auto u = oe2.get_neighbor();
        if (!bitset[u])
        {
          bitset[u] = true;
          func(u);
        }
      }
      if (!bitset[v])
      {
        bitset[v] = true;
        func(v);
      }
    }
  }

  inline void get_1d_2d_neighbors(const ReadTransaction &txn, label_t person_label, label_t knows_label, vid_t root, vid_t person_num, std::vector<vid_t> &neighbors)
  {
    std::vector<bool> bitset(person_num, false);
    bitset[root] = true;

    auto person_knows_person_out =
        txn.GetOutgoingGraphView<Date>(person_label, person_label, knows_label);
    auto person_knows_person_in =
        txn.GetIncomingGraphView<Date>(person_label, person_label, knows_label);
    for (auto ie = person_knows_person_in.get_edges(root); ie.is_valid(); ie.next())
    {
      auto v = ie.get_neighbor();
      if (!bitset[v])
      {
        bitset[v] = true;
        neighbors.push_back(v);
      }
    }
    for (auto oe = person_knows_person_out.get_edges(root); oe.is_valid(); oe.next())
    {
      auto v = oe.get_neighbor();
      if (!bitset[v])
      {
        bitset[v] = true;
        neighbors.push_back(v);
      }
    }
    auto person_knows_person_in_items = txn.BatchGetIncomingEdges<Date>(person_label, person_label, knows_label, neighbors);
    auto person_knows_person_out_items = txn.BatchGetOutgoingEdges<Date>(person_label, person_label, knows_label, neighbors);
    for (int i = 0; i < person_knows_person_in_items.size(); i++)
    {
      for (auto &e = person_knows_person_in_items[i]; e.is_valid(); e.next())
      {
        if (!bitset[e.get_neighbor()])
        {
          bitset[e.get_neighbor()] = true;
          neighbors.push_back(e.get_neighbor());
        }
      }
      for (auto &e = person_knows_person_out_items[i]; e.is_valid(); e.next())
      {
        if (!bitset[e.get_neighbor()])
        {
          bitset[e.get_neighbor()] = true;
          neighbors.push_back(e.get_neighbor());
        }
      }
    }
  }

} // namespace gs

#endif // APPS_UTILS_H_

#ifndef APPS_UTILS_H_
#define APPS_UTILS_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  inline void get_1d_friends(const ReadTransaction &txn, label_t person_label,
                             label_t knows_labels, vid_t root,
                             std::vector<vid_t> &friends)
  {
    friends.clear();

    const auto &oe = txn.GetOutgoingEdges<Date>(person_label, root, person_label,
                                                knows_labels);
    for (auto &e : oe)
    {
      friends.push_back(e.neighbor);
    }

    const auto &ie = txn.GetIncomingEdges<Date>(person_label, root, person_label,
                                                knows_labels);
    for (auto &e : ie)
    {
      friends.push_back(e.neighbor);
    }
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
    friends.clear();
    friends.resize(person_num, false);
    if (neighbors.empty())
    {
      return;
    }
    for (auto v : neighbors)
    {
      friends[v] = true;

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

    const auto &ie = person_knows_person_in.get_edges(root);
    for (auto &e : ie)
    {
      auto v = e.neighbor;
      const auto &ie2 = person_knows_person_in.get_edges(v);
      for (auto &e2 : ie2)
      {
        auto u = e2.neighbor;
        if (!bitset[u])
        {
          bitset[u] = true;
          func(u);
        }
      }
      const auto &oe2 = person_knows_person_out.get_edges(v);
      for (auto &e2 : oe2)
      {
        auto u = e2.neighbor;
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
    const auto &oe = person_knows_person_out.get_edges(root);
    for (auto &e : oe)
    {
      auto v = e.neighbor;
      const auto &ie2 = person_knows_person_in.get_edges(v);
      for (auto &e2 : ie2)
      {
        auto u = e2.neighbor;
        if (!bitset[u])
        {
          bitset[u] = true;
          func(u);
        }
      }
      const auto &oe2 = person_knows_person_out.get_edges(v);
      for (auto &e2 : oe2)
      {
        auto u = e2.neighbor;
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

} // namespace gs

#endif // APPS_UTILS_H_

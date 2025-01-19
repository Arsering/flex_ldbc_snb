#ifndef APPS_UTILS_H_
#define APPS_UTILS_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"

namespace gs
{

  inline void get_1d_friends(const ReadTransaction &txn, label_t person_label,
                             label_t knows_labels, vid_t root,
                             std::vector<vid_t> &friends)
  {
    friends.clear();

    auto oe = txn.GetOutgoingEdges<Date>(person_label, root, person_label,
                                         knows_labels);
    for (; oe.is_valid(); oe.next())
    {
      friends.push_back(oe.get_neighbor());
    }
    auto ie = txn.GetIncomingEdges<Date>(person_label, root, person_label,
                                         knows_labels);
    for (; ie.is_valid(); ie.next())
    {
      friends.push_back(ie.get_neighbor());
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

    for (auto ie = person_knows_person_in.get_edges(root); ie.is_valid(); ie.next())
    {
      neighbors.push_back(ie.get_neighbor());
    }

    for (auto oe = person_knows_person_out.get_edges(root); oe.is_valid(); oe.next())
    {

      neighbors.push_back(oe.get_neighbor());
    }
    friends.clear();
    friends.resize(person_num, false);
    if (neighbors.empty())
    {
      return;
    }

    auto person_knows_person_in_items=txn.BatchGetVidsNeighbors<Date>(person_label, person_label, knows_label, neighbors, false);
    auto person_knows_person_out_items=txn.BatchGetVidsNeighbors<Date>(person_label, person_label, knows_label, neighbors, true);
    for (int i=0;i<person_knows_person_in_items.size();i++){
      auto v=neighbors[i];
      friends[v] = true;
      for (int j=0;j<person_knows_person_in_items[i].size();j++){
        friends[person_knows_person_in_items[i][j]] = true;
      }
      for (int j=0;j<person_knows_person_out_items[i].size();j++){
        friends[person_knows_person_out_items[i][j]] = true;
      }
    }
    friends[root] = false;
  }

  inline void get_1d_2d_neighbors(const ReadTransaction &txn, label_t person_label, label_t knows_label, vid_t root, vid_t person_num, std::vector<vid_t> &neighbors){
    std::vector<bool> bitset(person_num, false);
    bitset[root] = true;

    auto person_knows_person_out =
        txn.GetOutgoingGraphView<Date>(person_label, person_label, knows_label);
    auto person_knows_person_in =
        txn.GetIncomingGraphView<Date>(person_label, person_label, knows_label);
    for(auto ie=person_knows_person_in.get_edges(root);ie.is_valid();ie.next()){
      auto v=ie.get_neighbor();
      if (!bitset[v])
      {
        bitset[v]=true;
        neighbors.push_back(v);
      }
    }
    for(auto oe=person_knows_person_out.get_edges(root);oe.is_valid();oe.next()){
      auto v=oe.get_neighbor();
      if (!bitset[v])
      {
        bitset[v]=true;
        neighbors.push_back(v);
      }
    }
    auto person_knows_person_in_items=txn.BatchGetVidsNeighbors<Date>(person_label, person_label, knows_label, neighbors, false);
    auto person_knows_person_out_items=txn.BatchGetVidsNeighbors<Date>(person_label, person_label, knows_label, neighbors, true);
    for (int i=0;i<person_knows_person_in_items.size();i++){
      for (int j=0;j<person_knows_person_in_items[i].size();j++){
        if (!bitset[person_knows_person_in_items[i][j]])
        {
          bitset[person_knows_person_in_items[i][j]]=true;
          neighbors.push_back(person_knows_person_in_items[i][j]);
        }
      }
      for (int j=0;j<person_knows_person_out_items[i].size();j++){
        if (!bitset[person_knows_person_out_items[i][j]])
        {
          bitset[person_knows_person_out_items[i][j]]=true;
          neighbors.push_back(person_knows_person_out_items[i][j]);
        }
      }
    }
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
    std::vector<vid_t> neighbors;
    for(auto ie=person_knows_person_in.get_edges(root);ie.is_valid();ie.next()){
      auto v=ie.get_neighbor();
      if (!bitset[v])
      {
        bitset[v]=true;
        neighbors.push_back(v);
      }
    }
    for(auto oe=person_knows_person_out.get_edges(root);oe.is_valid();oe.next()){
      auto v=oe.get_neighbor();
      if (!bitset[v])
      {
        bitset[v]=true;
        neighbors.push_back(v);
      }
    }
    auto person_knows_person_in_items=txn.BatchGetVidsNeighbors<Date>(person_label, person_label, knows_label, neighbors, false);
    auto person_knows_person_out_items=txn.BatchGetVidsNeighbors<Date>(person_label, person_label, knows_label, neighbors, true);
    for (int i=0;i<person_knows_person_in_items.size();i++){
      for (int j=0;j<person_knows_person_in_items[i].size();j++){
        if (!bitset[person_knows_person_in_items[i][j]])
        {
          bitset[person_knows_person_in_items[i][j]]=true;
          neighbors.push_back(person_knows_person_in_items[i][j]);
        }
      }
      for (int j=0;j<person_knows_person_out_items[i].size();j++){
        if (!bitset[person_knows_person_out_items[i][j]])
        {
          bitset[person_knows_person_out_items[i][j]]=true;
          neighbors.push_back(person_knows_person_out_items[i][j]);
        }
      }
    }

    for (auto v : neighbors)
    {
      func(v);
    }
  }

} // namespace gs

#endif // APPS_UTILS_H_

/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

ReadTransaction::ReadTransaction(const MutablePropertyFragment& graph,
                                 VersionManager& vm, timestamp_t timestamp)
    : graph_(graph), vm_(vm), timestamp_(timestamp) {}
ReadTransaction::~ReadTransaction() { release(); }

timestamp_t ReadTransaction::timestamp() const { return timestamp_; }

void ReadTransaction::Commit() { release(); }

void ReadTransaction::Abort() { release(); }

ReadTransaction::vertex_iterator::vertex_iterator(
    label_t label, vid_t cur, vid_t num, const MutablePropertyFragment& graph)
    : label_(label), cur_(cur), num_(num), graph_(graph) {}
ReadTransaction::vertex_iterator::~vertex_iterator() = default;

bool ReadTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void ReadTransaction::vertex_iterator::Next() { ++cur_; }
void ReadTransaction::vertex_iterator::Goto(vid_t target) {
  cur_ = std::min(target, num_);
}

oid_t ReadTransaction::vertex_iterator::GetId() const {
  return graph_.get_oid(label_, cur_);
}
vid_t ReadTransaction::vertex_iterator::GetIndex() const { return cur_; }

#if OV
Any ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}
#else
gbp::BufferBlock ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}
#endif

int ReadTransaction::vertex_iterator::FieldNum() const {
  return graph_.get_vertex_table(label_).col_num();
}

ReadTransaction::edge_iterator::edge_iterator(
    label_t neighbor_label, label_t edge_label,
    std::shared_ptr<MutableCsrConstEdgeIterBase> iter)
    : neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      iter_(std::move(iter)) {}
ReadTransaction::edge_iterator::~edge_iterator() = default;

#if OV
Any ReadTransaction::edge_iterator::GetData() const {
  return iter_->get_data();
}
#else
const void* ReadTransaction::edge_iterator::GetData() const {
  return iter_->get_data();
}
#endif

bool ReadTransaction::edge_iterator::IsValid() const {
  return iter_->is_valid();
}

void ReadTransaction::edge_iterator::Next() { iter_->next(); }

vid_t ReadTransaction::edge_iterator::GetNeighbor() const {
  return iter_->get_neighbor();
}

label_t ReadTransaction::edge_iterator::GetNeighborLabel() const {
  return neighbor_label_;
}

label_t ReadTransaction::edge_iterator::GetEdgeLabel() const {
  return edge_label_;
}

ReadTransaction::vertex_iterator ReadTransaction::GetVertexIterator(
    label_t label) const {
  return {label, 0, graph_.vertex_num(label), graph_};
}

ReadTransaction::vertex_iterator ReadTransaction::FindVertex(label_t label,
                                                             oid_t id) const {
  vid_t lid;
  if (graph_.get_lid(label, id, lid)) {
    return {label, lid, graph_.vertex_num(label), graph_};
  } else {
    return {label, graph_.vertex_num(label), graph_.vertex_num(label), graph_};
  }
}

bool ReadTransaction::GetVertexIndex(label_t label, oid_t id,
                                     vid_t& index) const {
  return graph_.get_lid(label, id, index);
}

vid_t ReadTransaction::GetVertexNum(label_t label) const {
  return graph_.vertex_num(label);
}

oid_t ReadTransaction::GetVertexId(label_t label, vid_t index) const {
  return graph_.get_oid(label, index);
}

ReadTransaction::edge_iterator ReadTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_outgoing_edges(label, u, neighnor_label, edge_label)};
}

ReadTransaction::edge_iterator ReadTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_incoming_edges(label, u, neighnor_label, edge_label)};
}

const Schema& ReadTransaction::schema() const { return graph_.schema(); }

void ReadTransaction::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    vm_.release_read_timestamp();
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

std::vector<oid_t> ReadTransaction::BatchGetVertexIds(
    label_t label, const std::vector<vid_t>& indices) const {
  std::vector<oid_t> oids(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    oids[i] = graph_.get_oid(label, indices[i]);
  }
  return oids;
}

std::pair<std::vector<vid_t>, std::vector<bool>>
ReadTransaction::BatchGetVertexIndices(label_t label,
                                       const std::vector<oid_t>& oids) const {
  std::vector<vid_t> indices(oids.size());
  std::vector<bool> exists(oids.size());
  for (size_t i = 0; i < oids.size(); ++i) {
    exists[i] = graph_.get_lid(label, oids[i], indices[i]);
  }
  return {indices, exists};
}

template <typename EDATA_T>
std::vector<AdjListView<EDATA_T>> ReadTransaction::BatchGetOutgoingEdges(
    label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
    label_t edge_label) const {
  std::vector<AdjListView<EDATA_T>> results;
  results.reserve(vids.size());
  for (auto v : vids) {
    results.push_back(
        GetOutgoingEdges<EDATA_T>(v_label, v, neighbor_label, edge_label));
  }
  return results;
}

template <typename EDATA_T>
std::vector<AdjListView<EDATA_T>> ReadTransaction::BatchGetIncomingEdges(
    label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
    label_t edge_label) const {
  std::vector<AdjListView<EDATA_T>> results;
  results.reserve(vids.size());
  for (auto v : vids) {
    results.push_back(
        GetIncomingEdges<EDATA_T>(v_label, v, neighbor_label, edge_label));
  }
  return results;
}

std::vector<std::vector<vid_t>> ReadTransaction::GetOtherVertices(
    const label_t& src_label_id, const label_t& dst_label_id,
    const label_t& edge_label_id, const std::vector<vid_t>& vids,
    const std::string& direction_str) const {

  std::vector<std::vector<vid_t>> ret;
  ret.resize(vids.size());
  if (direction_str == "out" || direction_str == "Out" ||
      direction_str == "OUT") {
    auto csr = graph_.get_oe_csr(src_label_id, dst_label_id, edge_label_id);
    assert(csr != nullptr);
    ret.resize(vids.size());
    for (size_t i = 0; i < vids.size(); ++i) {
      auto v = vids[i];
      auto iter = csr->edge_iter(v);
      auto& vec = ret[i];
      while (iter->is_valid()) {
        vec.push_back(iter->get_neighbor());
        iter->next();
      }
    }
  } else if (direction_str == "in" || direction_str == "In" ||
             direction_str == "IN") {
    auto csr = graph_.get_ie_csr(dst_label_id, src_label_id, edge_label_id);
    assert(csr != nullptr);
    ret.resize(vids.size());
    for (size_t i = 0; i < vids.size(); ++i) {
      auto v = vids[i];
      auto iter = csr->edge_iter(v);
      auto& vec = ret[i];
      while (iter->is_valid()) {
        vec.push_back(iter->get_neighbor());
        iter->next();
      }
    }
  } else if (direction_str == "both" || direction_str == "Both" ||
             direction_str == "BOTH") {
    ret.resize(vids.size());
    auto ocsr = graph_.get_oe_csr(src_label_id, dst_label_id, edge_label_id);
    auto icsr = graph_.get_ie_csr(dst_label_id, src_label_id, edge_label_id);
    assert(ocsr != nullptr);
    assert(icsr != nullptr);
    for (size_t i = 0; i < vids.size(); ++i) {
      auto v = vids[i];
      auto& vec = ret[i];
      auto iter = ocsr->edge_iter(v);
      while (iter->is_valid()) {
        vec.push_back(iter->get_neighbor());
        iter->next();
      }
      iter = icsr->edge_iter(v);
      while (iter->is_valid()) {
        vec.push_back(iter->get_neighbor());
        iter->next();
      }
    }
  } else {
    LOG(FATAL) << "Not implemented - " << direction_str;
  }
  return ret;
}

template std::vector<AdjListView<Date>> ReadTransaction::BatchGetOutgoingEdges<Date>(
    label_t, const std::vector<vid_t>&, label_t, label_t) const;

template std::vector<AdjListView<Date>> ReadTransaction::BatchGetIncomingEdges<Date>(
    label_t, const std::vector<vid_t>&, label_t, label_t) const;

template std::vector<AdjListView<grape::EmptyType>> ReadTransaction::BatchGetIncomingEdges<grape::EmptyType>(
    label_t, const std::vector<vid_t>&, label_t, label_t) const;


}  // namespace gs

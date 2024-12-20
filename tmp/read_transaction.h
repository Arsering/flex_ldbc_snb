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

#ifndef GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_

#include <array>
#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/id_indexer.h"

namespace gs {

class MutablePropertyFragment;
class VersionManager;

#if OV
template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    using nbr_t = MutableNbr<EDATA_T>;

   public:
    nbr_iterator(const nbr_t* ptr, const nbr_t* end, timestamp_t timestamp)
        : ptr_(ptr), end_(end), timestamp_(timestamp) {
      while (ptr_ != end && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
    }

    const nbr_t& operator*() const { return *ptr_; }

    const nbr_t* operator->() const { return ptr_; }

    nbr_iterator& operator++() {
      ++ptr_;
      while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    bool operator==(const nbr_iterator& rhs) const {
      return (ptr_ == rhs.ptr_);
    }

    bool operator!=(const nbr_iterator& rhs) const {
      return (ptr_ != rhs.ptr_);
    }

   private:
    const nbr_t* ptr_;
    const nbr_t* end_;
    timestamp_t timestamp_;
  };

 public:
  using slice_t = MutableNbrSlice<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(slice), timestamp_(timestamp) {}

  nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_);
  }
  nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_);
  }

  int estimated_degree() const { return edges_.size(); }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
};
#else
template <typename EDATA_T>
class AdjListView {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  using sliceiter_t = TypedMutableCsrConstEdgeIter<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(sliceiter_t(slice)), timestamp_(timestamp) {
    while (edges_.is_valid() && edges_.get_timestamp() > timestamp_) {
      edges_.next();
    }
  }

  FORCE_INLINE vid_t get_neighbor() { return edges_.get_neighbor(); }

  FORCE_INLINE const void* get_data() { return edges_.get_data(); }

  FORCE_INLINE timestamp_t get_timestamp() { return edges_.get_timestamp(); }

  FORCE_INLINE void next() {
    edges_.next();
    while (edges_.is_valid() && edges_.get_timestamp() > timestamp_) {
      edges_.next();
    }
  }

  FORCE_INLINE bool is_valid() {
    return edges_.is_valid() && edges_.get_timestamp() <= timestamp_;
  }

  int estimated_degree() const { return edges_.size(); }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  sliceiter_t edges_;
  timestamp_t timestamp_;
};
#endif
template <typename EDATA_T>
class GraphView {
 public:
  GraphView(const MutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}

  AdjListView<EDATA_T> get_edges(vid_t v) const {
    return AdjListView<EDATA_T>(csr_.get_edges(v), timestamp_);
  }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  const MutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class SingleGraphView {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  SingleGraphView(const SingleMutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}
#if OV
  bool exist(vid_t v) const {
    return (csr_.get_edge(v).timestamp.load() <= timestamp_);
  }

  const MutableNbr<EDATA_T>& get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }
#else
  FORCE_INLINE bool exist(vid_t v) const {
    auto item = csr_.get_edge(v);
    return (gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_);
  }

  FORCE_INLINE const gbp::BufferBlock exist(vid_t v, bool& exist) const {
    auto item = csr_.get_edge(v);
    exist = gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
    return item;
  }
  FORCE_INLINE bool exist1(gbp::BufferBlock& item) const {
    return gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
  }
  FORCE_INLINE const gbp::BufferBlock get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

  FORCE_INLINE timestamp_t timestamp() const { return timestamp_; }
#endif
 private:
  const SingleMutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

class ReadTransaction {
 public:
  ReadTransaction(const MutablePropertyFragment& graph, VersionManager& vm,
                  timestamp_t timestamp);
  ~ReadTransaction();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t num,
                    const MutablePropertyFragment& graph);
    ~vertex_iterator();

    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    oid_t GetId() const;
    vid_t GetIndex() const;
#if OV
    Any GetField(int col_id) const;
#else
    gbp::BufferBlock GetField(int col_id) const;
#endif
    int FieldNum() const;

   private:
    label_t label_;
    vid_t cur_;
    vid_t num_;
    const MutablePropertyFragment& graph_;
  };

  class edge_iterator {
   public:
    edge_iterator(label_t neighbor_label, label_t edge_label,
                  std::shared_ptr<MutableCsrConstEdgeIterBase> iter);
    ~edge_iterator();
#if OV
    Any GetData() const;
#else
    const void* GetData() const;
#endif
    bool IsValid() const;

    void Next();

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    label_t neighbor_label_;
    label_t edge_label_;

    std::shared_ptr<MutableCsrConstEdgeIterBase> iter_;
  };

  vertex_iterator GetVertexIterator(label_t label) const;

  vertex_iterator FindVertex(label_t label, oid_t id) const;

  bool GetVertexIndex(label_t label, oid_t id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  oid_t GetVertexId(label_t label, vid_t index) const;

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighnor_label,
                                   label_t edge_label) const;

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighnor_label,
                                  label_t edge_label) const;

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetOutgoingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));

    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetIncomingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  const Schema& schema() const;

  template <typename EDATA_T>
  GraphView<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetOutgoingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetIncomingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  /*
  批量获取顶点ID
  @param label: 顶点标签
  @param indices: 顶点索引组成的vector
  @return: 顶点ID组成的vector
  */
  std::vector<oid_t> BatchGetVertexIds(label_t label,
                                       const std::vector<vid_t>& indices) const;

  /*
  批量查找顶点索引
  @param label: 顶点标签
  @param oids: 顶点ID组成的vector
  @return: 顶点索引组成的vector和是否存在的vector
  */
  std::pair<std::vector<vid_t>, std::vector<bool>> BatchGetVertexIndices(
      label_t label, const std::vector<oid_t>& oids) const;

  /*
  批量获取顶点属性
  @param label: 顶点标签
  @param vids: 顶点ID组成的vector
  @param col_ids: 属性ID组成的vector
  @return: 顶点属性组成的vector
  */
  template <typename... T>
  std::vector<std::tuple<T...>> BatchGetVertexPropsFromVid(
      label_t label, const std::vector<vid_t>& vids,
      const std::array<int, sizeof...(T)>& col_ids) const;

  /*
  批量获取顶点属性
  @param label_id: 顶点标签
  @param vids: 顶点ID组成的vector
  @param prop_names: 属性名称组成的vector
  @return: 顶点属性组成的vector
  */
  template <typename... T>
  std::vector<std::tuple<T...>> BatchGetVertexPropsFromVid(
      label_t label_id, const std::vector<vid_t>& vids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>& prop_names) const;


  /* 批量获取出边
  @param v_label: 顶点标签             
  @param vids: 顶点ID组成的vector
  @param neighbor_label: 邻居标签
  @param edge_label: 边标签
  @return: 出边列表
  */
  template <typename EDATA_T>
  std::vector<AdjListView<EDATA_T>> BatchGetOutgoingEdges(
      label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
      label_t edge_label) const;

  /* 批量获取入边
  @param v_label: 顶点标签
  @param vids: 顶点ID组成的vector
  @param neighbor_label: 邻居标签
  @param edge_label: 边标签
  @return: 入边列表
  */
  template <typename EDATA_T>
  std::vector<AdjListView<EDATA_T>> BatchGetIncomingEdges(
      label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
      label_t edge_label) const;

  /* 获取边指向的邻居
  @param src_label_id: 源顶点标签
  @param dst_label_id: 目标顶点标签
  @param edge_label_id: 边标签
  @param vids: 顶点ID组成的vector
  @param direction_str: 方向
  @return: 邻居列表
  */
  std::vector<std::vector<vid_t>> GetOtherVertices(
      const label_t& src_label_id, const label_t& dst_label_id,
      const label_t& edge_label_id, const std::vector<vid_t>& vids,
      const std::string& direction_str) const;


  template <typename FUNC_T, typename... SELECTOR>
  void ScanVertices(const std::string& label,
                    const std::tuple<SELECTOR...>& props,
                    const FUNC_T& func) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    auto vnum = graph_.vertex_num(label_id);

    // 获取属性列
    auto columns = get_tuple_column_from_graph_with_property(label_id, props);
    std::tuple<typename SELECTOR::prop_t...> t;

    // 遍历所有顶点
    if constexpr (sizeof...(SELECTOR) == 0) {
      // 无属性的情况
      for (auto v = 0; v != vnum; ++v) {
        func(v, t);
      }
    } else {
      // 有属性的情况
      for (auto v = 0; v != vnum; ++v) {
        get_tuple_from_column_tuple(v, t, columns);
        func(v, t);
      }
    }
  }

 private:
  void release();

  const MutablePropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;

  // 辅助函数:获取单个顶点的多个属性
  // template <typename... T, size_t... I>
  // std::tuple<T...> GetVertexFields(
  //     label_t label, vid_t vid,
  //     const std::array<int, sizeof...(T)>& col_ids,
  //     std::index_sequence<I...>) const {
  //   return std::make_tuple(
  //       (graph_.get_vertex_table(label)
  //            .get_column_by_id(col_ids[I])
  //            ->get_view(vid))...);
  // }

  template <typename T>
  struct PropertySelector {
    using prop_t = T;
    std::string prop_name_;
    PropertySelector(std::string prop_name)
        : prop_name_(std::move(prop_name)) {}
    PropertySelector() = default;
  };

  // std::shared_ptr<RefColumnBase> create_ref_column(
  //     std::shared_ptr<ColumnBase> column) const {
  //   auto type = column->type();
  //   if (type == PropertyType::kInt32) {
  //     return std::make_shared<TypedRefColumn<int>>(
  //         *std::dynamic_pointer_cast<TypedColumn<int>>(column));
  //   } else if (type == PropertyType::kInt64) {
  //     return std::make_shared<TypedRefColumn<int64_t>>(
  //         *std::dynamic_pointer_cast<TypedColumn<int64_t>>(column));
  //   } else if (type == PropertyType::kDate) {
  //     return std::make_shared<TypedRefColumn<Date>>(
  //         *std::dynamic_pointer_cast<TypedColumn<Date>>(column));
  //   } else if (type == PropertyType::kString) {
  //     auto string_column = std::dynamic_pointer_cast<StringColumn>(column);
  //     if (!string_column) {
  //       LOG(ERROR) << "Failed to cast column to TypedColumn<string_view>";
  //       return nullptr;
  //     }
  //     return std::make_shared<TypedRefColumn<std::string_view>>(string_column);
  //   } else {
  //     LOG(FATAL) << "unexpected type to create column, "
  //                << static_cast<int>(type);
  //     return nullptr;
  //   }
  // }

  // // get the vertex property
  // template <typename T>
  // std::shared_ptr<TypedRefColumn<T>> GetTypedRefColumn(
  //     label_t label, const std::string& prop_name) const {
  //   using column_t = std::shared_ptr<TypedRefColumn<T>>;
  //   column_t column;
  //   if (prop_name == "id" || prop_name == "ID" ||
  //       prop_name == "Id") {  // FIXME: not support to message id allocator
  //     return nullptr;  // ID 字段暂时不支持
  //   } else {
  //     auto ptr = graph_.get_vertex_table(label).get_column(prop_name);
  //     if (!ptr) {
  //       LOG(ERROR) << "Column " << prop_name << " not found for label " << label;
  //       return nullptr;
  //     }
  //     auto ref_column = create_ref_column(ptr);
  //     if (!ref_column) {
  //       LOG(ERROR) << "Failed to create ref column for " << prop_name;
  //       return nullptr;
  //     }
  //     column = std::dynamic_pointer_cast<TypedRefColumn<T>>(ref_column);
  //     if (!column) {
  //       LOG(ERROR) << "Failed to cast column " << prop_name << " to type " << typeid(T).name();
  //       return nullptr;
  //     }
  //   }
  //   return column;
  // }

  template <size_t Is = 0, typename... T, typename column_tuple_t>
  void fetch_properties_in_column(const std::vector<vid_t>& vids,
                                  std::vector<std::tuple<T...>>& props,
                                  column_tuple_t& column) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    auto& cur_column = std::get<Is>(column);
    if (cur_column) {
      for (auto i = 0; i < vids.size(); ++i) {
        std::get<Is>(props[i]) = cur_column->get_view(vids[i]);
      }
    }

    if constexpr (Is + 1 < sizeof...(T)) {
      fetch_properties_in_column<Is + 1>(vids, props, column);
    }
  }

  // template <typename PropT>
  // auto get_single_column_from_graph_with_property(
  //     label_t label, const PropertySelector<PropT>& selector) const {
  //   auto res = GetTypedRefColumn<PropT>(label, selector.prop_name_);
  //   CHECK(res) << "Property " << selector.prop_name_ << " not found";
  //   return res;
  // }

  // template <typename... SELECTOR>
  // inline auto get_tuple_column_from_graph_with_property(
  //     label_t label, const std::tuple<SELECTOR...>& selectors) {
  //   return get_tuple_column_from_graph_with_property_impl(
  //       label, selectors, std::make_index_sequence<sizeof...(SELECTOR)>());
  // }

  // template <typename... SELECTOR, size_t... Is>
  // auto get_tuple_column_from_graph_with_property_impl(
  //     label_t label, const std::tuple<SELECTOR...>& selectors,
  //     std::index_sequence<Is...>) {
  //   return std::make_tuple(get_single_column_from_graph_with_property(
  //       label, std::get<Is>(selectors))...);
  // }

  // template <size_t I = 0, typename... T>
  // void get_tuple_from_column_tuple(
  //     size_t index, std::tuple<T...>& t,
  //     const std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) {
  //   auto ptr = std::get<I>(columns);
  //   if (ptr) {
  //     std::get<I>(t) = ptr->get_view(index);
  //   }

  //   if constexpr (I + 1 < sizeof...(T)) {
  //     get_tuple_from_column_tuple<I + 1>(index, t, columns);
  //   }
  // }

  // template <typename... T>
  // std::tuple<T...> GetVertexFields(
  //     label_t label, vid_t vid,
  //     const std::array<int, sizeof...(T)>& col_ids) const {
  //   return GetVertexFields<T...>(label, vid, col_ids,
  //                                std::make_index_sequence<sizeof...(T)>());
  // }

  // template <size_t I = 0, typename... T,
  //           typename std::enable_if<(sizeof...(T) > 0)>::type* = nullptr>
  // void get_tuple_column_from_graph(
  //     label_t label,
  //     const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
  //         prop_names,
  //     std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) const {
  //   using PT = std::tuple_element_t<I, std::tuple<T...>>;
  //   std::get<I>(columns) = std::dynamic_pointer_cast<TypedRefColumn<PT>>(
  //       GetTypedRefColumn<PT>(label, prop_names[I]));
  //   if constexpr (I + 1 < sizeof...(T)) {
  //     get_tuple_column_from_graph<I + 1>(label, prop_names, columns);
  //   }
  // }

  // template <size_t I = 0, typename... T,
  //           typename std::enable_if<(sizeof...(T) == 0)>::type* = nullptr>
  // void get_tuple_column_from_graph(
  //     label_t label,
  //     const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
  //         prop_names,
  //     std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) const {}

};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_

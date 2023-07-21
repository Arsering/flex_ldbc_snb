#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "utils.h"

namespace gs {

class IC6 : public AppBase {
 public:
  IC6(GraphDBSession& graph)
      : tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
        person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        post_label_id_(graph.schema().get_vertex_label_id("POST")),
        knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
        hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
        hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
        tag_num_(graph.graph().vertex_num(tag_label_id_)),
        tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(tag_label_id_, "name")))),
        graph_(graph) {}

  ~IC6() {}

  struct tag_info {
    tag_info(int count_, const std::string_view& tag_name_)
        : count(count_), tag_name(tag_name_) {}
    int count;
    std::string_view tag_name;
  };

  struct tag_info_comparer {
    bool operator()(const tag_info& lhs, const tag_info& rhs) {
      if (lhs.count > rhs.count) {
        return true;
      }
      if (lhs.count < rhs.count) {
        return false;
      }
      return lhs.tag_name < rhs.tag_name;
    }
  };

  bool Query(Decoder& input, Encoder& output) override {
    auto txn = graph_.GetReadTransaction();

    oid_t personid = input.get_long();
    std::string_view tagname = input.get_string();
    CHECK(input.empty());

    vid_t root{};
    if (!txn.GetVertexIndex(person_label_id_, personid, root)) {
      return false;
    }
    vid_t tag_id = tag_num_;
    for (vid_t i = 0; i < tag_num_; ++i) {
      if (tag_name_col_.get_view(i) == tagname) {
        tag_id = i;
        break;
      }
    }
    assert(tag_id != tag_num_);

    get_2d_friends(txn, person_label_id_, knows_label_id_, root,
                   txn.GetVertexNum(person_label_id_), friends_);

    std::vector<int> post_count(tag_num_, 0);

    const auto& posts_with_tag = txn.GetIncomingEdges<grape::EmptyType>(
        tag_label_id_, tag_id, post_label_id_, hasTag_label_id_);
    auto post_hasCreator_person_out =
        txn.GetOutgoingSingleGraphView<grape::EmptyType>(
            post_label_id_, person_label_id_, hasCreator_label_id_);
    auto post_hasTag_tag_out = txn.GetOutgoingGraphView<grape::EmptyType>(
        post_label_id_, tag_label_id_, hasTag_label_id_);
    for (auto& e : posts_with_tag) {
      vid_t post_id = e.neighbor;
      assert(post_hasCreator_person_out.exist(post_id));
      auto creator = post_hasCreator_person_out.get_edge(post_id).neighbor;
      if (friends_[creator]) {
        const auto& oe = post_hasTag_tag_out.get_edges(post_id);
        for (auto& e2 : oe) {
          ++post_count[e2.neighbor];
        }
      }
    }

    post_count[tag_id] = 0;

    tag_info_comparer comparer;
    std::priority_queue<tag_info, std::vector<tag_info>, tag_info_comparer> que(
        comparer);
    vid_t other_tag_id = 0;
    while (que.size() < 10 && other_tag_id < tag_num_) {
      if (post_count[other_tag_id] > 0) {
        que.emplace(post_count[other_tag_id],
                    tag_name_col_.get_view(other_tag_id));
      }
      ++other_tag_id;
    }
    while (other_tag_id < tag_num_) {
      const auto& top = que.top();
      auto cur_count = post_count[other_tag_id];
      if (cur_count > top.count) {
        que.pop();
        que.emplace(cur_count, tag_name_col_.get_view(other_tag_id));
      } else if (cur_count == top.count) {
        std::string_view tag_name = tag_name_col_.get_view(other_tag_id);
        if (tag_name < top.tag_name) {
          que.pop();
          que.emplace(cur_count, tag_name);
        }
      }
      ++other_tag_id;
    }
    std::vector<tag_info> vec;
    vec.reserve(que.size());
    while (!que.empty()) {
      vec.emplace_back(que.top());
      que.pop();
    }
    for (size_t i = vec.size(); i > 0; i--) {
      output.put_string_view(vec[i - 1].tag_name);
      output.put_int(vec[i - 1].count);
    }
    return true;
  }

 private:
  label_t tag_label_id_;
  label_t person_label_id_;
  label_t post_label_id_;

  label_t knows_label_id_;
  label_t hasCreator_label_id_;
  label_t hasTag_label_id_;

  std::vector<bool> friends_;

  vid_t tag_num_;
  const StringColumn& tag_name_col_;

  GraphDBSession& graph_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::IC6* app = new gs::IC6(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::IC6* casted = static_cast<gs::IC6*>(app);
  delete casted;
}
}

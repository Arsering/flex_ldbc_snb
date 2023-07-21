#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "utils.h"

namespace gs {

class IC10 : public AppBase {
 public:
  IC10(GraphDBSession& graph)
      : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
        place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
        knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
        tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
        post_label_id_(graph.schema().get_vertex_label_id("POST")),
        hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
        hasInterest_label_id_(graph.schema().get_edge_label_id("HASINTEREST")),
        hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
        person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "firstName")))),
        person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "lastName")))),
        person_birthday_col_(*(std::dynamic_pointer_cast<DateColumn>(
            graph.get_vertex_property_column(person_label_id_, "birthday")))),
        person_gender_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "gender")))),

        place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(place_label_id_, "name")))),
        graph_(graph) {}
  ~IC10() {}
  void get_friends(const ReadTransaction& txn, vid_t root, int mon) {
    std::vector<vid_t> friends_1d;
    const auto& oe = txn.GetOutgoingEdges<Date>(
        person_label_id_, root, person_label_id_, knows_label_id_);
    friends_set_[root] = true;
    for (auto& e : oe) {
      friends_1d.emplace_back(e.neighbor);
      friends_set_[e.neighbor] = true;
    }
    const auto& ie = txn.GetIncomingEdges<Date>(
        person_label_id_, root, person_label_id_, knows_label_id_);
    for (auto& e : ie) {
      friends_1d.emplace_back(e.neighbor);
      friends_set_[e.neighbor] = true;
    }
    for (auto v : friends_1d) {
      const auto& oe1 = txn.GetOutgoingEdges<Date>(
          person_label_id_, v, person_label_id_, knows_label_id_);
      for (auto& e : oe1) {
        auto u = e.neighbor;
        if (!friends_set_[u]) {
          friends_set_[u] = true;
          auto t = person_birthday_col_.get_view(u).milli_second / 1000;
          auto tm = gmtime((time_t*) (&t));
          if ((tm->tm_mon + 1 == mon && tm->tm_mday >= 21) ||
              ((mon <= 11 && tm->tm_mon == mon && tm->tm_mday < 22) ||
               (tm->tm_mon == 0 && mon == 12 && tm->tm_mday < 22))) {
            friends_list_.emplace_back(u);
          }
        }
      }
      const auto& ie1 = txn.GetIncomingEdges<Date>(
          person_label_id_, v, person_label_id_, knows_label_id_);
      for (auto& e : ie1) {
        auto u = e.neighbor;
        if (!friends_set_[u]) {
          friends_set_[u] = true;
          auto t = person_birthday_col_.get_view(u).milli_second / 1000;
          auto tm = gmtime((time_t*) (&t));
          if ((tm->tm_mon + 1 == mon && tm->tm_mday >= 21) ||
              ((mon <= 11 && tm->tm_mon == mon && tm->tm_mday < 22) ||
               (tm->tm_mon == 0 && mon == 12 && tm->tm_mday < 22))) {
            friends_list_.emplace_back(u);
          }
        }
      }
    }
  }

  struct person_info {
    person_info(int score_, vid_t person_vid_, oid_t person_id_)
        : score(score_), person_vid(person_vid_), person_id(person_id_) {}

    int score;
    vid_t person_vid;
    oid_t person_id;
  };

  struct person_info_comparer {
    bool operator()(const person_info& lhs, const person_info& rhs) {
      if (lhs.score > rhs.score) {
        return true;
      }
      if (lhs.score < rhs.score) {
        return false;
      }
      return lhs.person_id < rhs.person_id;
    }
  };

  bool Query(Decoder& input, Encoder& output) override {
    auto txn = graph_.GetReadTransaction();

    oid_t personid = input.get_long();
    int month = input.get_int();
    CHECK(input.empty());

    vid_t root{};
    if (!txn.GetVertexIndex(person_label_id_, personid, root)) {
      return false;
    }
    friends_list_.clear();
    friends_set_.clear();
    friends_set_.resize(txn.GetVertexNum(person_label_id_), false);
    get_friends(txn, root, month);
    const auto& oe = txn.GetOutgoingEdges<grape::EmptyType>(
        person_label_id_, root, tag_label_id_, hasInterest_label_id_);
    hasInterests_.clear();
    hasInterests_.resize(txn.GetVertexNum(tag_label_id_));
    for (auto& e : oe) {
      hasInterests_[e.neighbor] = true;
    }

    person_info_comparer comparer;
    std::priority_queue<person_info, std::vector<person_info>,
                        person_info_comparer>
        pq(comparer);

    auto post_hasCreator_person_in = txn.GetIncomingGraphView<grape::EmptyType>(
        person_label_id_, post_label_id_, hasCreator_label_id_);
    auto post_hasTag_tag_out = txn.GetOutgoingGraphView<grape::EmptyType>(
        post_label_id_, tag_label_id_, hasTag_label_id_);

    for (auto v : friends_list_) {
      int score = 0;
      const auto& post_ie = post_hasCreator_person_in.get_edges(v);
      for (auto& e : post_ie) {
        auto post = e.neighbor;
        const auto& tag_oe = post_hasTag_tag_out.get_edges(post);
        int diff = -1;
        for (auto& e1 : tag_oe) {
          if (hasInterests_[e1.neighbor]) {
            diff = 1;
            break;
          }
        }
        score += diff;
      }
      if (pq.size() < 10) {
        pq.emplace(score, v, txn.GetVertexId(person_label_id_, v));
      } else {
        const auto& top = pq.top();
        if (score > top.score) {
          pq.pop();
          pq.emplace(score, v, txn.GetVertexId(person_label_id_, v));
        } else if (score == top.score) {
          oid_t person_id = txn.GetVertexId(person_label_id_, v);
          if (person_id < top.person_id) {
            pq.pop();
            pq.emplace(score, v, person_id);
          }
        }
      }
    }
    std::vector<person_info> vec;
    vec.reserve(pq.size());
    while (!pq.empty()) {
      vec.emplace_back(pq.top());
      pq.pop();
    }
    auto person_isLocatedIn_place_out =
        txn.GetOutgoingSingleGraphView<grape::EmptyType>(
            person_label_id_, place_label_id_, isLocatedIn_label_id_);
    for (auto i = vec.size(); i > 0; i--) {
      auto& v = vec[i - 1];
      output.put_long(v.person_id);
      output.put_string_view(person_firstName_col_.get_view(v.person_vid));
      output.put_string_view(person_lastName_col_.get_view(v.person_vid));
      output.put_int(v.score);
      output.put_string_view(person_gender_col_.get_view(v.person_vid));
      assert(person_isLocatedIn_place_out.exist(v.person_vid));
      auto person_place =
          person_isLocatedIn_place_out.get_edge(v.person_vid).neighbor;
      output.put_string_view(place_name_col_.get_view(person_place));
    }
    return true;
  }

 private:
  label_t person_label_id_;
  label_t isLocatedIn_label_id_;
  label_t place_label_id_;
  label_t knows_label_id_;
  label_t tag_label_id_;
  label_t post_label_id_;
  label_t hasTag_label_id_;
  label_t hasInterest_label_id_;
  label_t hasCreator_label_id_;

  const StringColumn& person_firstName_col_;
  const StringColumn& person_lastName_col_;
  const DateColumn& person_birthday_col_;
  const StringColumn& person_gender_col_;
  const StringColumn& place_name_col_;

  std::vector<vid_t> friends_list_;
  std::vector<bool> friends_set_;

  std::vector<bool> hasInterests_;

  GraphDBSession& graph_;
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::IC10* app = new gs::IC10(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::IC10* casted = static_cast<gs::IC10*>(app);
  delete casted;
}
}

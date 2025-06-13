#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class INS5 : public AppBase {
 public:
  INS5(GraphDBSession& graph)
      : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
        hasMember_label_id_(graph.schema().get_edge_label_id("HASMEMBER")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) override {
    oid_t forumid = input.get_long();
    oid_t personid = input.get_long();
    int64_t joindate = input.get_long();
    CHECK(input.empty());

    auto txn = graph_.GetSingleEdgeInsertTransaction();
    if (!txn.AddEdge(forum_label_id_, forumid, person_label_id_, personid,
                     hasMember_label_id_, Any::From(Date(joindate)))) {
      LOG(ERROR) << "Adding edge FORUM[" << forumid << "] hasMember(" << joindate << ") PERSON[" << personid << "] failed...";
      txn.Abort();
      return false;
    }
    txn.Commit();
    return true;
  }

 private:
  label_t person_label_id_;
  label_t forum_label_id_;
  label_t hasMember_label_id_;

  GraphDBSession& graph_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::INS5* app = new gs::INS5(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::INS5* casted = static_cast<gs::INS5*>(app);
  delete casted;
}
}

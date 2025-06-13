#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class INS8 : public AppBase {
 public:
  INS8(GraphDBSession& graph)
      : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) override {
    oid_t person1id = input.get_long();
    oid_t person2id = input.get_long();
    int64_t creationdate = input.get_long();
    CHECK(input.empty());
    auto txn = graph_.GetSingleEdgeInsertTransaction();
    if (!txn.AddEdge(person_label_id_, person1id, person_label_id_, person2id,
                     knows_label_id_, Any::From(Date(creationdate)))) {
      LOG(ERROR) << "Adding edge PERSON[" << person1id << "] knows(" << creationdate << ") PERSON[" << person2id << "] failed...";
      txn.Abort();
      return false;
    }
    txn.Commit();
    return true;
  }

 private:
  label_t person_label_id_;
  label_t knows_label_id_;

  GraphDBSession& graph_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::INS8* app = new gs::INS8(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::INS8* casted = static_cast<gs::INS8*>(app);
  delete casted;
}
}

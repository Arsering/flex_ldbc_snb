#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class INS3 : public AppBase {
 public:
  INS3(GraphDBSession& graph)
      : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
        likes_label_id_(graph.schema().get_edge_label_id("LIKES")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) override {
    oid_t personid = input.get_long();
    oid_t commentid = input.get_long();
    int64_t creationdate = input.get_long();
    CHECK(input.empty());

    auto txn = graph_.GetSingleEdgeInsertTransaction();
    if (!txn.AddEdge(person_label_id_, personid, comment_label_id_, commentid,
                     likes_label_id_, Any::From(Date(creationdate)))) {
      LOG(INFO) << "Adding edge PERSON[" << personid << "] likes(" << creationdate << ") COMMENT[" << commentid << "] failed...";
      txn.Abort();
      return false;
    }
    txn.Commit();
    return true;
  }

 private:
  label_t person_label_id_;
  label_t comment_label_id_;
  label_t likes_label_id_;

  GraphDBSession& graph_;
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::INS3* app = new gs::INS3(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::INS3* casted = static_cast<gs::INS3*>(app);
  delete casted;
}
}

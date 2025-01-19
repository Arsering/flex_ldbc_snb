#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class INS4 : public AppBase {
 public:
  INS4(GraphDBSession& graph)
      : forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
        person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        hasModerator_label_id_(
            graph.schema().get_edge_label_id("HASMODERATOR")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) override {
    oid_t forumid = input.get_long();
    // std::cout<<"insert forum and forum id is "<<forumid<<std::endl;
    std::string_view forumtitle = input.get_string();
    int64_t creationdate = input.get_long();
    oid_t moderatorpersonid = input.get_long();
    CHECK(input.empty());

    auto txn = graph_.GetSingleVertexInsertTransaction();
    if (!txn.AddVertex(
            forum_label_id_, forumid,
            {Any::From(forumtitle), Any::From(Date(creationdate))})) {
      LOG(ERROR) << "Adding vertex FORUM[" << forumid << "] failed...";
      txn.Abort();
      return false;
    }
    if (!txn.AddEdge(forum_label_id_, forumid, person_label_id_,
                     moderatorpersonid, hasModerator_label_id_, Any())) {
      LOG(ERROR) << "Adding edge FORUM[" << forumid << "] hasModerator PERSON[" << moderatorpersonid << "] failed...";
      txn.Abort();
      return false;
    }
    txn.Commit();
    return true;
  }

 private:
  label_t forum_label_id_;
  label_t person_label_id_;
  label_t hasModerator_label_id_;

  GraphDBSession& graph_;
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::INS4* app = new gs::INS4(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::INS4* casted = static_cast<gs::INS4*>(app);
  delete casted;
}
}

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

#define NEW_ALLOCATOR

namespace gs {

class INS6 : public AppBase {
 public:
  INS6(GraphDBSession& graph)
      : post_label_id_(graph.schema().get_vertex_label_id("POST")),
        forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
        person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
        place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
        hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
        hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
        isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
        containerOf_label_id_(graph.schema().get_edge_label_id("CONTAINEROF")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) override {
    oid_t authorpersonid = input.get_long();
    std::string_view browser = input.get_string();
    int64_t creationdate = input.get_long();
    std::string_view content = input.get_string();
    oid_t countryid = input.get_long();
    oid_t forumid = input.get_long();
    oid_t postid = input.get_long();
    std::string_view imagefile = input.get_string();
    std::string_view language = input.get_string();
    int length = input.get_int();
    std::string_view ip_addr = input.get_string();

    auto txn = graph_.GetSingleVertexInsertTransaction();
#ifndef NEW_ALLOCATOR
    if (!txn.AddVertex(
            post_label_id_, postid,
            {Any::From(imagefile), Any::From(Date(creationdate)),
             Any::From(ip_addr), Any::From(browser), Any::From(language),
             Any::From(content), Any::From(length)})) {
      LOG(ERROR) << "Adding vertex POST[" << postid << "] failed...";
      txn.Abort();
      return false;
    }
#else
    if (!txn.AddVertex(
            post_label_id_, postid,person_label_id_,authorpersonid,
            {Any::From(imagefile), Any::From(Date(creationdate)),
             Any::From(ip_addr), Any::From(browser), Any::From(language),
             Any::From(content), Any::From(length)})) {
      LOG(ERROR) << "Adding vertex POST[" << postid << "] failed...";
      txn.Abort();
      return false;
    }
#endif
    if (!txn.AddEdge(post_label_id_, postid, person_label_id_, authorpersonid,
                     hasCreator_label_id_, Any())) {
      LOG(ERROR) << "Adding edge POST[" << postid << "] hasCreator PERSON[" << authorpersonid << "] failed...";
      txn.Abort();
      return false;
    }
    if (!txn.AddEdge(post_label_id_, postid, place_label_id_, countryid,
                     isLocatedIn_label_id_, Any())) {
      LOG(ERROR) << "Adding edge POST[" << postid << "] isLocatedIn PLACE[" << countryid << "] failed...";
      txn.Abort();
      return false;
    }
    if (!txn.AddEdge(forum_label_id_, forumid, post_label_id_, postid,
                     containerOf_label_id_, Any())) {
      LOG(ERROR) << "Adding edge FORUM[" << forumid << "] containerOf POST[" << postid << "] failed...";
      txn.Abort();
      return false;
    }

    int tagnum = input.get_int();
    for (int i = 0; i < tagnum; ++i) {
      oid_t tag = input.get_long();
      if (!txn.AddEdge(post_label_id_, postid, tag_label_id_, tag,
                       hasTag_label_id_, Any())) {
        LOG(ERROR) << "Adding edge POST[" << postid << "] hasTag TAG[" << tag << "] failed...";
        txn.Abort();
        return false;
      }
    }
    CHECK(input.empty());

    txn.Commit();
    return true;
  }

 private:
  label_t post_label_id_;
  label_t forum_label_id_;
  label_t person_label_id_;
  label_t tag_label_id_;
  label_t place_label_id_;
  label_t hasCreator_label_id_;
  label_t hasTag_label_id_;
  label_t isLocatedIn_label_id_;
  label_t containerOf_label_id_;

  GraphDBSession& graph_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::INS6* app = new gs::INS6(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::INS6* casted = static_cast<gs::INS6*>(app);
  delete casted;
}
}

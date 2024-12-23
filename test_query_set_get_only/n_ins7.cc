#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#define NEW_ALLOCATOR
namespace gs {

class INS7 : public AppBase {
 public:
  INS7(GraphDBSession& graph)
      : post_label_id_(graph.schema().get_vertex_label_id("POST")),
        person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
        place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
        hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
        isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
        replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) override {
    oid_t authorpersonid = input.get_long();
    oid_t commentid = input.get_long();
    std::string_view browser = input.get_string();
    std::string_view content = input.get_string();
    oid_t countryid = input.get_long();
    int64_t creationdate = input.get_long();
    int length = input.get_int();
    std::string_view ip_addr = input.get_string();
    oid_t replytocommentid = input.get_long();
    oid_t replytopostid = input.get_long();
    CHECK(input.empty());

    auto txn = graph_.GetSingleVertexInsertTransaction();
#ifndef NEW_ALLOCATOR
    if (!txn.AddVertex(
            comment_label_id_, commentid,
            {Any::From(Date(creationdate)), Any::From(ip_addr),
             Any::From(browser), Any::From(content), Any::From(length)})) {
      LOG(ERROR) << "Adding vertex COMMENT[" << commentid << "] failed...";
      txn.Abort();
      return false;
    }
#else
    if (!txn.AddVertex(
            comment_label_id_, commentid,
            {Any::From(Date(creationdate)), Any::From(ip_addr),
             Any::From(browser), Any::From(content), Any::From(length)})) {
      LOG(ERROR) << "Adding vertex COMMENT[" << commentid << "] failed...";
      txn.Abort();
      return false;
    }
#endif
    if (!txn.AddEdge(comment_label_id_, commentid, person_label_id_,
                     authorpersonid, hasCreator_label_id_, Any())) {
      LOG(ERROR) << "Adding edge COMMENT[" << commentid << "] hasCreator PERSON[" << authorpersonid << "] failed...";
      txn.Abort();
      return false;
    }
    if (!txn.AddEdge(comment_label_id_, commentid, place_label_id_, countryid,
                     isLocatedIn_label_id_, Any())) {
      LOG(ERROR) << "Adding edge COMMENT[" << commentid << "] isLocatedIn PLACE[" << countryid << "] failed...";
      txn.Abort();
      return false;
    }
    if (replytopostid != -1) {
      if (!txn.AddEdge(comment_label_id_, commentid, post_label_id_,
                       replytopostid, replyOf_label_id_, Any())) {
        LOG(ERROR) << "Adding edge COMMENT[" << commentid << "] replyOf POST[" << replytopostid << "] failed...";
        txn.Abort();
        return false;
      }
    }
    if (replytocommentid != -1) {
      if (!txn.AddEdge(comment_label_id_, commentid, comment_label_id_,
                       replytocommentid, replyOf_label_id_, Any())) {
        LOG(ERROR) << "Adding edge COMMENT[" << commentid << "] replyOf COMMENT[" << replytocommentid << "] failed...";
        txn.Abort();
        return false;
      }
    }
    txn.Commit();
    return true;
  }

 private:
  label_t post_label_id_;
  label_t person_label_id_;
  label_t comment_label_id_;
  label_t place_label_id_;
  label_t hasCreator_label_id_;
  label_t isLocatedIn_label_id_;
  label_t replyOf_label_id_;

  GraphDBSession& graph_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::INS7* app = new gs::INS7(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::INS7* casted = static_cast<gs::INS7*>(app);
  delete casted;
}
}

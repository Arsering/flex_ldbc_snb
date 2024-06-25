#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class INS2 : public AppBase
  {
  public:
    INS2(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          likes_label_id_(graph.schema().get_edge_label_id("LIKES")),
          graph_(graph) {}

    bool Query(Decoder &input, Encoder &output) override
    {
      oid_t personid = input.get_long();
      oid_t postid = input.get_long();
      int64_t creationdate = input.get_long();
      CHECK(input.empty());

      auto txn = graph_.GetSingleEdgeInsertTransaction();
      if (!txn.AddEdge(person_label_id_, personid, post_label_id_, postid,
                       likes_label_id_, Any::From(Date(creationdate))))
      {
        LOG(ERROR) << "Adding edge PERSON[" << personid << "] likes(" << creationdate << ") POST[" << postid << "] failed...";
        txn.Abort();
        return false;
      }
      txn.Commit();
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t likes_label_id_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::INS2 *app = new gs::INS2(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::INS2 *casted = static_cast<gs::INS2 *>(app);
    delete casted;
  }
}

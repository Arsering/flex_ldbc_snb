#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "utils.h"

namespace gs
{

  class IS6 : public AppBase
  {
  public:
    IS6(GraphDBSession &graph)
        : forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          containerOf_label_id_(graph.schema().get_edge_label_id("CONTAINEROF")),
          hasModerator_label_id_(
              graph.schema().get_edge_label_id("HASMODERATOR")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          forum_title_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(forum_label_id_, "title")))),
          graph_(graph) {}
    ~IS6() {}

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t req = input.get_long();
      CHECK(input.empty());

      vid_t v{};
      vid_t u = std::numeric_limits<vid_t>::max();
      auto forum_containerOf_post_in =
          txn.GetIncomingSingleGraphView<grape::EmptyType>(
              post_label_id_, forum_label_id_, containerOf_label_id_);
      if (txn.GetVertexIndex(post_label_id_, req, v))
      {
        assert(forum_containerOf_post_in.exist(v));
        u = forum_containerOf_post_in.get_edge(v).neighbor;
      }
      else if (txn.GetVertexIndex(comment_label_id_, req, v))
      {
        auto comment_replyOf_post_out =
            txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                comment_label_id_, post_label_id_, replyOf_label_id_);
        auto comment_replyOf_comment_out =
            txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                comment_label_id_, comment_label_id_, replyOf_label_id_);
        while (true)
        {
          if (comment_replyOf_post_out.exist(v))
          {
            v = comment_replyOf_post_out.get_edge(v).neighbor;
            assert(forum_containerOf_post_in.exist(v));
            u = forum_containerOf_post_in.get_edge(v).neighbor;
            break;
          }
          else
          {
            assert(comment_replyOf_comment_out.exist(v));
            v = comment_replyOf_comment_out.get_edge(v).neighbor;
          }
        }
      }
      else
      {
        return false;
      }

      CHECK(u != std::numeric_limits<vid_t>::max());

      auto forum_hasModerator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              forum_label_id_, person_label_id_, hasModerator_label_id_);

      output.put_long(txn.GetVertexId(forum_label_id_, u));
      output.put_string_view(forum_title_col_.get_view(u));
      assert(forum_hasModerator_person_out.exist(u));
      auto p = forum_hasModerator_person_out.get_edge(u).neighbor;
      output.put_long(txn.GetVertexId(person_label_id_, p));
      output.put_string_view(person_firstName_col_.get_view(p));
      output.put_string_view(person_lastName_col_.get_view(p));

      return true;
    }

  private:
    label_t forum_label_id_;
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t replyOf_label_id_;
    label_t containerOf_label_id_;
    label_t hasModerator_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &forum_title_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IS6 *app = new gs::IS6(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IS6 *casted = static_cast<gs::IS6 *>(app);
    delete casted;
  }
}

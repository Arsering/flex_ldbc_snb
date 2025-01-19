#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

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
          person_firstName_col_(graph.GetPropertyHandle(person_label_id_, "firstName")),
          person_lastName_col_(graph.GetPropertyHandle(person_label_id_, "lastName")),
          forum_title_col_(graph.GetPropertyHandle(forum_label_id_, "title")),
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
#if OV
        assert(forum_containerOf_post_in.exist(v));
        u = forum_containerOf_post_in.get_edge(v).neighbor;
#else

        auto item = forum_containerOf_post_in.get_edge(v);
        assert(forum_containerOf_post_in.exist1(item));
        u = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
#endif
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
#if OV
          if (comment_replyOf_post_out.exist(v))
          {
            v = comment_replyOf_post_out.get_edge(v).neighbor;
            assert(forum_containerOf_post_in.exist(v));
            u = forum_containerOf_post_in.get_edge(v).neighbor;
#else
          auto item = comment_replyOf_post_out.get_edge(v);
          if (comment_replyOf_post_out.exist1(item))
          {
            v = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            // assert(forum_containerOf_post_in.exist(v));
            item = forum_containerOf_post_in.get_edge(v);
            assert(forum_containerOf_post_in.exist1(item));
            u = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
#endif
            break;
          }
          else
          {
#if OV
            assert(comment_replyOf_comment_out.exist(v));
            v = comment_replyOf_comment_out.get_edge(v).neighbor;
#else
            auto item = comment_replyOf_comment_out.get_edge(v);
            assert(comment_replyOf_comment_out.exist1(item));

            v = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
#endif
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
#if OV
      output.put_string_view(forum_title_col_.get_view(u));
      assert(forum_hasModerator_person_out.exist(u));
      auto p = forum_hasModerator_person_out.get_edge(u).neighbor;
      output.put_long(txn.GetVertexId(person_label_id_, p));
      output.put_string_view(person_firstName_col_.get_view(p));
      output.put_string_view(person_lastName_col_.get_view(p));
#else
      auto item = forum_title_col_.getProperty(u);
      output.put_buffer_object(item);
      // auto p = forum_hasModerator_person_out.get_edge(u).neighbor;
      item = forum_hasModerator_person_out.get_edge(u);
      assert(forum_hasModerator_person_out.exist1(item));

      auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
      output.put_long(txn.GetVertexId(person_label_id_, p));
      item = person_firstName_col_.getProperty(p);
      output.put_buffer_object(item);
      item = person_lastName_col_.getProperty(p);
      output.put_buffer_object(item);

#endif
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

    cgraph::PropertyHandle person_firstName_col_;
    cgraph::PropertyHandle person_lastName_col_;
    cgraph::PropertyHandle forum_title_col_;


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

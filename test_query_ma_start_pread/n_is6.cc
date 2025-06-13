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
        std::vector<gs::MutableNbr<grape::EmptyType>> items;
        forum_containerOf_post_in.read(v, items);
        assert(items[0].timestamp.load() <= txn.GetTimeStamp());
        // assert(forum_containerOf_post_in.exist(v));
        // u = forum_containerOf_post_in.get_edge(v).neighbor;
        u = items[0].neighbor;
      }
      else if (txn.GetVertexIndex(comment_label_id_, req, v))
      {
        auto comment_replyOf_post_out =
            txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                comment_label_id_, post_label_id_, replyOf_label_id_);
        auto comment_replyOf_comment_out =
            txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                comment_label_id_, comment_label_id_, replyOf_label_id_);
        std::vector<gs::MutableNbr<grape::EmptyType>> items;
        while (true)
        {
          comment_replyOf_post_out.read(v, items);
          // if (comment_replyOf_post_out.exist(v))
          if (items[0].timestamp.load() <= txn.GetTimeStamp())
          {
            std::vector<gs::MutableNbr<grape::EmptyType>> items;
            v = items[0].neighbor;
            // v = comment_replyOf_post_out.get_edge(v).neighbor;
            forum_containerOf_post_in.read(v, items);
            // assert(forum_containerOf_post_in.exist(v));
            // u = forum_containerOf_post_in.get_edge(v).neighbor;
            assert(items[0].timestamp.load() <= txn.GetTimeStamp());
            u = items[0].neighbor;
            break;
          }
          else
          {
            comment_replyOf_comment_out.read(v, items);
            // assert(comment_replyOf_comment_out.exist(v));
            assert(items[0].timestamp.load() <= txn.GetTimeStamp());
            // v = comment_replyOf_comment_out.get_edge(v).neighbor;
            v = items[0].neighbor;
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
      std::vector<char> item_v;
      std::vector<gs::MutableNbr<grape::EmptyType>> item_e;

      forum_title_col_.read(u, item_v);
      // output.put_string_view(forum_title_col_.get_view(u));
      output.put_string_view({item_v.data(), item_v.size()});
      forum_hasModerator_person_out.read(u, item_e);
      // assert(forum_hasModerator_person_out.exist(u));
      // auto p = forum_hasModerator_person_out.get_edge(u).neighbor;
      assert(item_e[0].timestamp.load() <= txn.GetTimeStamp());
      auto p = item_e[0].neighbor;

      output.put_long(txn.GetVertexId(person_label_id_, p));
      person_firstName_col_.read(p, item_v);
      output.put_string_view({item_v.data(), item_v.size()});
      person_lastName_col_.read(p, item_v);
      output.put_string_view({item_v.data(), item_v.size()});
      // output.put_string_view(person_firstName_col_.get_view(p));
      // output.put_string_view(person_lastName_col_.get_view(p));

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

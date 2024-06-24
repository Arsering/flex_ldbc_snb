#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class IS4 : public AppBase
  {
  public:
    IS4(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),
          post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
          post_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
              graph.get_vertex_property_column(post_label_id_, "length")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          graph_(graph) {}
    ~IS4() {}

    bool Query(Decoder &input, Encoder &output)
    {
      // LOG(INFO)<<"begin is4";
      auto txn = graph_.GetReadTransaction();
      oid_t id = input.get_long();
      CHECK(input.empty());

      vid_t lid{};

#if OV
      if (txn.GetVertexIndex(post_label_id_, id, lid))
      {
        output.put_long(post_creationDate_col_.get_view(lid).milli_second);
        const auto &content = post_length_col_.get_view(lid) == 0
                                  ? post_imageFile_col_.get_view(lid)
                                  : post_content_col_.get_view(lid);
        output.put_string_view(content);
        return true;
      }
      else if (txn.GetVertexIndex(comment_label_id_, id, lid))
      {
        output.put_long(comment_creationDate_col_.get_view(lid).milli_second);
        const auto &content = comment_content_col_.get_view(lid);
        output.put_string_view(content);
        return true;
      }
#else
      if (txn.GetVertexIndex(post_label_id_, id, lid))
      {
        auto item = post_creationDate_col_.get(lid);
        output.put_long(gbp::BufferBlock::Ref<gs::Date>(item).milli_second);

        item = post_length_col_.get(lid);
        auto content = gbp::BufferBlock::Ref<int>(item) == 0 ? post_imageFile_col_.get(lid) : post_content_col_.get(lid);
        output.put_buffer_object(content);
        return true;
      }
      else if (txn.GetVertexIndex(comment_label_id_, id, lid))
      {
        auto item = comment_creationDate_col_.get(lid);
        output.put_long(gbp::BufferBlock::Ref<gs::Date>(item).milli_second);

        const auto &content = comment_content_col_.get(lid);
        output.put_buffer_object(content);
        return true;
      }
#endif
      return false;
    }

  private:
    label_t post_label_id_;
    label_t comment_label_id_;
    const DateColumn &post_creationDate_col_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &post_content_col_;
    const StringColumn &post_imageFile_col_;
    const IntColumn &post_length_col_;
    const StringColumn &comment_content_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IS4 *app = new gs::IS4(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IS4 *casted = static_cast<gs::IS4 *>(app);
    delete casted;
  }
}

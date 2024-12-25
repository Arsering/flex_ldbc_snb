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
          post_creationDate_col_id_(graph.get_vertex_property_column_id(post_label_id_, "creationDate")),
          comment_creationDate_col_id_(graph.get_vertex_property_column_id(comment_label_id_, "creationDate")),
          post_content_col_id_(graph.get_vertex_property_column_id(post_label_id_, "content")),
          post_imageFile_col_id_(graph.get_vertex_property_column_id(post_label_id_, "imageFile")),
          post_length_col_id_(graph.get_vertex_property_column_id(post_label_id_, "length")),
          comment_content_col_id_(graph.get_vertex_property_column_id(comment_label_id_, "content")),
          graph_(graph) {}
    ~IS4() {}

    bool Query(Decoder &input, Encoder &output)
    {
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
        auto item = txn.GetVertexProp(post_label_id_, lid, post_creationDate_col_id_);
        output.put_long(gbp::BufferBlock::Ref<gs::Date>(item).milli_second);

        item = txn.GetVertexProp(post_label_id_, lid, post_length_col_id_);
        auto content = gbp::BufferBlock::Ref<int>(item) == 0 ? txn.GetVertexProp(post_label_id_, lid, post_imageFile_col_id_) : txn.GetVertexProp(post_label_id_, lid, post_content_col_id_);
        output.put_buffer_object(content);
        return true;
      }
      else if (txn.GetVertexIndex(comment_label_id_, id, lid))
      {
        auto item = txn.GetVertexProp(comment_label_id_, lid, comment_creationDate_col_id_);
        output.put_long(gbp::BufferBlock::Ref<gs::Date>(item).milli_second);

        auto content = txn.GetVertexProp(comment_label_id_, lid, comment_content_col_id_);
        output.put_buffer_object(content);
        return true;
      }
#endif
      return false;
    }

  private:
    label_t post_label_id_;
    label_t comment_label_id_;

    int post_creationDate_col_id_;
    int comment_creationDate_col_id_;
    int post_content_col_id_;
    int post_imageFile_col_id_;
    int post_length_col_id_;
    int comment_content_col_id_;

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

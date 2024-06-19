#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include <flex/utils/property/column.h>

namespace gs
{

class FORUM_IS : public AppBase {
public:
  FORUM_IS(GraphDBSession &graph)
      : forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
        forum_title_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(forum_label_id_, "title")))),
        forum_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(forum_label_id_, "creationDate")))),
        graph_(graph) {}
  ~FORUM_IS() {}

  bool Query(Decoder &input, Encoder &output) {

    // LOG(INFO)<<"begin forum is";

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
    if (txn.GetVertexIndex(forum_label_id_, id, lid)) {
      auto content = forum_title_col_.get(lid) ;
      auto date=forum_creationDate_col_.get(lid);
      output.put_buffer_object(content);
      output.put_long(gbp::BufferBlock::Ref<gs::Date>(date).milli_second);
      return true;
    }
#endif
      return false;
    }

  private:
    label_t forum_label_id_;
    const StringColumn &forum_title_col_;
    const DateColumn &forum_creationDate_col_;
    GraphDBSession &graph_;
};

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::FORUM_IS *app = new gs::FORUM_IS(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::FORUM_IS *casted = static_cast<gs::FORUM_IS *>(app);
    delete casted;
  }
}

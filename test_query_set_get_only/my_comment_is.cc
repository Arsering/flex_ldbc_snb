#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include <flex/graphscope_bufferpool/include/buffer_obj.h>
#include <flex/utils/property/column.h>

namespace gs
{

class COMMENT_IS : public AppBase {
public:
  COMMENT_IS(GraphDBSession &graph)
      : comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
        comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
            graph.get_vertex_property_column(comment_label_id_,
                                             "creationDate")))),
        comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(comment_label_id_, "content")))),
        comment_locationIP_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(comment_label_id_, "locationIP")))),
        comment_browserUsed_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(comment_label_id_, "browserUsed")))),
        comment_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
            graph.get_vertex_property_column(comment_label_id_, "length")))),
        graph_(graph) {}
  ~COMMENT_IS() {}

  bool Query(Decoder &input, Encoder &output) {
    // LOG(INFO)<<"begin comment is";
    auto txn = graph_.GetReadTransaction();
    oid_t id = input.get_long();
    CHECK(input.empty());

    vid_t lid{};

#if OV
      if (txn.GetVertexIndex(comment_label_id_, id, lid))
      {
        output.put_long(comment_creationDate_col_.get_view(lid).milli_second);
        const auto &content = comment_content_col_.get_view(lid);
        output.put_string_view(content);
        return true;
      }
#else
      if (txn.GetVertexIndex(comment_label_id_, id, lid))
      {
        // auto item = comment_creationDate_col_.get(lid);
        // output.put_long(gbp::BufferBlock::Ref<gs::Date>(item).milli_second);

        // const auto &content = comment_content_col_.get(lid);
        // output.put_buffer_object(content);

        // const auto &length=comment_length_col_.get(lid);
        // const auto &browser_used=comment_browserUsed_col_.get(lid);
        // const auto &location=comment_locationIP_col_.get(lid);
        // output.put_int(gbp::BufferBlock::Ref<int>(length));
        // output.put_buffer_object(browser_used);
        // output.put_buffer_object(location);
        return true;
      }
#endif
      return false;
    }

  private:
    label_t comment_label_id_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &comment_content_col_;
    const StringColumn &comment_browserUsed_col_;
    const StringColumn &comment_locationIP_col_;
    const IntColumn &comment_length_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::COMMENT_IS *app = new gs::COMMENT_IS(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::COMMENT_IS *casted = static_cast<gs::COMMENT_IS *>(app);
    delete casted;
  }
}

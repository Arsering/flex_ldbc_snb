#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IS4 : public AppBase {
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

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();
        oid_t id = input.get_long();
        CHECK(input.empty());

        vid_t lid{};
        if (txn.GetVertexIndex(post_label_id_, id, lid)) {
            // 批量获取帖子属性
            auto props = txn.BatchGetVertexPropsFromVid(
                post_label_id_, {lid},
                std::vector<std::string>{
                    "creationDate", "content", "imageFile", "length"
                });

            output.put_long(gbp::BufferBlock::Ref<Date>(props[0]).milli_second);
            
            auto length = gbp::BufferBlock::Ref<int>(props[3]);
            output.put_buffer_object(length == 0 ? props[2] : props[1]);
            return true;
        }
        else if (txn.GetVertexIndex(comment_label_id_, id, lid)) {
            // 批量获取评论属性
            auto props = txn.BatchGetVertexPropsFromVid(
                comment_label_id_, {lid},
                std::vector<std::string>{"creationDate", "content"});

            output.put_long(gbp::BufferBlock::Ref<Date>(props[0]).milli_second);
            output.put_buffer_object(props[1]);
            return true;
        }
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

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IS4 *app = new gs::IS4(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IS4 *casted = static_cast<gs::IS4 *>(app);
        delete casted;
    }
} 
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IS5 : public AppBase {
public:
    IS5(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t id = input.get_long();
        CHECK(input.empty());

        vid_t lid{};
        vid_t creator_vid;

        if (txn.GetVertexIndex(post_label_id_, id, lid)) {
            // 获取帖子的创建者
            auto creators = txn.GetOtherVertices(
                post_label_id_, person_label_id_, hasCreator_label_id_,
                {lid}, "out");
            CHECK(creators[0].size() == 1) << "Post should have exactly one creator";
            creator_vid = creators[0][0];
        }
        else if (txn.GetVertexIndex(comment_label_id_, id, lid)) {
            // 获取评论的创建者
            auto creators = txn.GetOtherVertices(
                comment_label_id_, person_label_id_, hasCreator_label_id_,
                {lid}, "out");
            CHECK(creators[0].size() == 1) << "Comment should have exactly one creator";
            creator_vid = creators[0][0];
        }
        else {
            return false;
        }

        // 批量获取创建者属性
        auto creator_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, {creator_vid},
            std::vector<std::string>{"firstName", "lastName"});

        output.put_long(txn.GetVertexId(person_label_id_, creator_vid));
        output.put_buffer_object(creator_props[0]); // firstName
        output.put_buffer_object(creator_props[1]); // lastName

        return true;
    }

private:
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t person_label_id_;
    label_t hasCreator_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IS5 *app = new gs::IS5(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IS5 *casted = static_cast<gs::IS5 *>(app);
        delete casted;
    }
} 
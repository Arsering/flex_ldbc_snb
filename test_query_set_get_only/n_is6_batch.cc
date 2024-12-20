#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IS6 : public AppBase {
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

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        CHECK(input.empty());

        vid_t message_vid{};
        vid_t forum_vid;

        // 查找消息所在的论坛
        if (txn.GetVertexIndex(post_label_id_, req, message_vid)) {
            // 如果是帖子,直接获取所在论坛
            auto forums = txn.GetOtherVertices(
                post_label_id_, forum_label_id_, containerOf_label_id_,
                {message_vid}, "in");
            CHECK(forums[0].size() == 1) << "Post should belong to exactly one forum";
            forum_vid = forums[0][0];
        }
        else if (txn.GetVertexIndex(comment_label_id_, req, message_vid)) {
            // 如果是评论,需要找到原始帖子
            vid_t current = message_vid;
            while (true) {
                // 尝试获取回复的帖子
                auto posts = txn.GetOtherVertices(
                    comment_label_id_, post_label_id_, replyOf_label_id_,
                    {current}, "out");
                
                if (!posts[0].empty()) {
                    // 找到了原始帖子
                    message_vid = posts[0][0];
                    // 获取帖子所在论坛
                    auto forums = txn.GetOtherVertices(
                        post_label_id_, forum_label_id_, containerOf_label_id_,
                        {message_vid}, "in");
                    CHECK(forums[0].size() == 1) << "Post should belong to exactly one forum";
                    forum_vid = forums[0][0];
                    break;
                }

                // 获取回复的评论
                auto comments = txn.GetOtherVertices(
                    comment_label_id_, comment_label_id_, replyOf_label_id_,
                    {current}, "out");
                CHECK(!comments[0].empty()) << "Comment should reply to something";
                current = comments[0][0];
            }
        }
        else {
            return false;
        }

        // 批量获取论坛属性和版主
        auto forum_props = txn.BatchGetVertexPropsFromVid(
            forum_label_id_, {forum_vid},
            std::vector<std::string>{"title"});

        auto moderators = txn.GetOtherVertices(
            forum_label_id_, person_label_id_, hasModerator_label_id_,
            {forum_vid}, "out");
        CHECK(moderators[0].size() == 1) << "Forum should have exactly one moderator";
        vid_t moderator_vid = moderators[0][0];

        // 批量获取版主属性
        auto moderator_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, {moderator_vid},
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        output.put_long(txn.GetVertexId(forum_label_id_, forum_vid));
        output.put_buffer_object(forum_props[0]); // title
        output.put_long(txn.GetVertexId(person_label_id_, moderator_vid));
        output.put_buffer_object(moderator_props[0]); // firstName
        output.put_buffer_object(moderator_props[1]); // lastName

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

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IS6 *app = new gs::IS6(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IS6 *casted = static_cast<gs::IS6 *>(app);
        delete casted;
    }
} 
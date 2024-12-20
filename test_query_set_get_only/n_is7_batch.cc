#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IS7 : public AppBase {
public:
    IS7(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          graph_(graph) {}

    struct comment_info {
        comment_info(vid_t commentid, vid_t pid, oid_t personid,
                   int64_t creationdate)
            : commentid(commentid),
              pid(pid),
              personid(personid),
              creationdate(creationdate) {}
        vid_t commentid;
        vid_t pid;
        oid_t personid;
        int64_t creationdate;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        CHECK(input.empty());

        vid_t message_vid{};
        vid_t message_author_id;

        // 获取消息作者
        if (txn.GetVertexIndex(post_label_id_, req, message_vid)) {
            auto creators = txn.GetOtherVertices(
                post_label_id_, person_label_id_, hasCreator_label_id_,
                {message_vid}, "out");
            CHECK(creators[0].size() == 1) << "Post should have exactly one creator";
            message_author_id = creators[0][0];
        }
        else if (txn.GetVertexIndex(comment_label_id_, req, message_vid)) {
            auto creators = txn.GetOtherVertices(
                comment_label_id_, person_label_id_, hasCreator_label_id_,
                {message_vid}, "out");
            CHECK(creators[0].size() == 1) << "Comment should have exactly one creator";
            message_author_id = creators[0][0];
        }
        else {
            return false;
        }

        // 获取作者的朋友
        auto out_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {message_author_id}, "out");

        auto in_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {message_author_id}, "in");

        // 标记所有朋友
        std::vector<bool> friends;
        friends.resize(txn.GetVertexNum(person_label_id_));
        for (const auto& friend_vid : out_friends[0]) {
            friends[friend_vid] = true;
        }
        for (const auto& friend_vid : in_friends[0]) {
            friends[friend_vid] = true;
        }

        // 获取评论
        auto comments = txn.GetOtherVertices(
            comment_label_id_, comment_label_id_, replyOf_label_id_,
            {message_vid}, "in");

        std::vector<comment_info> comment_list;
        std::vector<vid_t> comment_vids;

        // 获取评论的创建者
        for (const auto& comment_vid : comments[0]) {
            auto creators = txn.GetOtherVertices(
                comment_label_id_, person_label_id_, hasCreator_label_id_,
                {comment_vid}, "out");
            CHECK(creators[0].size() == 1) << "Comment should have exactly one creator";
            vid_t creator_vid = creators[0][0];
            
            comment_list.emplace_back(comment_vid, creator_vid,
                txn.GetVertexId(person_label_id_, creator_vid), 0);
            comment_vids.push_back(comment_vid);
        }

        // 批量获取评论属性
        auto comment_props = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, comment_vids,
            std::vector<std::string>{"creationDate", "content"});

        // 更新评论创建时间
        for (size_t i = 0; i < comment_list.size(); i++) {
            comment_list[i].creationdate = 
                gbp::BufferBlock::Ref<Date>(comment_props[i * 2]).milli_second;
        }

        // 按创建时间排序
        std::sort(comment_list.begin(), comment_list.end(),
            [](const comment_info& a, const comment_info& b) {
                return a.creationdate < b.creationdate;
            });

        // 批量获取创建者属性
        std::vector<vid_t> creator_vids;
        for (const auto& comment : comment_list) {
            creator_vids.push_back(comment.pid);
        }

        auto creator_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, creator_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t i = 0; i < comment_list.size(); i++) {
            const auto& comment = comment_list[i];
            output.put_long(txn.GetVertexId(comment_label_id_, comment.commentid));
            output.put_long(comment.creationdate);
            output.put_buffer_object(comment_props[i * 2 + 1]); // content
            output.put_long(comment.personid);
            output.put_buffer_object(creator_props[i * 2]);     // firstName
            output.put_buffer_object(creator_props[i * 2 + 1]); // lastName
            output.put_byte(friends[comment.pid] ? 1 : 0);
        }

        return true;
    }

private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t replyOf_label_id_;
    label_t hasCreator_label_id_;
    label_t knows_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &comment_content_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IS7 *app = new gs::IS7(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IS7 *casted = static_cast<gs::IS7 *>(app);
        delete casted;
    }
} 
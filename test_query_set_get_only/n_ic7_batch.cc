#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC7 : public AppBase {
public:
    IC7(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          graph_(graph) {}

    struct comment_info {
        comment_info(vid_t v_, oid_t comment_id_, vid_t creator_vid_,
                    int64_t creationdate_)
            : v(v_), comment_id(comment_id_), creator_vid(creator_vid_),
              creationdate(creationdate_) {}
        vid_t v;
        oid_t comment_id;
        vid_t creator_vid;
        int64_t creationdate;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        CHECK(input.empty());

        vid_t post_vid{};
        if (!txn.GetVertexIndex(post_label_id_, req, post_vid)) {
            return false;
        }

        // 获取帖子的创建者
        auto post_creators = txn.GetOtherVertices(
            post_label_id_, person_label_id_, hasCreator_label_id_,
            {post_vid}, "out");
        CHECK(post_creators[0].size() == 1) << "Post should have exactly one creator";
        vid_t post_author_id = post_creators[0][0];

        // 获取所有评论
        auto comments = txn.GetOtherVertices(
            post_label_id_, comment_label_id_, replyOf_label_id_,
            {post_vid}, "in");

        // 获取评论的创建者
        auto creators = txn.GetOtherVertices(
            comment_label_id_, person_label_id_, hasCreator_label_id_,
            comments[0], "out");

        // 收集评论信息
        std::vector<comment_info> comment_list;
        std::vector<vid_t> comment_vids;
        std::vector<vid_t> creator_vids;

        for (size_t i = 0; i < comments[0].size(); i++) {
            CHECK(creators[i].size() == 1) << "Comment should have exactly one creator";
            vid_t creator_vid = creators[i][0];
            vid_t comment_vid = comments[0][i];

            comment_vids.push_back(comment_vid);
            creator_vids.push_back(creator_vid);
            comment_list.emplace_back(comment_vid,
                txn.GetVertexId(comment_label_id_, comment_vid),
                creator_vid, 0);
        }

        // 批量获取评论创建时间
        auto comment_dates = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, comment_vids,
            std::vector<std::string>{"creationDate"});

        // 更新创建时间
        for (size_t i = 0; i < comment_list.size(); i++) {
            comment_list[i].creationdate = gbp::BufferBlock::Ref<Date>(
                comment_dates[i]).milli_second;
        }

        // 按创建时间排序
        std::sort(comment_list.begin(), comment_list.end(),
            [](const comment_info& a, const comment_info& b) {
                if (a.creationdate != b.creationdate) {
                    return a.creationdate < b.creationdate;
                }
                return a.comment_id < b.comment_id;
            });

        // 获取创建者的朋友关系
        std::vector<bool> is_friend;
        is_friend.resize(txn.GetVertexNum(person_label_id_));

        auto out_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {post_author_id}, "out");

        auto in_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {post_author_id}, "in");

        // 标记所有朋友
        for (const auto& friend_vid : out_friends[0]) {
            is_friend[friend_vid] = true;
        }
        for (const auto& friend_vid : in_friends[0]) {
            is_friend[friend_vid] = true;
        }

        // 批量获取创建者属性
        auto creator_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, creator_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t i = 0; i < comment_list.size(); i++) {
            const auto& comment = comment_list[i];
            output.put_long(comment.comment_id);
            output.put_long(comment.creationdate);
            output.put_long(txn.GetVertexId(person_label_id_, comment.creator_vid));
            output.put_buffer_object(creator_props[i * 2]);     // firstName
            output.put_buffer_object(creator_props[i * 2 + 1]); // lastName
            output.put_byte(is_friend[comment.creator_vid] ? 1 : 0);
        }

        return true;
    }

private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const DateColumn &comment_creationDate_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC7 *app = new gs::IC7(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC7 *casted = static_cast<gs::IC7 *>(app);
        delete casted;
    }
} 
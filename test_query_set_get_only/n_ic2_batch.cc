#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC2 : public AppBase {
public:
    IC2(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),
          post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
          post_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
              graph.get_vertex_property_column(post_label_id_, "length")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          graph_(graph) {}

    struct message_info {
        message_info(bool is_post_, vid_t v_, oid_t messageid_, 
                    vid_t creator_vid_, int64_t creationdate_)
            : is_post(is_post_), v(v_), messageid(messageid_),
              creator_vid(creator_vid_), creationdate(creationdate_) {}
        bool is_post;
        vid_t v;
        oid_t messageid;
        vid_t creator_vid;
        int64_t creationdate;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        int64_t max_date = input.get_long();
        CHECK(input.empty());

        vid_t root{};
        if (!txn.GetVertexIndex(person_label_id_, req, root)) {
            return false;
        }

        // 获取所有朋友
        auto out_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {root}, "out");

        auto in_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {root}, "in");

        // 收集所有朋友
        std::vector<vid_t> friends;
        friends.insert(friends.end(), out_friends[0].begin(), out_friends[0].end());
        friends.insert(friends.end(), in_friends[0].begin(), in_friends[0].end());

        // 获取朋友创建的帖子
        auto posts = txn.GetOtherVertices(
            person_label_id_, post_label_id_, hasCreator_label_id_,
            friends, "in");

        // 获取朋友创建的评论
        auto comments = txn.GetOtherVertices(
            person_label_id_, comment_label_id_, hasCreator_label_id_,
            friends, "in");

        // 收集所有消息
        std::vector<message_info> messages;
        std::vector<vid_t> post_vids;
        std::vector<vid_t> comment_vids;

        // 处理帖子
        for (size_t i = 0; i < friends.size(); i++) {
            for (const auto& post_vid : posts[i]) {
                post_vids.push_back(post_vid);
                messages.emplace_back(true, post_vid,
                    txn.GetVertexId(post_label_id_, post_vid),
                    friends[i], 0);
            }
        }

        // 处理评论
        for (size_t i = 0; i < friends.size(); i++) {
            for (const auto& comment_vid : comments[i]) {
                comment_vids.push_back(comment_vid);
                messages.emplace_back(false, comment_vid,
                    txn.GetVertexId(comment_label_id_, comment_vid),
                    friends[i], 0);
            }
        }

        // 批量获取消息创建时间
        auto post_dates = txn.BatchGetVertexPropsFromVid(
            post_label_id_, post_vids,
            std::vector<std::string>{"creationDate"});

        auto comment_dates = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, comment_vids,
            std::vector<std::string>{"creationDate"});

        // 更新创建时间并过滤
        size_t post_idx = 0, comment_idx = 0;
        std::vector<message_info> filtered_messages;
        for (auto& msg : messages) {
            int64_t creationdate;
            if (msg.is_post) {
                creationdate = gbp::BufferBlock::Ref<Date>(
                    post_dates[post_idx++]).milli_second;
            } else {
                creationdate = gbp::BufferBlock::Ref<Date>(
                    comment_dates[comment_idx++]).milli_second;
            }
            if (creationdate <= max_date) {
                msg.creationdate = creationdate;
                filtered_messages.push_back(msg);
            }
        }

        // 按创建时间排序
        std::sort(filtered_messages.begin(), filtered_messages.end(),
            [](const message_info& a, const message_info& b) {
                if (a.creationdate != b.creationdate) {
                    return a.creationdate > b.creationdate;
                }
                return a.messageid > b.messageid;
            });

        // 批量获取消息内容
        std::vector<vid_t> filtered_post_vids;
        std::vector<vid_t> filtered_comment_vids;
        std::vector<vid_t> creator_vids;
        for (const auto& msg : filtered_messages) {
            if (msg.is_post) {
                filtered_post_vids.push_back(msg.v);
            } else {
                filtered_comment_vids.push_back(msg.v);
            }
            creator_vids.push_back(msg.creator_vid);
        }

        auto post_contents = txn.BatchGetVertexPropsFromVid(
            post_label_id_, filtered_post_vids,
            std::vector<std::string>{"content", "imageFile", "length"});

        auto comment_contents = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, filtered_comment_vids,
            std::vector<std::string>{"content"});

        // 批量获取创建者属性
        auto creator_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, creator_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        post_idx = 0;
        comment_idx = 0;
        for (size_t i = 0; i < filtered_messages.size(); i++) {
            const auto& msg = filtered_messages[i];
            output.put_long(msg.messageid);
            output.put_long(msg.creationdate);
            output.put_long(txn.GetVertexId(person_label_id_, msg.creator_vid));
            output.put_buffer_object(creator_props[i * 2]);     // firstName
            output.put_buffer_object(creator_props[i * 2 + 1]); // lastName

            if (msg.is_post) {
                auto length = gbp::BufferBlock::Ref<int>(
                    post_contents[post_idx * 3 + 2]);
                output.put_buffer_object(length == 0 ? 
                    post_contents[post_idx * 3 + 1] : // imageFile
                    post_contents[post_idx * 3]);     // content
                post_idx++;
            } else {
                output.put_buffer_object(comment_contents[comment_idx++]);
            }
        }

        return true;
    }

private:
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t person_label_id_;
    label_t knows_label_id_;
    label_t hasCreator_label_id_;

    const DateColumn &post_creationDate_col_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &post_content_col_;
    const StringColumn &post_imageFile_col_;
    const IntColumn &post_length_col_;
    const StringColumn &comment_content_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC2 *app = new gs::IC2(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC2 *casted = static_cast<gs::IC2 *>(app);
        delete casted;
    }
} 
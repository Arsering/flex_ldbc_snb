#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IS2 : public AppBase {
public:
    IS2(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
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

    struct message_info {
        message_info(bool is_post_, vid_t v_, oid_t messageid_)
            : is_post(is_post_), v(v_), messageid(messageid_), creationdate(0) {}
        bool is_post;
        vid_t v;
        oid_t messageid;
        int64_t creationdate;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        CHECK(input.empty());

        vid_t root{};
        if (!txn.GetVertexIndex(person_label_id_, req, root)) {
            return false;
        }

        // 批量获取该人创建的帖子和评论
        auto posts = txn.GetOtherVertices(
            person_label_id_, post_label_id_, hasCreator_label_id_,
            {root}, "in");
        
        auto comments = txn.GetOtherVertices(
            person_label_id_, comment_label_id_, hasCreator_label_id_,
            {root}, "in");

        // 收集所有消息
        std::vector<message_info> messages;
        
        // 处理帖子
        for (const auto& post_list : posts) {
            for (auto post_vid : post_list) {
                messages.emplace_back(true, post_vid, 
                    txn.GetVertexId(post_label_id_, post_vid));
            }
        }

        // 处理评论
        for (const auto& comment_list : comments) {
            for (auto comment_vid : comment_list) {
                messages.emplace_back(false, comment_vid,
                    txn.GetVertexId(comment_label_id_, comment_vid));
            }
        }

        // 批量获取创建时间
        std::vector<vid_t> post_vids;
        std::vector<vid_t> comment_vids;
        for (const auto& msg : messages) {
            if (msg.is_post) {
                post_vids.push_back(msg.v);
            } else {
                comment_vids.push_back(msg.v);
            }
        }

        auto post_dates = txn.BatchGetVertexPropsFromVid(
            post_label_id_, post_vids,
            std::vector<std::string>{"creationDate"});
        
        auto comment_dates = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, comment_vids,
            std::vector<std::string>{"creationDate"});

        // 更新创建时间
        size_t post_idx = 0, comment_idx = 0;
        for (auto& msg : messages) {
            if (msg.is_post) {
                msg.creationdate = gbp::BufferBlock::Ref<Date>(
                    post_dates[post_idx++]).milli_second;
            } else {
                msg.creationdate = gbp::BufferBlock::Ref<Date>(
                    comment_dates[comment_idx++]).milli_second;
            }
        }

        // 排序并输出结果
        std::sort(messages.begin(), messages.end(),
            [](const message_info& a, const message_info& b) {
                if (a.creationdate != b.creationdate) {
                    return a.creationdate > b.creationdate;
                }
                return a.messageid > b.messageid;
            });

        // 批量获取内容
        auto post_contents = txn.BatchGetVertexPropsFromVid(
            post_label_id_, post_vids,
            std::vector<std::string>{"content", "imageFile", "length"});
        
        auto comment_contents = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, comment_vids,
            std::vector<std::string>{"content"});

        // 输出结果
        post_idx = 0;
        comment_idx = 0;
        for (const auto& msg : messages) {
            output.put_long(msg.messageid);
            output.put_long(msg.creationdate);
            
            if (msg.is_post) {
                auto length = gbp::BufferBlock::Ref<int>(post_contents[post_idx * 3 + 2]);
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
    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
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
        gs::IS2 *app = new gs::IS2(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IS2 *casted = static_cast<gs::IS2 *>(app);
        delete casted;
    }
} 
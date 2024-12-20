#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC12 : public AppBase {
public:
    IC12(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          tagclass_label_id_(graph.schema().get_vertex_label_id("TAGCLASS")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasType_label_id_(graph.schema().get_edge_label_id("HASTYPE")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          tagclass_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tagclass_label_id_, "name")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          graph_(graph) {}

    struct person_info {
        person_info(vid_t v_, oid_t person_id_, int reply_count_)
            : v(v_), person_id(person_id_), reply_count(reply_count_) {}
        vid_t v;
        oid_t person_id;
        int reply_count;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        oid_t tagClassName = input.get_long();
        CHECK(input.empty());

        vid_t person_vid{};
        if (!txn.GetVertexIndex(person_label_id_, req, person_vid)) {
            return false;
        }

        // 获取此人的所有朋友
        auto out_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {person_vid}, "out");

        auto in_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {person_vid}, "in");

        // 收集所有朋友
        std::vector<vid_t> friends;
        friends.insert(friends.end(), out_friends[0].begin(), out_friends[0].end());
        friends.insert(friends.end(), in_friends[0].begin(), in_friends[0].end());

        // 获取朋友创建的评论
        auto comments = txn.GetOtherVertices(
            person_label_id_, comment_label_id_, hasCreator_label_id_,
            friends, "in");

        // 获取评论的标签
        std::vector<vid_t> all_comment_vids;
        std::vector<vid_t> comment_creators;
        for (size_t i = 0; i < friends.size(); i++) {
            for (const auto& comment_vid : comments[i]) {
                all_comment_vids.push_back(comment_vid);
                comment_creators.push_back(friends[i]);
            }
        }

        auto comment_tags = txn.GetOtherVertices(
            comment_label_id_, tag_label_id_, hasTag_label_id_,
            all_comment_vids, "out");

        // 获取标签的类型
        std::vector<vid_t> tag_vids;
        for (const auto& tags : comment_tags) {
            tag_vids.insert(tag_vids.end(), tags.begin(), tags.end());
        }

        auto tag_classes = txn.GetOtherVertices(
            tag_label_id_, tagclass_label_id_, hasType_label_id_,
            tag_vids, "out");

        // 统计每个朋友的回复数量
        std::unordered_map<vid_t, int> reply_counts;
        size_t tag_idx = 0;
        for (size_t i = 0; i < all_comment_vids.size(); i++) {
            for (size_t j = 0; j < comment_tags[i].size(); j++) {
                CHECK(tag_classes[tag_idx].size() == 1) << "Tag should have exactly one type";
                vid_t tagclass_vid = tag_classes[tag_idx][0];
                oid_t tagclass_id = txn.GetVertexId(tagclass_label_id_, tagclass_vid);
                if (tagclass_id == tagClassName) {
                    reply_counts[comment_creators[i]]++;
                }
                tag_idx++;
            }
        }

        // 收集朋友信息
        std::vector<person_info> person_list;
        std::vector<vid_t> person_vids;
        for (const auto& p : reply_counts) {
            if (p.second > 0) {
                person_vids.push_back(p.first);
                person_list.emplace_back(p.first,
                    txn.GetVertexId(person_label_id_, p.first),
                    p.second);
            }
        }

        // 按回复数量和用户ID排序
        std::sort(person_list.begin(), person_list.end(),
            [](const person_info& a, const person_info& b) {
                if (a.reply_count != b.reply_count) {
                    return a.reply_count > b.reply_count;
                }
                return a.person_id < b.person_id;
            });

        // 批量获取朋友属性
        auto person_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, person_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t i = 0; i < person_list.size(); i++) {
            const auto& person = person_list[i];
            output.put_long(person.person_id);
            output.put_buffer_object(person_props[i * 2]);     // firstName
            output.put_buffer_object(person_props[i * 2 + 1]); // lastName
            output.put_int(person.reply_count);
        }

        return true;
    }

private:
    label_t person_label_id_;
    label_t tag_label_id_;
    label_t tagclass_label_id_;
    label_t comment_label_id_;
    label_t knows_label_id_;
    label_t hasType_label_id_;
    label_t hasTag_label_id_;
    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &tagclass_name_col_;
    const DateColumn &comment_creationDate_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC12 *app = new gs::IC12(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC12 *casted = static_cast<gs::IC12 *>(app);
        delete casted;
    }
} 
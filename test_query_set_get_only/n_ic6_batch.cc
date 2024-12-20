#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC6 : public AppBase {
public:
    IC6(GraphDBSession &graph)
        : tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tag_label_id_, "name")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}

    struct person_info {
        person_info(vid_t v_, oid_t person_id_, int post_count_)
            : v(v_), person_id(person_id_), post_count(post_count_) {}
        vid_t v;
        oid_t person_id;
        int post_count;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        CHECK(input.empty());

        vid_t tag_vid{};
        if (!txn.GetVertexIndex(tag_label_id_, req, tag_vid)) {
            return false;
        }

        // 获取包含该标签的帖子
        auto posts = txn.GetOtherVertices(
            tag_label_id_, post_label_id_, hasTag_label_id_,
            {tag_vid}, "in");

        // 获取帖子的创建者
        auto creators = txn.GetOtherVertices(
            post_label_id_, person_label_id_, hasCreator_label_id_,
            posts[0], "out");

        // 统计每个人创建的帖子数量
        std::unordered_map<vid_t, int> person_post_counts;
        for (const auto& creator_vid : creators) {
            CHECK(creator_vid.size() == 1) << "Post should have exactly one creator";
            person_post_counts[creator_vid[0]]++;
        }

        // 收集创建者信息
        std::vector<person_info> person_list;
        std::vector<vid_t> person_vids;
        for (const auto& p : person_post_counts) {
            person_vids.push_back(p.first);
            person_list.emplace_back(p.first,
                txn.GetVertexId(person_label_id_, p.first),
                p.second);
        }

        // 按帖子数量和用户ID排序
        std::sort(person_list.begin(), person_list.end(),
            [](const person_info& a, const person_info& b) {
                if (a.post_count != b.post_count) {
                    return a.post_count > b.post_count;
                }
                return a.person_id < b.person_id;
            });

        // 批量获取创建者属性
        auto person_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, person_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t i = 0; i < person_list.size(); i++) {
            const auto& person = person_list[i];
            output.put_long(person.person_id);
            output.put_buffer_object(person_props[i * 2]);     // firstName
            output.put_buffer_object(person_props[i * 2 + 1]); // lastName
            output.put_int(person.post_count);
        }

        return true;
    }

private:
    label_t tag_label_id_;
    label_t person_label_id_;
    label_t post_label_id_;
    label_t knows_label_id_;
    label_t hasTag_label_id_;
    label_t hasCreator_label_id_;

    const StringColumn &tag_name_col_;
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC6 *app = new gs::IC6(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC6 *casted = static_cast<gs::IC6 *>(app);
        delete casted;
    }
} 
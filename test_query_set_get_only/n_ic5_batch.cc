#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC5 : public AppBase {
public:
    IC5(GraphDBSession &graph)
        : tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          hasMember_label_id_(graph.schema().get_edge_label_id("HASMEMBER")),
          containerOf_label_id_(graph.schema().get_edge_label_id("CONTAINEROF")),
          tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tag_label_id_, "name")))),
          forum_title_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(forum_label_id_, "title")))),
          graph_(graph) {}

    struct forum_info {
        forum_info(vid_t v_, oid_t forum_id_, int member_count_)
            : v(v_), forum_id(forum_id_), member_count(member_count_) {}
        vid_t v;
        oid_t forum_id;
        int member_count;
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

        // 获取帖子所在的论坛
        auto forums = txn.GetOtherVertices(
            post_label_id_, forum_label_id_, containerOf_label_id_,
            posts[0], "in");

        // 统计论坛出现次数并去重
        std::unordered_map<vid_t, int> forum_post_counts;
        for (const auto& forum_vid : forums) {
            CHECK(forum_vid.size() == 1) << "Post should belong to exactly one forum";
            forum_post_counts[forum_vid[0]]++;
        }

        // 获取论坛的成员数量
        std::vector<vid_t> forum_vids;
        for (const auto& p : forum_post_counts) {
            forum_vids.push_back(p.first);
        }

        auto members = txn.GetOtherVertices(
            forum_label_id_, person_label_id_, hasMember_label_id_,
            forum_vids, "out");

        // 收集论坛信息
        std::vector<forum_info> forum_list;
        for (size_t i = 0; i < forum_vids.size(); i++) {
            forum_list.emplace_back(forum_vids[i],
                txn.GetVertexId(forum_label_id_, forum_vids[i]),
                members[i].size());
        }

        // 按成员数量和论坛ID排序
        std::sort(forum_list.begin(), forum_list.end(),
            [](const forum_info& a, const forum_info& b) {
                if (a.member_count != b.member_count) {
                    return a.member_count > b.member_count;
                }
                return a.forum_id < b.forum_id;
            });

        // 批量获取论坛标题
        auto forum_props = txn.BatchGetVertexPropsFromVid(
            forum_label_id_, forum_vids,
            std::vector<std::string>{"title"});

        // 输出结果
        for (size_t i = 0; i < forum_list.size(); i++) {
            const auto& forum = forum_list[i];
            output.put_long(forum.forum_id);
            output.put_buffer_object(forum_props[i]); // title
            output.put_int(forum_post_counts[forum.v]); // postCount
            output.put_int(forum.member_count); // memberCount
        }

        return true;
    }

private:
    label_t tag_label_id_;
    label_t person_label_id_;
    label_t forum_label_id_;
    label_t post_label_id_;
    label_t knows_label_id_;
    label_t hasTag_label_id_;
    label_t hasMember_label_id_;
    label_t containerOf_label_id_;

    const StringColumn &tag_name_col_;
    const StringColumn &forum_title_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC5 *app = new gs::IC5(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC5 *casted = static_cast<gs::IC5 *>(app);
        delete casted;
    }
} 
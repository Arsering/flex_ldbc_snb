#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC4 : public AppBase {
public:
    IC4(GraphDBSession &graph)
        : tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tag_label_id_, "name")))),
          graph_(graph) {}

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        int64_t startDate = input.get_long();
        int daysNum = input.get_int();
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

        // 收集所有帖子
        std::vector<vid_t> post_vids;
        std::vector<vid_t> creator_vids;
        for (size_t i = 0; i < friends.size(); i++) {
            for (const auto& post_vid : posts[i]) {
                post_vids.push_back(post_vid);
                creator_vids.push_back(friends[i]);
            }
        }

        // 批量获取帖子创建时间
        auto post_dates = txn.BatchGetVertexPropsFromVid(
            post_label_id_, post_vids,
            std::vector<std::string>{"creationDate"});

        // 过滤时间范围内的帖子
        std::vector<vid_t> filtered_posts;
        std::vector<vid_t> filtered_creators;
        int64_t endDate = startDate + daysNum * 24LL * 3600 * 1000;
        for (size_t i = 0; i < post_vids.size(); i++) {
            int64_t creationdate = gbp::BufferBlock::Ref<Date>(
                post_dates[i]).milli_second;
            if (creationdate >= startDate && creationdate < endDate) {
                filtered_posts.push_back(post_vids[i]);
                filtered_creators.push_back(creator_vids[i]);
            }
        }

        // 获取帖子的标签
        auto tags = txn.GetOtherVertices(
            post_label_id_, tag_label_id_, hasTag_label_id_,
            filtered_posts, "out");

        // 统计标签出现次数
        std::unordered_map<vid_t, int> tag_counts;
        for (const auto& post_tags : tags) {
            for (const auto& tag_vid : post_tags) {
                tag_counts[tag_vid]++;
            }
        }

        // 转换为vector并排序
        std::vector<std::pair<vid_t, int>> sorted_tags;
        for (const auto& p : tag_counts) {
            sorted_tags.emplace_back(p.first, p.second);
        }

        std::sort(sorted_tags.begin(), sorted_tags.end(),
            [](const auto& a, const auto& b) {
                if (a.second != b.second) {
                    return a.second > b.second;
                }
                return a.first < b.first;
            });

        // 批量获取标签名称
        std::vector<vid_t> tag_vids;
        for (const auto& p : sorted_tags) {
            tag_vids.push_back(p.first);
        }

        auto tag_props = txn.BatchGetVertexPropsFromVid(
            tag_label_id_, tag_vids,
            std::vector<std::string>{"name"});

        // 输出结果
        for (size_t i = 0; i < sorted_tags.size(); i++) {
            output.put_long(txn.GetVertexId(tag_label_id_, sorted_tags[i].first));
            output.put_buffer_object(tag_props[i]); // name
            output.put_int(sorted_tags[i].second);
        }

        return true;
    }

private:
    label_t tag_label_id_;
    label_t person_label_id_;
    label_t post_label_id_;
    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t hasTag_label_id_;

    const DateColumn &post_creationDate_col_;
    const StringColumn &tag_name_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC4 *app = new gs::IC4(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC4 *casted = static_cast<gs::IC4 *>(app);
        delete casted;
    }
} 
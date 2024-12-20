#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include <queue>

namespace gs {

class IC14 : public AppBase {
public:
    IC14(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}

    struct path_info {
        path_info(double weight_, const std::vector<vid_t>& path_)
            : weight(weight_), path(path_) {}
        double weight;
        std::vector<vid_t> path;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req1 = input.get_long();
        oid_t req2 = input.get_long();
        CHECK(input.empty());

        vid_t person1_vid{}, person2_vid{};
        if (!txn.GetVertexIndex(person_label_id_, req1, person1_vid) ||
            !txn.GetVertexIndex(person_label_id_, req2, person2_vid)) {
            return false;
        }

        // 使用Dijkstra算法找到所有路径
        std::vector<path_info> paths;
        if (!FindAllPaths(txn, person1_vid, person2_vid, paths)) {
            output.put_int(0);
            return true;
        }

        // 按权重排序
        std::sort(paths.begin(), paths.end(),
            [](const path_info& a, const path_info& b) {
                if (std::abs(a.weight - b.weight) > 1e-6) {
                    return a.weight > b.weight;
                }
                return a.path.size() < b.path.size();
            });

        // 批量获取路径上所有人的属性
        std::vector<vid_t> all_persons;
        for (const auto& p : paths) {
            all_persons.insert(all_persons.end(), p.path.begin(), p.path.end());
        }

        auto person_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, all_persons,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        output.put_int(paths.size());
        size_t props_offset = 0;
        for (const auto& p : paths) {
            output.put_double(p.weight);
            output.put_int(p.path.size());
            for (size_t i = 0; i < p.path.size(); i++) {
                output.put_long(txn.GetVertexId(person_label_id_, p.path[i]));
                output.put_buffer_object(person_props[props_offset + i * 2]);     // firstName
                output.put_buffer_object(person_props[props_offset + i * 2 + 1]); // lastName
            }
            props_offset += p.path.size() * 2;
        }

        return true;
    }

private:
    bool FindAllPaths(ReadTransaction& txn, vid_t start, vid_t end,
                     std::vector<path_info>& paths) {
        // 获取所有人的帖子数量作为权重
        std::vector<int> post_counts;
        post_counts.resize(txn.GetVertexNum(person_label_id_));

        auto all_posts = txn.GetOtherVertices(
            person_label_id_, post_label_id_, hasCreator_label_id_,
            {start}, "in");

        for (const auto& posts : all_posts) {
            post_counts[start] = posts.size();
        }

        // 使用Dijkstra算法找到所有路径
        std::priority_queue<std::pair<double, std::vector<vid_t>>> pq;
        pq.push({post_counts[start], {start}});

        std::vector<bool> visited;
        visited.resize(txn.GetVertexNum(person_label_id_));

        while (!pq.empty() && paths.size() < 10) {
            auto curr = pq.top();
            pq.pop();

            double weight = curr.first;
            const auto& path = curr.second;
            vid_t u = path.back();

            if (u == end) {
                paths.emplace_back(weight, path);
                continue;
            }

            if (visited[u]) continue;
            visited[u] = true;

            // 获取当前节点的朋友
            auto out_friends = txn.GetOtherVertices(
                person_label_id_, person_label_id_, knows_label_id_,
                {u}, "out");

            auto in_friends = txn.GetOtherVertices(
                person_label_id_, person_label_id_, knows_label_id_,
                {u}, "in");

            // 处理所有朋友
            std::vector<vid_t> friends;
            friends.insert(friends.end(), out_friends[0].begin(), out_friends[0].end());
            friends.insert(friends.end(), in_friends[0].begin(), in_friends[0].end());

            // 获取朋友的帖子数量
            auto friend_posts = txn.GetOtherVertices(
                person_label_id_, post_label_id_, hasCreator_label_id_,
                friends, "in");

            for (size_t i = 0; i < friends.size(); i++) {
                vid_t v = friends[i];
                if (visited[v]) continue;

                post_counts[v] = friend_posts[i].size();
                auto new_path = path;
                new_path.push_back(v);
                double new_weight = weight * post_counts[v];

                pq.push({new_weight, new_path});
            }
        }

        return !paths.empty();
    }

    label_t person_label_id_;
    label_t post_label_id_;
    label_t knows_label_id_;
    label_t hasCreator_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC14 *app = new gs::IC14(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC14 *casted = static_cast<gs::IC14 *>(app);
        delete casted;
    }
} 
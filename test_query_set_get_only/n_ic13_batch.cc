#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC13 : public AppBase {
public:
    IC13(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}

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

        // 使用BFS找到最短路径
        std::vector<vid_t> path;
        if (!FindShortestPath(txn, person1_vid, person2_vid, path)) {
            output.put_int(-1);
            return true;
        }

        // 批量获取路径上所有人的属性
        auto person_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, path,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        output.put_int(path.size() - 1);
        for (size_t i = 0; i < path.size(); i++) {
            output.put_long(txn.GetVertexId(person_label_id_, path[i]));
            output.put_buffer_object(person_props[i * 2]);     // firstName
            output.put_buffer_object(person_props[i * 2 + 1]); // lastName
        }

        return true;
    }

private:
    bool FindShortestPath(ReadTransaction& txn, vid_t start, vid_t end,
                         std::vector<vid_t>& path) {
        std::vector<vid_t> prev;
        prev.resize(txn.GetVertexNum(person_label_id_), vid_t(-1));

        std::vector<vid_t> current_level{start};
        std::vector<vid_t> next_level;

        while (!current_level.empty()) {
            // 批量获取当前层所有人的朋友
            auto out_friends = txn.GetOtherVertices(
                person_label_id_, person_label_id_, knows_label_id_,
                current_level, "out");

            auto in_friends = txn.GetOtherVertices(
                person_label_id_, person_label_id_, knows_label_id_,
                current_level, "in");

            // 处理所有朋友
            for (size_t i = 0; i < current_level.size(); i++) {
                vid_t u = current_level[i];

                // 处理出边朋友
                for (const auto& v : out_friends[i]) {
                    if (prev[v] == vid_t(-1) && v != start) {
                        prev[v] = u;
                        if (v == end) {
                            // 找到目标,构建路径
                            path.clear();
                            vid_t curr = end;
                            while (curr != vid_t(-1)) {
                                path.push_back(curr);
                                curr = prev[curr];
                            }
                            std::reverse(path.begin(), path.end());
                            return true;
                        }
                        next_level.push_back(v);
                    }
                }

                // 处理入边朋友
                for (const auto& v : in_friends[i]) {
                    if (prev[v] == vid_t(-1) && v != start) {
                        prev[v] = u;
                        if (v == end) {
                            // 找到目标,构建路径
                            path.clear();
                            vid_t curr = end;
                            while (curr != vid_t(-1)) {
                                path.push_back(curr);
                                curr = prev[curr];
                            }
                            std::reverse(path.begin(), path.end());
                            return true;
                        }
                        next_level.push_back(v);
                    }
                }
            }

            current_level.swap(next_level);
            next_level.clear();
        }

        return false;
    }

    label_t person_label_id_;
    label_t knows_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC13 *app = new gs::IC13(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC13 *casted = static_cast<gs::IC13 *>(app);
        delete casted;
    }
} 
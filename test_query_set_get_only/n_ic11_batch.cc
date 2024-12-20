#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC11 : public AppBase {
public:
    IC11(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          organisation_label_id_(graph.schema().get_vertex_label_id("ORGANISATION")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          workAt_label_id_(graph.schema().get_edge_label_id("WORKAT")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          organisation_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(organisation_label_id_, "name")))),
          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          place_num_(graph.graph().vertex_num(place_label_id_)),
          graph_(graph) {}

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        oid_t countryName = input.get_long();
        int workfromyear = input.get_int();
        CHECK(input.empty());

        vid_t root{};
        if (!txn.GetVertexIndex(person_label_id_, req, root)) {
            return false;
        }

        // 获取1度和2度好友
        std::vector<vid_t> friends;
        std::vector<bool> visited(txn.GetVertexNum(person_label_id_), false);
        visited[root] = true;

        // 使用 GetOtherVertices 获取1度好友
        auto first_degree_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_, 
            {root}, "both");

        // 标记1度好友
        for (const auto& nbrs : first_degree_friends) {
            for (auto nbr : nbrs) {
                if (!visited[nbr]) {
                    visited[nbr] = true;
                    friends.push_back(nbr);
                }
            }
        }

        // 使用 GetOtherVertices 获取2度好友
        auto second_degree_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_, 
            friends, "both");

        // 标记2度好友
        std::vector<vid_t> friends2;
        for (size_t i = 0; i < friends.size(); i++) {
            for (auto nbr : second_degree_friends[i]) {
                if (!visited[nbr]) {
                    visited[nbr] = true;
                    friends2.push_back(nbr);
                }
            }
        }

        // 合并1度和2度好友
        friends.insert(friends.end(), friends2.begin(), friends2.end());

        vid_t country_id;
        for (vid_t i = 0; i < place_num_; ++i)
        {
            auto place_name_item = place_name_col_.get(i);
            if (place_name_item == countryName)
            {
                country_id = i;
                break;
            }
        }

        // 定义person_info结构体
        struct person_info {
            person_info(vid_t person_vid_, int workfrom_, oid_t person_id_)
                : person_vid(person_vid_),
                  workfrom(workfrom_),
                  person_id(person_id_) {}
            vid_t person_vid;
            int workfrom;
            oid_t person_id;
        };

        // 定义比较器
        struct person_info_comparer {
            bool operator()(const person_info& lhs, const person_info& rhs) {
                if (lhs.workfrom < rhs.workfrom) {
                    return true;
                }
                if (lhs.workfrom > rhs.workfrom) {
                    return false;
                }
                return lhs.person_id < rhs.person_id;
            }
        };

        person_info_comparer comparer;
        std::priority_queue<person_info, std::vector<person_info>, person_info_comparer> pq(comparer);

        // 获取国家下的公司
        auto ie = txn.GetOtherVertices(
            place_label_id_, organisation_label_id_, isLocatedIn_label_id_,
            {country_id}, "in");

        // 获取在这些公司工作的人和工作时长
        auto work_edges = txn.BatchGetEdgeProps<int>(
            person_label_id_, organisation_label_id_, workAt_label_id_,
            ie[0], "in");

        // 处理每个公司的员工
        for (size_t i = 0; i < ie[0].size(); i++) {
            // work_edges[i] 包含了在第i个公司工作的所有人
            for (const auto& [person_vid, wf] : work_edges[i]) {
                // 检查是否是2度好友且工作时间满足要求
                if (visited[person_vid] && wf < workfromyear) {
                    if (pq.size() < 10) {
                        pq.emplace(person_vid, wf, txn.GetVertexId(person_label_id_, person_vid));
                    } else {
                        const auto& top = pq.top();
                        if (wf < top.workfrom) {
                            pq.pop();
                            pq.emplace(person_vid, wf, txn.GetVertexId(person_label_id_, person_vid));
                        } else if (wf == top.workfrom) {
                            oid_t other_person_id = txn.GetVertexId(person_label_id_, person_vid);
                            if (other_person_id < top.person_id) {
                                pq.pop();
                                pq.emplace(person_vid, wf, other_person_id);
                            }
                        }
                    }
                }
            }
        }

        // 收集结果
        std::vector<person_info> results;
        results.reserve(pq.size());
        while (!pq.empty()) {
            results.emplace_back(pq.top());
            pq.pop();
        }

        // 批量获取朋友属性
        std::vector<vid_t> person_vids;
        for (const auto& p : results) {
            person_vids.push_back(p.person_vid);
        }

        auto person_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, person_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t i = results.size(); i > 0; i--) {
            const auto& p = results[i - 1];
            output.put_long(p.person_id);
            output.put_buffer_object(person_props[(i - 1) * 2]);     // firstName
            output.put_buffer_object(person_props[(i - 1) * 2 + 1]); // lastName
            output.put_int(p.workfrom);
        }

        return true;
    }

private:
    label_t person_label_id_;
    label_t place_label_id_;
    label_t organisation_label_id_;
    label_t knows_label_id_;
    label_t workAt_label_id_;
    label_t isLocatedIn_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &organisation_name_col_;
    const StringColumn &place_name_col_;

    vid_t place_num_;
    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC11 *app = new gs::IC11(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC11 *casted = static_cast<gs::IC11 *>(app);
        delete casted;
    }
}

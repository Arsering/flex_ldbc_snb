#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IS3 : public AppBase {
public:
    IS3(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}

    struct person_info {
        person_info(vid_t v_, int64_t creationdate_, oid_t person_id_)
            : v(v_), creationdate(creationdate_), person_id(person_id_) {}
        vid_t v;
        int64_t creationdate;
        oid_t person_id;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        CHECK(input.empty());

        vid_t root{};
        if (!txn.GetVertexIndex(person_label_id_, req, root)) {
            return false;
        }

        // // 批量获取所有朋友关系
        // auto out_friends = txn.GetOtherVertices(
        //     person_label_id_, person_label_id_, knows_label_id_,
        //     {root}, "out");

        // auto in_friends = txn.GetOtherVertices(
        //     person_label_id_, person_label_id_, knows_label_id_,
        //     {root}, "in");

        // 收集所有朋友
        std::vector<person_info> friends;
        
        // 获取朋友关系的创建时间
        auto out_dates = txn.BatchGetEdgeProps<Date>(
            person_label_id_, person_label_id_, knows_label_id_,
            {root}, "out");

        auto in_dates = txn.BatchGetEdgeProps<Date>(
            person_label_id_, person_label_id_, knows_label_id_,
            {root}, "in");

        // 处理出边朋友
        for (size_t i = 0; i < out_dates[0].size(); i++) {
            auto friend_vid = out_dates[0][i].first;
            auto creation_date = out_dates[0][i].second.milli_second;
            friends.emplace_back(friend_vid, creation_date,
                txn.GetVertexId(person_label_id_, friend_vid));
        }

        // 处理入边朋友
        for (size_t i = 0; i < in_dates[0].size(); i++) {
            auto friend_vid = in_dates[0][i].first;
            auto creation_date = in_dates[0][i].second.milli_second;
            friends.emplace_back(friend_vid, creation_date,
                txn.GetVertexId(person_label_id_, friend_vid));
        }

        // 按创建时间排序
        std::sort(friends.begin(), friends.end(),
            [](const person_info& a, const person_info& b) {
                if (a.creationdate != b.creationdate) {
                    return a.creationdate > b.creationdate;
                }
                return a.person_id < b.person_id;
            });

        // 批量获取朋友的属性
        std::vector<vid_t> friend_vids;
        for (const auto& f : friends) {
            friend_vids.push_back(f.v);
        }

        auto friend_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, friend_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t i = 0; i < friends.size(); i++) {
            const auto& f = friends[i];
            output.put_long(f.person_id);
            output.put_long(f.creationdate);
            output.put_buffer_object(friend_props[i * 2]);     // firstName
            output.put_buffer_object(friend_props[i * 2 + 1]); // lastName
        }

        return true;
    }

private:
    label_t person_label_id_;
    label_t knows_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IS3 *app = new gs::IS3(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IS3 *casted = static_cast<gs::IS3 *>(app);
        delete casted;
    }
} 
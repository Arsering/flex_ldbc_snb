#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC10 : public AppBase {
public:
    IC10(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")), 
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          hasInterest_label_id_(graph.schema().get_edge_label_id("HASINTEREST")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          person_birthday_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(person_label_id_, "birthday")))),
          person_gender_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "gender")))),
          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          graph_(graph) {}

    struct person_info {
        person_info(vid_t v_, oid_t person_id_, int score_)
            : v(v_), person_id(person_id_), score(score_) {}
        vid_t v;
        oid_t person_id;
        int score;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        int month = input.get_int();
        CHECK(input.empty());

        vid_t root{};
        if (!txn.GetVertexIndex(person_label_id_, req, root)) {
            return false;
        }

        // 获取1度和2度好友
        std::vector<vid_t> friends_1d;
        std::vector<bool> friends_set(txn.GetVertexNum(person_label_id_), false);
        friends_set[root] = true;

        // 获取1度好友
        auto out_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {root}, "out");

        auto in_friends = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            {root}, "in");

        // 收集1度好友
        for (const auto& v : out_friends[0]) {
            friends_1d.push_back(v);
            friends_set[v] = true;
        }
        for (const auto& v : in_friends[0]) {
            friends_1d.push_back(v);
            friends_set[v] = true;
        }

        // 获取2度好友
        std::vector<vid_t> friends_2d;
        auto friends_1d_out = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            friends_1d, "out");

        auto friends_1d_in = txn.GetOtherVertices(
            person_label_id_, person_label_id_, knows_label_id_,
            friends_1d, "in");

        // 先收集所有潜在的2度好友
        std::vector<vid_t> potential_friends_2d;
        for (const auto& friends : friends_1d_out) {
            for (const auto& v : friends) {
                if (!friends_set[v]) {
                    potential_friends_2d.push_back(v);
                    friends_set[v] = true;
                }
            }
        }
        for (const auto& friends : friends_1d_in) {
            for (const auto& v : friends) {
                if (!friends_set[v]) {
                    potential_friends_2d.push_back(v);
                    friends_set[v] = true;
                }
            }
        }

        // 获取2度好友的生日并筛选
        auto birthday_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, potential_friends_2d,
            std::vector<std::string>{"birthday"});

        for (size_t i = 0; i < potential_friends_2d.size(); i++) {
            auto t = gbp::BufferBlock::Ref<Date>(birthday_props[i]).milli_second / 1000;
            auto tm = gmtime((time_t *)(&t));
            if ((tm->tm_mon + 1 == month && tm->tm_mday >= 21) ||
                ((month <= 11 && tm->tm_mon == month && tm->tm_mday < 22) ||
                 (tm->tm_mon == 0 && month == 12 && tm->tm_mday < 22))) {
                friends_2d.push_back(potential_friends_2d[i]);
            }
        }

        // 获取朋友创建的帖子和对应的标签
        auto posts = txn.GetOtherVertices(
            person_label_id_, post_label_id_, hasCreator_label_id_,
            friends_2d, "in");

        // 为每个人构建posts和tags的对应关系
        std::vector<std::vector<vid_t>> friend_posts(friends_2d.size());  // 每个人的posts
        std::vector<std::vector<std::vector<vid_t>>> friend_post_tags(friends_2d.size());  // 每个人的每个post的tags
        
        // 获取所有post的tags
        std::vector<vid_t> all_posts;
        for (size_t i = 0; i < friends_2d.size(); i++) {
            friend_posts[i] = posts[i];  // 直接保存每个人的posts
            all_posts.insert(all_posts.end(), posts[i].begin(), posts[i].end());
        }

        auto post_tags = txn.GetOtherVertices(
            post_label_id_, tag_label_id_, hasTag_label_id_,
            all_posts, "out");

        // 构建每个post的tags映射
        size_t post_idx = 0;
        for (size_t i = 0; i < friends_2d.size(); i++) {
            friend_post_tags[i].resize(friend_posts[i].size());  // 为每个post预分配tags空间
            for (size_t j = 0; j < friend_posts[i].size(); j++) {
                friend_post_tags[i][j] = post_tags[post_idx++];  // 保存每个post的tags
            }
        }

        // 获取此人感兴趣的标签
        auto interests = txn.GetOtherVertices(
            person_label_id_, tag_label_id_, hasInterest_label_id_,
            {root}, "out");

        // 标记感兴趣的标签
        std::vector<bool> hasInterests(txn.GetVertexNum(tag_label_id_));
        for (const auto& tag_vid : interests[0]) {
            hasInterests[tag_vid] = true;
        }

        // 定义优先队列的比较器
        struct person_info_comparer {
            bool operator()(const person_info& lhs, const person_info& rhs) {
                if (lhs.score != rhs.score) {
                    return lhs.score < rhs.score;  // 分数高的优先
                }
                return lhs.person_id > rhs.person_id;  // 分数相同时ID小的优先
            }
        };

        // 使用优先队列存储前10个结果
        std::priority_queue<person_info, std::vector<person_info>, person_info_comparer> pq;

        // 计算每个朋友的得分并维护前10
        for (size_t i = 0; i < friends_2d.size(); i++) {
            int score = 0;

            // 统计帖子得分
            for (size_t j = 0; j < friend_posts[i].size(); j++) {
                bool has_common_tag = false;
                for (const auto& tag_vid : friend_post_tags[i][j]) {
                    if (hasInterests[tag_vid]) {
                        has_common_tag = true;
                        break;
                    }
                }
                score += (has_common_tag ? 1 : -1);
            }

            // 维护前10
            if (pq.size() < 10) {
                pq.emplace(friends_2d[i],
                    txn.GetVertexId(person_label_id_, friends_2d[i]),
                    score);
            } else {
                const auto& top = pq.top();
                if (score > top.score || (score == top.score && 
                    txn.GetVertexId(person_label_id_, friends_2d[i]) < top.person_id)) {
                    pq.pop();
                    pq.emplace(friends_2d[i],
                        txn.GetVertexId(person_label_id_, friends_2d[i]),
                        score);
                }
            }
        }

        // 收集结果
        std::vector<person_info> person_list;
        person_list.reserve(pq.size());
        while (!pq.empty()) {
            person_list.emplace_back(pq.top());
            pq.pop();
        }
        std::reverse(person_list.begin(), person_list.end());  // 反转以得到正确顺序

        // 批量获取属性
        std::vector<vid_t> person_vids;
        for (const auto& p : person_list) {
            person_vids.push_back(p.v);
        }

        auto person_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, person_vids,
            std::vector<std::string>{"firstName", "lastName", "gender"});

        auto place_vids = txn.GetOtherVertices(
            person_label_id_, place_label_id_, isLocatedIn_label_id_,
            person_vids, "out");

        std::vector<vid_t> all_place_vids;
        for (const auto& v : place_vids) {
            assert(v.size() == 1);
            all_place_vids.push_back(v[0]);
        }
        auto place_props = txn.BatchGetVertexPropsFromVid(
            place_label_id_, all_place_vids,
            std::vector<std::string>{"name"});
        
        // 输出结果
        for (size_t i = 0; i < person_list.size(); i++) {
            const auto& p = person_list[i];
            output.put_long(p.person_id);
            output.put_buffer_object(person_props[i * 3]);     // firstName
            output.put_buffer_object(person_props[i * 3 + 1]); // lastName
            output.put_int(p.score);
            output.put_buffer_object(person_props[i * 3 + 2]); // gender
            output.put_buffer_object(place_props[i]);          // place name
        }

        return true;
    }

private:
    label_t person_label_id_;
    label_t place_label_id_;
    label_t post_label_id_;
    label_t tag_label_id_;
    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t hasTag_label_id_;
    label_t hasInterest_label_id_;
    label_t isLocatedIn_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const DateColumn &person_birthday_col_;
    const StringColumn &person_gender_col_;
    const StringColumn &place_name_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC10 *app = new gs::IC10(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC10 *casted = static_cast<gs::IC10 *>(app);
        delete casted;
    }
} 
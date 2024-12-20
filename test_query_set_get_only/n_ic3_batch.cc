#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC3 : public AppBase {
public:
    IC3(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          isPartOf_label_id_(graph.schema().get_edge_label_id("ISPARTOF")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          graph_(graph) {}

    struct message_info {
        message_info(bool is_post_, vid_t v_, oid_t messageid_, 
                    vid_t creator_vid_, int64_t creationdate_)
            : is_post(is_post_), v(v_), messageid(messageid_),
              creator_vid(creator_vid_), creationdate(creationdate_) {}
        bool is_post;
        vid_t v;
        oid_t messageid;
        vid_t creator_vid;
        int64_t creationdate;
    };

    bool Query(Decoder &input, Encoder &output) override {
        auto txn = graph_.GetReadTransaction();

        oid_t req = input.get_long();
        oid_t countryX = input.get_long();
        oid_t countryY = input.get_long();
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

        // 获取朋友所在的地点
        auto friend_places = txn.GetOtherVertices(
            person_label_id_, place_label_id_, isLocatedIn_label_id_,
            friends, "out");

        // 获取地点所在的国家
        std::vector<vid_t> place_vids;
        for (const auto& places : friend_places) {
            CHECK(places.size() == 1) << "Person should have exactly one location";
            place_vids.push_back(places[0]);
        }

        auto countries = txn.GetOtherVertices(
            place_label_id_, place_label_id_, isPartOf_label_id_,
            place_vids, "out");

        // 标记符合条件的朋友
        std::vector<bool> valid_friends(friends.size());
        for (size_t i = 0; i < friends.size(); i++) {
            CHECK(countries[i].size() == 1) << "Place should belong to exactly one country";
            vid_t country_vid = countries[i][0];
            oid_t country_id = txn.GetVertexId(place_label_id_, country_vid);
            valid_friends[i] = (country_id == countryX || country_id == countryY);
        }

        // 获取符合条件的朋友创建的消息
        std::vector<vid_t> valid_friend_vids;
        for (size_t i = 0; i < friends.size(); i++) {
            if (valid_friends[i]) {
                valid_friend_vids.push_back(friends[i]);
            }
        }

        auto posts = txn.GetOtherVertices(
            person_label_id_, post_label_id_, hasCreator_label_id_,
            valid_friend_vids, "in");

        auto comments = txn.GetOtherVertices(
            person_label_id_, comment_label_id_, hasCreator_label_id_,
            valid_friend_vids, "in");

        // 收集所有消息
        std::vector<message_info> messages;
        std::vector<vid_t> post_vids;
        std::vector<vid_t> comment_vids;

        // 处理帖子
        for (size_t i = 0; i < valid_friend_vids.size(); i++) {
            for (const auto& post_vid : posts[i]) {
                post_vids.push_back(post_vid);
                messages.emplace_back(true, post_vid,
                    txn.GetVertexId(post_label_id_, post_vid),
                    valid_friend_vids[i], 0);
            }
        }

        // 处理评论
        for (size_t i = 0; i < valid_friend_vids.size(); i++) {
            for (const auto& comment_vid : comments[i]) {
                comment_vids.push_back(comment_vid);
                messages.emplace_back(false, comment_vid,
                    txn.GetVertexId(comment_label_id_, comment_vid),
                    valid_friend_vids[i], 0);
            }
        }

        // 批量获取消息创建时间
        auto post_dates = txn.BatchGetVertexPropsFromVid(
            post_label_id_, post_vids,
            std::vector<std::string>{"creationDate"});

        auto comment_dates = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, comment_vids,
            std::vector<std::string>{"creationDate"});

        // 更新创建时间并过滤
        size_t post_idx = 0, comment_idx = 0;
        std::vector<message_info> filtered_messages;
        int64_t endDate = startDate + daysNum * 24LL * 3600 * 1000;
        for (auto& msg : messages) {
            int64_t creationdate;
            if (msg.is_post) {
                creationdate = gbp::BufferBlock::Ref<Date>(
                    post_dates[post_idx++]).milli_second;
            } else {
                creationdate = gbp::BufferBlock::Ref<Date>(
                    comment_dates[comment_idx++]).milli_second;
            }
            if (creationdate >= startDate && creationdate < endDate) {
                msg.creationdate = creationdate;
                filtered_messages.push_back(msg);
            }
        }

        // 按创建时间排序
        std::sort(filtered_messages.begin(), filtered_messages.end(),
            [](const message_info& a, const message_info& b) {
                if (a.creationdate != b.creationdate) {
                    return a.creationdate < b.creationdate;
                }
                return a.messageid < b.messageid;
            });

        // 批量获取创建者属性
        std::vector<vid_t> creator_vids;
        for (const auto& msg : filtered_messages) {
            creator_vids.push_back(msg.creator_vid);
        }

        auto creator_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, creator_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t i = 0; i < filtered_messages.size(); i++) {
            const auto& msg = filtered_messages[i];
            output.put_long(msg.messageid);
            output.put_long(msg.creationdate);
            output.put_long(txn.GetVertexId(person_label_id_, msg.creator_vid));
            output.put_buffer_object(creator_props[i * 2]);     // firstName
            output.put_buffer_object(creator_props[i * 2 + 1]); // lastName
        }

        return true;
    }

private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t place_label_id_;
    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t isLocatedIn_label_id_;
    label_t isPartOf_label_id_;

    const DateColumn &post_creationDate_col_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &place_name_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
    void *CreateApp(gs::GraphDBSession &db) {
        gs::IC3 *app = new gs::IC3(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app) {
        gs::IC3 *casted = static_cast<gs::IC3 *>(app);
        delete casted;
    }
} 
#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

// #define ZED_PROFILE

namespace gs
{

  class IC9 : public AppBase
  {
  public:
    IC9(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),
          post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
          post_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
              graph.get_vertex_property_column(post_label_id_, "length")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          graph_(graph) {}
    ~IC9() {}

    struct message_info
    {
      message_info(vid_t message_vid_, vid_t other_person_vid_, oid_t message_id_,
                   int64_t creation_date_, bool is_comment_)
          : message_vid(message_vid_),
            other_person_vid(other_person_vid_),
            message_id(message_id_),
            creation_date(creation_date_),
            is_comment(is_comment_) {}

      vid_t message_vid;
      vid_t other_person_vid;
      oid_t message_id;
      int64_t creation_date;
      bool is_comment;
    };

    struct message_info_comparer
    {
      bool operator()(const message_info &lhs, const message_info &rhs)
      {
        if (lhs.creation_date > rhs.creation_date)
        {
          return true;
        }
        if (lhs.creation_date < rhs.creation_date)
        {
          return false;
        }
        return lhs.message_id < rhs.message_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
        auto txn = graph_.GetReadTransaction();

        oid_t personid = input.get_long();
        int64_t maxdate = input.get_long();
        CHECK(input.empty());

        vid_t root{};
        if (!txn.GetVertexIndex(person_label_id_, personid, root)) {
            return false;
        }

        message_info_comparer cmp;
        std::priority_queue<message_info, std::vector<message_info>, message_info_comparer> que(cmp);

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

        // 合并所有好友
        friends.insert(friends.end(), friends2.begin(), friends2.end());

        // 使用 GetOtherVertices 获取所有帖子和评论
        auto posts = txn.GetOtherVertices(
            person_label_id_, post_label_id_, hasCreator_label_id_,
            friends, "in");
        auto comments = txn.GetOtherVertices(
            person_label_id_, comment_label_id_, hasCreator_label_id_,
            friends, "in");

        // 收集所有的 post 和 comment vid，同时记录对应的 friend 索引
        std::vector<vid_t> post_vids;
        std::vector<vid_t> comment_vids;
        std::vector<size_t> post_friend_indices;
        std::vector<size_t> comment_friend_indices;

        for (size_t i = 0; i < friends.size(); i++) {
            for (auto post_vid : posts[i]) {
                post_vids.push_back(post_vid);
                post_friend_indices.push_back(i);
            }
            
            for (auto comment_vid : comments[i]) {
                comment_vids.push_back(comment_vid);
                comment_friend_indices.push_back(i);
            }
        }

        // 批量获取创建时间
        auto post_dates = txn.BatchGetVertexPropsFromVid(
            post_label_id_, post_vids,
            std::vector<std::string>{"creationDate"});

        auto comment_dates = txn.BatchGetVertexPropsFromVid(
            comment_label_id_, comment_vids,
            std::vector<std::string>{"creationDate"});
        // 处理帖子
        for (size_t i = 0; i < post_vids.size(); i++) {
            auto creation_date = gbp::BufferBlock::Ref<Date>(post_dates[i]).milli_second;
            if (creation_date < maxdate) {
                auto u = post_vids[i];
                auto friend_idx = post_friend_indices[i];
                if (que.size() < 20) {
                    oid_t u_id = txn.GetVertexId(post_label_id_, u);
                    que.emplace(u, friends[friend_idx], u_id, creation_date, false);
                } else {
                    const auto& top = que.top();
                    if (creation_date > top.creation_date) {
                        que.pop();
                        oid_t u_id = txn.GetVertexId(post_label_id_, u);
                        que.emplace(u, friends[friend_idx], u_id, creation_date, false);
                    } else if (creation_date == top.creation_date) {
                        oid_t u_id = txn.GetVertexId(post_label_id_, u);
                        if (u_id < top.message_id) {
                            que.pop();
                            que.emplace(u, friends[friend_idx], u_id, creation_date, false);
                        }
                    }
                }
            }
        }

        // 处理评论
        for (size_t i = 0; i < comment_vids.size(); i++) {
            auto creation_date = gbp::BufferBlock::Ref<Date>(comment_dates[i]).milli_second;
            if (creation_date < maxdate) {
                auto u = comment_vids[i];
                auto friend_idx = comment_friend_indices[i];
                if (que.size() < 20) {
                    oid_t u_id = txn.GetVertexId(comment_label_id_, u);
                    que.emplace(u, friends[friend_idx], u_id, creation_date, true);
                } else {
                    const auto& top = que.top();
                    if (creation_date > top.creation_date) {
                        que.pop();
                        oid_t u_id = txn.GetVertexId(comment_label_id_, u);
                        que.emplace(u, friends[friend_idx], u_id, creation_date, true);
                    } else if (creation_date == top.creation_date) {
                        oid_t u_id = txn.GetVertexId(comment_label_id_, u);
                        if (u_id < top.message_id) {
                            que.pop();
                            que.emplace(u, friends[friend_idx], u_id, creation_date, true);
                        }
                    }
                }
            }
        }

        // 输出结果
        std::vector<message_info> vec;
        vec.reserve(que.size());
        while (!que.empty()) {
            vec.emplace_back(que.top());
            que.pop();
        }

        // 修改获取 person 属性的部分
        std::vector<vid_t> person_vids;
        for (const auto& v : vec) {
            person_vids.push_back(v.other_person_vid);
        }

        // 获取所有属性值
        auto person_props = txn.BatchGetVertexPropsFromVid(
            person_label_id_, person_vids,
            std::vector<std::string>{"firstName", "lastName"});

        // 输出结果
        for (size_t idx = vec.size(); idx > 0; --idx) {
            auto& v = vec[idx - 1];
            vid_t person_lid = v.other_person_vid;
            
            output.put_long(txn.GetVertexId(person_label_id_, person_lid));
            
            // 每个人有两个属性,所以需要取两个连续的 BufferBlock
            size_t prop_idx = (idx - 1) * 2;  // 获取当前 person 属性的起始位置
            output.put_buffer_object(person_props[prop_idx]);     // firstName
            output.put_buffer_object(person_props[prop_idx + 1]); // lastName

            output.put_long(v.message_id);

            if (v.is_comment) {
                auto content = comment_content_col_.get(v.message_vid);
                output.put_buffer_object(content);
            } else {
                vid_t post_lid = v.message_vid;
                auto item = post_length_col_.get(post_lid);
                auto post_content = 
                    gbp::BufferBlock::Ref<int>(item) == 0 ? post_imageFile_col_.get(post_lid) 
                        : post_content_col_.get(post_lid);
                output.put_buffer_object(post_content);
            }
            output.put_long(v.creation_date);
        }

        return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t hasCreator_label_id_;
    label_t knows_label_id_;

    const StringColumn &post_content_col_;
    const StringColumn &post_imageFile_col_;
    const IntColumn &post_length_col_;
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &comment_content_col_;
    const DateColumn &comment_creationDate_col_;
    const DateColumn &post_creationDate_col_;

    #ifdef ZED_PROFILE
    int comment_count=0;
    int person_count=0;
    #endif

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC9 *app = new gs::IC9(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC9 *casted = static_cast<gs::IC9 *>(app);
    delete casted;
  }
}

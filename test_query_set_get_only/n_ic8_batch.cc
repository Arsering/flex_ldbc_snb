#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{
  class IC8 : public AppBase
  {
  public:
    IC8(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          graph_(graph) {}

    ~IC8() {}

    struct comment_info
    {
      comment_info(vid_t comment_vid_, int64_t creationDate_, oid_t comment_id_)
          : comment_vid(comment_vid_),
            creationDate(creationDate_),
            comment_id(comment_id_) {}

      vid_t comment_vid;
      int64_t creationDate;
      oid_t comment_id;
    };

    struct comment_info_comparer
    {
      bool operator()(const comment_info &lhs, const comment_info &rhs)
      {
        if (lhs.creationDate > rhs.creationDate)
        {
          return true;
        }
        if (lhs.creationDate < rhs.creationDate)
        {
          return false;
        }
        return lhs.comment_id < rhs.comment_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }

      comment_info_comparer comparer;
      std::priority_queue<comment_info, std::vector<comment_info>,
                          comment_info_comparer> pq(comparer);

      // 收集所有的评论
      std::vector<vid_t> all_comments;

      // 收集帖子的回复
      auto posts = txn.GetOtherVertices(
          person_label_id_, post_label_id_, hasCreator_label_id_,
          {root}, "in");

      // 收集所有帖子
      std::vector<vid_t> all_posts;
      for (const auto& post_list : posts) {
          for (auto post_vid : post_list) {
              message_count++;
              all_posts.push_back(post_vid);
          }
      }

      // 批量获取帖子的回复
      auto post_comments = txn.GetOtherVertices(
          post_label_id_, comment_label_id_, replyOf_label_id_,
          all_posts, "in");

      // 收集帖子的回复
      for (const auto& comment_list : post_comments) {
          for (auto comment_vid : comment_list) {
              reply_count++;
              all_comments.push_back(comment_vid);
          }
      }

      // 收集评论的回复
      auto person_comments = txn.GetOtherVertices(
          person_label_id_, comment_label_id_, hasCreator_label_id_,
          {root}, "in");

      // 收集所有评论
      std::vector<vid_t> person_comment_vids;
      for (const auto& comment_list : person_comments) {
          for (auto comment_vid : comment_list) {
              message_count++;
              person_comment_vids.push_back(comment_vid);
          }
      }

      // 批量获取评论的回复
      auto comment_replies = txn.GetOtherVertices(
          comment_label_id_, comment_label_id_, replyOf_label_id_,
          person_comment_vids, "in");

      // 收集评论的回复
      for (const auto& reply_list : comment_replies) {
          for (auto reply_vid : reply_list) {
              reply_count++;
              all_comments.push_back(reply_vid);
          }
      }

      // 批量获取所有评论的创建时间
      auto comment_dates = txn.BatchGetVertexPropsFromVid(
          comment_label_id_, all_comments,
          std::vector<std::string>{"creationDate"});

      // 处理所有评论
      for (size_t i = 0; i < all_comments.size(); i++) {
          auto comment_vid = all_comments[i];
          auto creation_date = gbp::BufferBlock::Ref<Date>(comment_dates[i]).milli_second;
          
          if (pq.size() < 20) {
              pq.emplace(comment_vid, creation_date,
                      txn.GetVertexId(comment_label_id_, comment_vid));
          } else {
              const auto& top = pq.top();
              if (creation_date > top.creationDate) {
                  pq.pop();
                  pq.emplace(comment_vid, creation_date,
                          txn.GetVertexId(comment_label_id_, comment_vid));
              } else if (creation_date == top.creationDate) {
                  oid_t comment_id = txn.GetVertexId(comment_label_id_, comment_vid);
                  if (comment_id < top.comment_id) {
                      pq.pop();
                      pq.emplace(comment_vid, creation_date, comment_id);
                  }
              }
          }
      }

      std::vector<comment_info> vec;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        pq.pop();
      }

      // 收集所有评论的创建者
      std::vector<vid_t> comment_vids;
      std::vector<vid_t> creator_vids;
      comment_vids.reserve(vec.size());

      // 从 vec 中收集评论的 vid
      for (const auto& v : vec) {
          comment_vids.push_back(v.comment_vid);
      }

      // 使用 GetOtherVertices 批量获取创建者
      auto creators = txn.GetOtherVertices(
          comment_label_id_, person_label_id_, hasCreator_label_id_,
          comment_vids, "out");

      // 收集所有创建者的 vid
      for (size_t i = 0; i < vec.size(); i++) {
          auto& creator_list = creators[i];
          CHECK(creator_list.size() == 1) << "Comment should have exactly one creator";
          creator_vids.push_back(creator_list[0]);
      }

      // 批量获取创建者的属性
      auto person_props = txn.BatchGetVertexPropsFromVid(
          person_label_id_, creator_vids,
          std::vector<std::string>{"firstName", "lastName"});

      // 批量获取评论的内容
      auto comment_contents = txn.BatchGetVertexPropsFromVid(
          comment_label_id_, comment_vids,
          std::vector<std::string>{"content"});

      // 输出结果
      for (size_t i = vec.size(); i > 0; i--) {
          size_t idx = i - 1;
          auto& v = vec[idx];
          
          // 输出创建者ID
          output.put_long(txn.GetVertexId(person_label_id_, creator_vids[idx]));
          
          // 输出创建者属性
          output.put_buffer_object(person_props[idx * 2]);     // firstName
          output.put_buffer_object(person_props[idx * 2 + 1]); // lastName
          
          // 输出评论信息
          output.put_long(v.creationDate);
          output.put_long(v.comment_id);
          output.put_buffer_object(comment_contents[idx]);
      }

      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;

    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    const StringColumn &comment_content_col_;
    const DateColumn &comment_creationDate_col_;

    GraphDBSession &graph_;
    int reply_count = 0;
    int message_count=0;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC8 *app = new gs::IC8(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC8 *casted = static_cast<gs::IC8 *>(app);
    delete casted;
  }
}

#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class IC2 : public AppBase
  {
  public:
    IC2(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          post_creationDate_col_(graph.GetPropertyHandle(post_label_id_, "creationDate")),
          comment_creationDate_col_(graph.GetPropertyHandle(comment_label_id_, "creationDate")),
          person_firstName_col_(graph.GetPropertyHandle(person_label_id_, "firstName")),
          person_lastName_col_(graph.GetPropertyHandle(person_label_id_, "lastName")),
          post_content_col_(graph.GetPropertyHandle(post_label_id_, "content")),
          post_imageFile_col_(graph.GetPropertyHandle(post_label_id_, "imageFile")),
          post_length_col_(graph.GetPropertyHandle(post_label_id_, "length")),
          comment_content_col_(graph.GetPropertyHandle(comment_label_id_, "content")),
          graph_(graph)
    {
    }
    ~IC2() {}

    struct message_info
    {
      message_info(vid_t message_vid_, vid_t person_vid_, oid_t message_id_,
                   int64_t creationdate_, bool is_post_)
          : message_vid(message_vid_),
            person_vid(person_vid_),
            message_id(message_id_),
            creationdate(creationdate_),
            is_post(is_post_) {}

      vid_t message_vid;
      vid_t person_vid;
      oid_t message_id;
      int64_t creationdate;
      bool is_post;
    };

    struct message_info_comparer
    {
      bool operator()(const message_info &lhs, const message_info &rhs)
      {
        if (lhs.creationdate > rhs.creationdate)
        {
          return true;
        }
        if (lhs.creationdate < rhs.creationdate)
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
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      message_info_comparer comparer;
      std::priority_queue<message_info, std::vector<message_info>,
                          message_info_comparer>
          pq(comparer);

      int64_t current_date = 0;

      auto post_hasCreator_person_in = txn.GetIncomingGraphView<grape::EmptyType>(
          person_label_id_, post_label_id_, hasCreator_label_id_);
      auto comment_hasCreator_person_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              person_label_id_, comment_label_id_, hasCreator_label_id_);
      auto ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; ie.is_valid(); ie.next())
      {
        auto v = ie.get_neighbor();
        auto v_oid = txn.GetVertexId(person_label_id_, v);
        auto posts = post_hasCreator_person_in.get_edges(v);
        for (; posts.is_valid(); posts.next())
        {
          auto pid = posts.get_neighbor();
          auto item = post_creationDate_col_.getProperty(pid);
          auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second;
          if (creationDate < maxdate)
          {
            if (pq.size() < 20)
            {
              pq.emplace(pid, v, txn.GetVertexId(post_label_id_, pid),
                         creationDate, true);
              current_date = pq.top().creationdate;
            }
            else if (creationDate > current_date)
            {
              auto message_id = txn.GetVertexId(post_label_id_, pid);
              pq.pop();
              pq.emplace(pid, v, message_id, creationDate, true);
              current_date = pq.top().creationdate;
            }
            else if (creationDate == current_date)
            {
              auto message_id = txn.GetVertexId(post_label_id_, pid);
              if (message_id < pq.top().message_id)
              {
                pq.pop();
                pq.emplace(pid, v, message_id, creationDate, true);
              }
            }
          }
        }
        auto comments = comment_hasCreator_person_in.get_edges(v);
        for (; comments.is_valid(); comments.next())
        {
          auto cid = comments.get_neighbor();
          auto item = comment_creationDate_col_.getProperty(cid);
          auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second;
          if (creationDate < maxdate)
          {
            if (pq.size() < 20)
            {
              pq.emplace(cid, v, txn.GetVertexId(comment_label_id_, cid),
                         creationDate, false);
              current_date = pq.top().creationdate;
            }
            else if (creationDate > current_date)
            {
              auto message_id = txn.GetVertexId(comment_label_id_, cid);
              pq.pop();
              pq.emplace(cid, v, message_id, creationDate, false);
              current_date = pq.top().creationdate;
            }
            else if (creationDate == current_date)
            {
              auto message_id = txn.GetVertexId(comment_label_id_, cid);
              if (message_id < pq.top().message_id)
              {
                pq.pop();
                pq.emplace(cid, v, message_id, creationDate, false);
              }
            }
          }
        }
      }

      auto oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; oe.is_valid(); oe.next())
      {
        auto v = oe.get_neighbor();
        auto v_oid = txn.GetVertexId(person_label_id_, v);
        auto posts = post_hasCreator_person_in.get_edges(v);

        for (; posts.is_valid(); posts.next())
        {
          auto pid = posts.get_neighbor();
          auto item = post_creationDate_col_.getProperty(pid);
          auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second;
          if (creationDate < maxdate)
          {
            if (pq.size() < 20)
            {
              pq.emplace(pid, v, txn.GetVertexId(post_label_id_, pid),
                         creationDate, true);
              current_date = pq.top().creationdate;
            }
            else if (creationDate > current_date)
            {
              auto message_id = txn.GetVertexId(post_label_id_, pid);
              pq.pop();
              pq.emplace(pid, v, message_id, creationDate, true);
              current_date = pq.top().creationdate;
            }
            else if (creationDate == current_date)
            {
              auto message_id = txn.GetVertexId(post_label_id_, pid);
              if (message_id < pq.top().message_id)
              {
                pq.pop();
                pq.emplace(pid, v, message_id, creationDate, true);
              }
            }
          }
        }
        auto comments = comment_hasCreator_person_in.get_edges(v);
        for (; comments.is_valid(); comments.next())
        {
          auto cid = comments.get_neighbor();
          auto item = comment_creationDate_col_.getProperty(cid);
          auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second;
          if (creationDate < maxdate)
          {
            if (pq.size() < 20)
            {
              pq.emplace(cid, v, txn.GetVertexId(comment_label_id_, cid),
                         creationDate, false);
              current_date = pq.top().creationdate;
            }
            else if (creationDate > current_date)
            {
              auto message_id = txn.GetVertexId(comment_label_id_, cid);
              pq.pop();
              pq.emplace(cid, v, message_id, creationDate, false);
              current_date = pq.top().creationdate;
            }
            else if (creationDate == current_date)
            {
              auto message_id = txn.GetVertexId(comment_label_id_, cid);
              if (message_id < pq.top().message_id)
              {
                pq.pop();
                pq.emplace(cid, v, message_id, creationDate, false);
              }
            }
          }
        }
      }

      std::vector<message_info> vec;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        pq.pop();
      }
      for (size_t i = vec.size(); i > 0; i--)
      {
        auto &v = vec[i - 1];
        output.put_long(txn.GetVertexId(person_label_id_, v.person_vid));
        auto item = person_firstName_col_.getProperty(v.person_vid);
        output.put_buffer_object(item);
        item = person_lastName_col_.getProperty(v.person_vid);
        output.put_buffer_object(item);
        item.free();

        output.put_long(v.message_id);
        if (!v.is_post)
        {
          auto content = comment_content_col_.getProperty(v.message_vid);
          output.put_buffer_object(content);
        }
        else
        {
          auto item = post_length_col_.getProperty(v.message_vid);
          auto content = gbp::BufferBlock::RefSingle<int>(item) == 0 ? post_imageFile_col_.getProperty(v.message_vid) : post_content_col_.getProperty(v.message_vid);
          output.put_buffer_object(content);
        }
        output.put_long(v.creationdate);
      }
      return true;
    }

  private:
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t person_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;

    cgraph::PropertyHandle post_creationDate_col_;
    cgraph::PropertyHandle comment_creationDate_col_;
    cgraph::PropertyHandle person_firstName_col_;
    cgraph::PropertyHandle person_lastName_col_;
    cgraph::PropertyHandle post_content_col_;
    cgraph::PropertyHandle post_imageFile_col_;
    cgraph::PropertyHandle post_length_col_;
    cgraph::PropertyHandle comment_content_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC2 *app = new gs::IC2(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC2 *casted = static_cast<gs::IC2 *>(app);
    delete casted;
  }
}

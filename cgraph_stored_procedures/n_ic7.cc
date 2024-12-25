#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class IC7 : public AppBase
  {
  public:
    IC7(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          likes_label_id_(graph.schema().get_edge_label_id("LIKES")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          person_firstName_col_idx_(graph.get_vertex_property_column_id(person_label_id_, "firstName")),
          person_lastName_col_idx_(graph.get_vertex_property_column_id(person_label_id_, "lastName")),
          post_content_col_idx_(graph.get_vertex_property_column_id(post_label_id_, "content")),
          post_imageFile_col_idx_(graph.get_vertex_property_column_id(post_label_id_, "imageFile")),
          post_length_col_idx_(graph.get_vertex_property_column_id(post_label_id_, "length")),
          comment_content_col_idx_(graph.get_vertex_property_column_id(comment_label_id_, "content")),
          post_creationDate_col_idx_(graph.get_vertex_property_column_id(post_label_id_, "creationDate")),
          comment_creationDate_col_idx_(graph.get_vertex_property_column_id(comment_label_id_, "creationDate")),
          graph_(graph) {}
    ~IC7() {}

    struct person_info
    {
      person_info(vid_t v, vid_t mid, int64_t creationDate, oid_t person_id,
                  oid_t message_id, bool is_post)
          : v(v),
            mid(mid),
            creationDate(creationDate),
            person_id(person_id),
            message_id(message_id),
            is_post(is_post) {}

      vid_t v;
      vid_t mid;
      int64_t creationDate;
      oid_t person_id;
      oid_t message_id;
      bool is_post;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.creationDate > rhs.creationDate)
        {
          return true;
        }
        if (lhs.creationDate < rhs.creationDate)
        {
          return false;
        }
        return lhs.person_id < rhs.person_id;
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
      auto person_num = txn.GetVertexNum(person_label_id_);
      friends_.clear();
      friends_.resize(person_num);
#if OV
      const auto &ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (auto &e : ie)
      {
        friends_[e.neighbor] = true;
      }
      const auto &oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (auto &e : oe)
      {
        friends_[e.neighbor] = true;
      }
#else
      auto ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; ie.is_valid(); ie.next())
      {
        friends_[ie.get_neighbor()] = true;
      }
      auto oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; oe.is_valid(); oe.next())
      {
        friends_[oe.get_neighbor()] = true;
      }
#endif
      std::vector<person_info> vec;

      auto person_likes_post_in = txn.GetIncomingGraphView<Date>(
          post_label_id_, person_label_id_, likes_label_id_);

      auto post_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, post_label_id_, hasCreator_label_id_);
      for (; post_ie.is_valid(); post_ie.next())
      {
        auto pid = post_ie.get_neighbor();
        auto message_id = txn.GetVertexId(post_label_id_, pid);
        auto person_ie = person_likes_post_in.get_edges(pid);
        for (; person_ie.is_valid(); person_ie.next())
        {
          auto item = person_ie.get_data();
          vec.emplace_back(person_ie.get_neighbor(), pid, ((Date *)item)->milli_second,
                           txn.GetVertexId(person_label_id_, person_ie.get_neighbor()),
                           message_id, true);
        }
      }
      auto person_likes_comment_in = txn.GetIncomingGraphView<Date>(
          comment_label_id_, person_label_id_, likes_label_id_);
      auto comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, comment_label_id_, hasCreator_label_id_);
      for (; comment_ie.is_valid(); comment_ie.next())
      {
        auto cid = comment_ie.get_neighbor();
        auto message_id = txn.GetVertexId(comment_label_id_, cid);
        auto person_ie = person_likes_comment_in.get_edges(cid);
        for (; person_ie.is_valid(); person_ie.next())
        {
          auto item = person_ie.get_data();
          vec.emplace_back(person_ie.get_neighbor(), cid, ((Date *)item)->milli_second,
                           txn.GetVertexId(person_label_id_, person_ie.get_neighbor()),
                           message_id, false);
        }
      }

      sort(vec.begin(), vec.end(),
           [&](const person_info &a, const person_info &b)
           {
             if (a.person_id != b.person_id)
             {
               return a.person_id < b.person_id;
             }
             if (a.creationDate != b.creationDate)
             {
               return a.creationDate > b.creationDate;
             }
             return a.message_id < b.message_id;
           });
      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);

      for (size_t i = 0; i < vec.size(); ++i)
      {
        if (i && vec[i].person_id == vec[i - 1].person_id)
          continue;

        if (pq.size() < 20)
        {
          pq.emplace(vec[i]);
        }
        else if (comparer(vec[i], pq.top()))
        {
          pq.pop();
          pq.emplace(vec[i]);
        }
      }

      std::vector<person_info> tmp;
      tmp.reserve(pq.size());
      while (!pq.empty())
      {
        tmp.emplace_back(pq.top());
        pq.pop();
      }

      constexpr int64_t mill_per_min = 60 * 1000l;
      for (auto i = tmp.size(); i > 0; --i)
      {
        const auto &v = tmp[i - 1];
        output.put_long(v.person_id);
        auto firstname = txn.GetVertexProp(person_label_id_, v.v, person_firstName_col_idx_);
        output.put_buffer_object(firstname);
        auto lastname = txn.GetVertexProp(person_label_id_, v.v, person_lastName_col_idx_);
        output.put_buffer_object(lastname);
        output.put_long(v.creationDate);
        output.put_long(v.message_id);
        // uint32_t x;
        if (v.is_post)
        {
          auto item = txn.GetVertexProp(post_label_id_, v.mid, post_length_col_idx_);
          auto content = gbp::BufferBlock::Ref<int>(item) == 0 ? txn.GetVertexProp(post_label_id_, v.mid, post_imageFile_col_idx_) : txn.GetVertexProp(post_label_id_, v.mid, post_content_col_idx_);

          output.put_buffer_object(content);
          item = txn.GetVertexProp(post_label_id_, v.mid, post_creationDate_col_idx_);
          auto min = (v.creationDate -
                      gbp::BufferBlock::Ref<Date>(item).milli_second) /
                     mill_per_min;
          output.put_int(min);
        }
        else
        {
          auto content = txn.GetVertexProp(comment_label_id_, v.mid, comment_content_col_idx_);
          output.put_buffer_object(content);
          auto item = txn.GetVertexProp(comment_label_id_, v.mid, comment_creationDate_col_idx_);
          auto min = (v.creationDate -
                      gbp::BufferBlock::Ref<Date>(item).milli_second) /
                     mill_per_min;
          output.put_int(min);
        }
        output.put_byte(friends_[v.v] ? 0 : 1);
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t knows_label_id_;
    label_t comment_label_id_;
    label_t likes_label_id_;
    label_t hasCreator_label_id_;

    int person_firstName_col_idx_;
    int person_lastName_col_idx_;
    int post_content_col_idx_;
    int post_imageFile_col_idx_;
    int post_length_col_idx_;
    int comment_content_col_idx_;
    int post_creationDate_col_idx_;
    int comment_creationDate_col_idx_;

    std::vector<oid_t> persons_;
    std::vector<oid_t> messages_;
    std::vector<bool> friends_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC7 *app = new gs::IC7(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC7 *casted = static_cast<gs::IC7 *>(app);
    delete casted;
  }
}

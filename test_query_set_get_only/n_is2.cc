#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IS2 : public AppBase
  {
  public:
    IS2(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),
          post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
          post_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
              graph.get_vertex_property_column(post_label_id_, "length")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          graph_(graph) {}

    struct message_info
    {
      message_info(bool is_post_, vid_t v_, int64_t creationdate_,
                   oid_t messageid_)
          : is_post(is_post_),
            v(v_),
            creationdate(creationdate_),
            messageid(messageid_) {}
      bool is_post;
      vid_t v;
      int64_t creationdate;
      oid_t messageid;
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
        return lhs.messageid > rhs.messageid;
      }
    };

    ~IS2() {}

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t req = input.get_long();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, req, root))
      {
        return false;
      }
      message_info_comparer comparer;
      std::priority_queue<message_info, std::vector<message_info>,
                          message_info_comparer>
          pq(comparer);
#if OV

      const auto &post_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, post_label_id_, hasCreator_label_id_);
      int64_t current_creationdate = 0;
      for (auto &e : post_ie)
      {
        if (pq.size() < 10)
        {
          auto creationdate =
              post_creationDate_col_.get_view(e.neighbor).milli_second;
          pq.emplace(true, e.neighbor, creationdate,
                     txn.GetVertexId(post_label_id_, e.neighbor));
          current_creationdate = pq.top().creationdate;
        }
        else
        {
          auto creationdate =
              post_creationDate_col_.get_view(e.neighbor).milli_second;
          if (creationdate > current_creationdate)
          {
            pq.pop();
            pq.emplace(true, e.neighbor, creationdate,
                       txn.GetVertexId(post_label_id_, e.neighbor));
            current_creationdate = pq.top().creationdate;
          }
          else if (creationdate == current_creationdate)
          {
            auto messageid = txn.GetVertexId(post_label_id_, e.neighbor);
            if (messageid > pq.top().messageid)
            {
              pq.pop();
              pq.emplace(true, e.neighbor, creationdate, messageid);
            }
          }
        }
      }

      const auto &comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, comment_label_id_, hasCreator_label_id_);

      for (auto &e : comment_ie)
      {
        if (pq.size() < 10)
        {
          auto creationdate =
              comment_creationDate_col_.get_view(e.neighbor).milli_second;
          pq.emplace(false, e.neighbor, creationdate,
                     txn.GetVertexId(comment_label_id_, e.neighbor));
          current_creationdate = pq.top().creationdate;
        }
        else
        {
          auto creationdate =
              comment_creationDate_col_.get_view(e.neighbor).milli_second;
          if (creationdate > current_creationdate)
          {
            pq.pop();
            pq.emplace(false, e.neighbor, creationdate,
                       txn.GetVertexId(comment_label_id_, e.neighbor));
            current_creationdate = pq.top().creationdate;
          }
          else if (creationdate == current_creationdate)
          {
            auto messageid = txn.GetVertexId(comment_label_id_, e.neighbor);
            if (messageid > pq.top().messageid)
            {
              pq.pop();
              pq.emplace(false, e.neighbor, creationdate, messageid);
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

      auto comment_replyOf_post_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, post_label_id_, replyOf_label_id_);
      auto comment_replyOf_comment_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, comment_label_id_, replyOf_label_id_);
      auto post_hasCreator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              post_label_id_, person_label_id_, hasCreator_label_id_);
      for (size_t i = vec.size(); i > 0; i--)
      {
        const auto &v = vec[i - 1];
        output.put_long(v.messageid);
        output.put_long(v.creationdate);
        if (v.is_post)
        {
          const auto &content = post_length_col_.get_view(v.v) == 0
                                    ? post_imageFile_col_.get_view(v.v)
                                    : post_content_col_.get_view(v.v);
          output.put_string_view(content);
          output.put_long(v.messageid);
          output.put_long(req);
          const auto &firstname = person_firstName_col_.get_view(root);
          output.put_string_view(firstname);
          const auto &lastname = person_lastName_col_.get_view(root);
          output.put_string_view(lastname);
        }
        else
        {
          const auto &content = comment_content_col_.get_view(v.v);
          output.put_string_view(content);
          vid_t u = v.v;
          while (true)
          {
            if (comment_replyOf_post_out.exist(u))
            {
              auto post_id = comment_replyOf_post_out.get_edge(u).neighbor;
              output.put_long(txn.GetVertexId(post_label_id_, post_id));
              u = post_hasCreator_person_out.get_edge(post_id).neighbor;
              output.put_long(txn.GetVertexId(person_label_id_, u));
              const auto &firstname = person_firstName_col_.get_view(u);
              output.put_string_view(firstname);
              const auto &lastname = person_lastName_col_.get_view(u);
              output.put_string_view(lastname);
              break;
            }
            else
            {
              assert(comment_replyOf_comment_out.exist(u));
              u = comment_replyOf_comment_out.get_edge(u).neighbor;
            }
          }
        }
      }
#else
      auto post_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, post_label_id_, hasCreator_label_id_);
      int64_t current_creationdate = 0;
      for (; post_ie.is_valid(); post_ie.next())
      {
        auto item = post_creationDate_col_.get(post_ie.get_neighbor());
        auto creationdate = gbp::BufferBlock::Ref<gs::Date>(item).milli_second;

        if (pq.size() < 10)
        {
          pq.emplace(true, post_ie.get_neighbor(), creationdate,
                     txn.GetVertexId(post_label_id_, post_ie.get_neighbor()));
          current_creationdate = pq.top().creationdate;
        }
        else
        {
          if (creationdate > current_creationdate)
          {
            pq.pop();
            pq.emplace(true, post_ie.get_neighbor(), creationdate,
                       txn.GetVertexId(post_label_id_, post_ie.get_neighbor()));
            current_creationdate = pq.top().creationdate;
          }
          else if (creationdate == current_creationdate)
          {
            auto messageid = txn.GetVertexId(post_label_id_, post_ie.get_neighbor());
            if (messageid > pq.top().messageid)
            {
              pq.pop();
              pq.emplace(true, post_ie.get_neighbor(), creationdate, messageid);
            }
          }
        }
      }
      auto comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, comment_label_id_, hasCreator_label_id_);

      for (; comment_ie.is_valid(); comment_ie.next())
      {
        if (pq.size() < 10)
        {
          auto item = comment_creationDate_col_.get(comment_ie.get_neighbor());
          auto creationdate = gbp::BufferBlock::Ref<gs::Date>(item).milli_second;

          pq.emplace(false, comment_ie.get_neighbor(), creationdate,
                     txn.GetVertexId(comment_label_id_, comment_ie.get_neighbor()));
          current_creationdate = pq.top().creationdate;
        }
        else
        {
          auto item = comment_creationDate_col_.get(comment_ie.get_neighbor());
          auto creationdate = gbp::BufferBlock::Ref<gs::Date>(item).milli_second;

          if (creationdate > current_creationdate)
          {
            pq.pop();
            pq.emplace(false, comment_ie.get_neighbor(), creationdate,
                       txn.GetVertexId(comment_label_id_, comment_ie.get_neighbor()));
            current_creationdate = pq.top().creationdate;
          }
          else if (creationdate == current_creationdate)
          {
            auto messageid = txn.GetVertexId(comment_label_id_, comment_ie.get_neighbor());
            if (messageid > pq.top().messageid)
            {
              pq.pop();
              pq.emplace(false, comment_ie.get_neighbor(), creationdate, messageid);
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

      auto comment_replyOf_post_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, post_label_id_, replyOf_label_id_);
      auto comment_replyOf_comment_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, comment_label_id_, replyOf_label_id_);
      auto post_hasCreator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              post_label_id_, person_label_id_, hasCreator_label_id_);

      for (size_t i = vec.size(); i > 0; i--)
      {
        const auto &v = vec[i - 1];
        output.put_long(v.messageid);
        output.put_long(v.creationdate);
        if (v.is_post)
        {
          auto item = post_length_col_.get(v.v);
          auto content = gbp::BufferBlock::Ref<int>(item) == 0 ? post_imageFile_col_.get(v.v) : post_content_col_.get(v.v);

          output.put_buffer_object(content);
          output.put_long(v.messageid);
          output.put_long(req);
          auto firstname = person_firstName_col_.get(root);
          output.put_buffer_object(firstname);

          auto lastname = person_lastName_col_.get(root);
          output.put_buffer_object(lastname);
        }
        else
        {
          auto content = comment_content_col_.get(v.v);
          output.put_buffer_object(content);

          vid_t u = v.v;
          while (true)
          {
            auto post_id = comment_replyOf_post_out.exist(u, exist_mark);
            if (exist_mark)
            {
              output.put_long(txn.GetVertexId(post_label_id_, gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(post_id).neighbor));
              auto item = post_hasCreator_person_out.get_edge(gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(post_id).neighbor);

              u = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
              output.put_long(txn.GetVertexId(person_label_id_, u));
              auto firstname = person_firstName_col_.get(u);

              output.put_buffer_object(firstname);
              auto lastname = person_lastName_col_.get(u);
              output.put_buffer_object(lastname);
              break;
            }
            else
            {
              auto item = comment_replyOf_comment_out.exist(u, exist_mark);
              assert(exist_mark);
              u = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            }
          }
        }
      }
#endif
      return true;
    }

  private:
#if !OV
    bool exist_mark = false;
#endif
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t person_label_id_;

    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    const DateColumn &post_creationDate_col_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &post_content_col_;
    const StringColumn &post_imageFile_col_;
    const IntColumn &post_length_col_;

    const StringColumn &comment_content_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IS2 *app = new gs::IS2(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IS2 *casted = static_cast<gs::IS2 *>(app);
    delete casted;
  }
}

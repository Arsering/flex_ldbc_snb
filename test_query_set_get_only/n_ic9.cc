#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

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
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }

      message_info_comparer cmp;
      std::priority_queue<message_info, std::vector<message_info>,
                          message_info_comparer>
          que(cmp);

      auto post_hasCreator_person_in = txn.GetIncomingGraphView<grape::EmptyType>(
          person_label_id_, post_label_id_, hasCreator_label_id_);
      auto comment_hasCreator_person_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              person_label_id_, comment_label_id_, hasCreator_label_id_);

      iterate_1d_2d_neighbors(
          txn, person_label_id_, knows_label_id_, root,
          [&](vid_t v)
          {
            {
#if OV
              const auto &edges = post_hasCreator_person_in.get_edges(v);
              for (auto &e : edges)
              {
                auto u = e.neighbor;
                auto creation_date =
                    post_creationDate_col_.get_view(u).milli_second;
#else
              auto edges = post_hasCreator_person_in.get_edges(v);
              for (; edges.is_valid(); edges.next())
              {
                auto u = edges.get_neighbor();
                auto item = post_creationDate_col_.get(u);
                auto creation_date =
                    gbp::Decode<Date>(item).milli_second;
#endif
                if (creation_date < maxdate)
                {
                  if (que.size() < 20)
                  {
                    oid_t u_id = txn.GetVertexId(post_label_id_, u);
                    que.emplace(u, v, u_id, creation_date, false);
                  }
                  else
                  {
                    const auto &top = que.top();
                    if (creation_date > top.creation_date)
                    {
                      que.pop();
                      oid_t u_id = txn.GetVertexId(post_label_id_, u);
                      que.emplace(u, v, u_id, creation_date, false);
                    }
                    else if (creation_date == top.creation_date)
                    {
                      oid_t u_id = txn.GetVertexId(post_label_id_, u);
                      if (u_id < top.message_id)
                      {
                        que.pop();
                        que.emplace(u, v, u_id, creation_date, false);
                      }
                    }
                  }
                }
              }
            }
            {
#if OV
              const auto &edges = comment_hasCreator_person_in.get_edges(v);
              for (auto &e : edges)
              {
                auto u = e.neighbor;
                auto creation_date =
                    comment_creationDate_col_.get_view(u).milli_second;
#else
              auto edges = comment_hasCreator_person_in.get_edges(v);
              for (; edges.is_valid(); edges.next())
              {
                auto u = edges.get_neighbor();
                auto item = comment_creationDate_col_.get(u);
                auto creation_date =
                    gbp::Decode<Date>(item).milli_second;
#endif
                if (creation_date < maxdate)
                {
                  if (que.size() < 20)
                  {
                    oid_t u_id = txn.GetVertexId(comment_label_id_, u);
                    que.emplace(u, v, u_id, creation_date, true);
                  }
                  else
                  {
                    const auto &top = que.top();
                    if (creation_date > top.creation_date)
                    {
                      que.pop();
                      oid_t u_id = txn.GetVertexId(comment_label_id_, u);
                      que.emplace(u, v, u_id, creation_date, true);
                    }
                    else if (creation_date == top.creation_date)
                    {
                      oid_t u_id = txn.GetVertexId(comment_label_id_, u);
                      if (u_id < top.message_id)
                      {
                        que.pop();
                        que.emplace(u, v, u_id, creation_date, true);
                      }
                    }
                  }
                }
              }
            }
          },
          txn.GetVertexNum(person_label_id_));

      std::vector<message_info> vec;
      vec.reserve(que.size());
      while (!que.empty())
      {
        vec.emplace_back(que.top());
        que.pop();
      }
      for (size_t idx = vec.size(); idx > 0; --idx)
      {
        auto &v = vec[idx - 1];
        vid_t person_lid = v.other_person_vid;
        output.put_long(txn.GetVertexId(person_label_id_, person_lid));
#if OV
        auto firstname = person_firstName_col_.get_view(person_lid);
        output.put_string_view(firstname);
        auto lastname = person_lastName_col_.get_view(person_lid);
        output.put_string_view(lastname);
#else
        auto firstname = person_firstName_col_.get(person_lid);
        output.put_string_view({firstname.Data(), firstname.Size()});
        auto lastname = person_lastName_col_.get(person_lid);
        output.put_string_view({lastname.Data(), lastname.Size()});
#endif
        output.put_long(v.message_id);
#if OV
        if (v.is_comment)
        {
          auto content = comment_content_col_.get_view(v.message_vid);
          output.put_string_view(content);
        }
        else
        {
          vid_t post_lid = v.message_vid;
          auto post_content = post_length_col_.get_view(post_lid) == 0
                                  ? post_imageFile_col_.get_view(post_lid)
                                  : post_content_col_.get_view(post_lid);
          output.put_string_view(post_content);
        }
#else
        if (v.is_comment)
        {
          auto content = comment_content_col_.get(v.message_vid);
          output.put_string_view({content.Data(), content.Size()});
        }
        else
        {
          vid_t post_lid = v.message_vid;
          auto item = post_length_col_.get(post_lid);
          auto post_content = gbp::Decode<int>(item) == 0 ? post_imageFile_col_.get(post_lid) : post_content_col_.get(post_lid);

          output.put_string_view({post_content.Data(), post_content.Size()});
        }
#endif
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

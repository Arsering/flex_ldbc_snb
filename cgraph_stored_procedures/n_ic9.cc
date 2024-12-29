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
          post_creationDate_col_(graph.GetPropertyHandle(post_label_id_, "creationDate")),
          comment_creationDate_col_(graph.GetPropertyHandle(comment_label_id_, "creationDate")),
          person_firstName_col_(graph.GetPropertyHandle(person_label_id_, "firstName")),
          person_lastName_col_(graph.GetPropertyHandle(person_label_id_, "lastName")),
          post_content_col_(graph.GetPropertyHandle(post_label_id_, "content")),
          post_imageFile_col_(graph.GetPropertyHandle(post_label_id_, "imageFile")),
          post_length_col_(graph.GetPropertyHandle(post_label_id_, "length")),
          comment_content_col_(graph.GetPropertyHandle(comment_label_id_, "content")),
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
      #ifdef ZED_PROFILE
      // std::cout<<"begin query"<<std::endl;
      #endif
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
              #ifdef ZED_PROFILE
              person_count+=1;
              #endif

              auto edges = post_hasCreator_person_in.get_edges(v);
              for (; edges.is_valid(); edges.next())
              {
                auto u = edges.get_neighbor();
                auto item = post_creationDate_col_.getProperty(u);
                auto creation_date =
                    gbp::BufferBlock::Ref<Date>(item).milli_second;
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
                auto item = comment_creationDate_col_.getProperty(u);
                #ifdef ZED_PROFILE
                comment_count++;
                #endif
                auto creation_date =
                    gbp::BufferBlock::Ref<Date>(item).milli_second;
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
        auto firstname = person_firstName_col_.getProperty(person_lid);
        output.put_buffer_object(firstname);

        auto lastname = person_lastName_col_.getProperty(person_lid);
        output.put_buffer_object(lastname);

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
          auto content = comment_content_col_.getProperty(v.message_vid);
          output.put_buffer_object(content);
        }
        else
        {
          vid_t post_lid = v.message_vid;
          auto item = post_length_col_.getProperty(post_lid);
          auto post_content = gbp::BufferBlock::Ref<int>(item) == 0 ? post_imageFile_col_.getProperty(post_lid) : post_content_col_.getProperty(post_lid);

          output.put_buffer_object(post_content);
        }
#endif
        output.put_long(v.creation_date);
      }
      #ifdef ZED_PROFILE
      // std::cout<<"end query,"<<person_count<<","<<comment_count<<std::endl;
      comment_count=0;
      person_count=0;
      #endif
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t hasCreator_label_id_;
    label_t knows_label_id_;

    cgraph::PropertyHandle post_content_col_;
    cgraph::PropertyHandle post_imageFile_col_;
    cgraph::PropertyHandle post_length_col_;
    cgraph::PropertyHandle person_firstName_col_;
    cgraph::PropertyHandle person_lastName_col_;
    cgraph::PropertyHandle comment_content_col_;
    cgraph::PropertyHandle comment_creationDate_col_;
    cgraph::PropertyHandle post_creationDate_col_;


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

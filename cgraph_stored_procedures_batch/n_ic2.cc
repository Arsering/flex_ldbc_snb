#include <cstddef>
#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"
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

      auto ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      auto oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      std::vector<vid_t> vids;
      for(;ie.is_valid();ie.next())
      {
        vids.push_back(ie.get_neighbor());
      }
      for (; oe.is_valid(); oe.next())
      {
        vids.push_back(oe.get_neighbor());
      }
      std::vector<std::pair<int,int>> person_post_index;
      std::vector<std::pair<int,int>> person_comment_index;
      auto post_vids = txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_, post_label_id_, hasCreator_label_id_, vids, person_post_index, false);
      auto comment_vids = txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, vids, person_comment_index, false);
      auto post_creationDates=txn.BatchGetVertexPropsFromVids(post_label_id_, post_vids, {post_creationDate_col_});
      auto comment_creationDates=txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {comment_creationDate_col_});
      auto person_oids=txn.BatchGetVertexIds(person_label_id_, vids);
      std::map<vid_t,oid_t> person_oids_map;
      auto post_oids=txn.BatchGetVertexIds(post_label_id_, post_vids);
      auto comment_oids=txn.BatchGetVertexIds(comment_label_id_, comment_vids);
      for(int i=0;i<vids.size();i++){
        auto v = vids[i];
        auto v_oid = person_oids[i];
        person_oids_map[v]=v_oid;
        for(int j=person_post_index[i].first;j<person_post_index[i].second;j++){
          auto pid = post_vids[j];
          auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(post_creationDates[0][j]).milli_second;
          if (creationDate < maxdate)
          {
            if (pq.size() < 20)
            {
              pq.emplace(pid, v, post_oids[j],
                         creationDate, true);
              current_date = pq.top().creationdate;
            }
            else if (creationDate > current_date)
            {
              auto message_id = post_oids[j];
              pq.pop();
              pq.emplace(pid, v, message_id, creationDate, true);
              current_date = pq.top().creationdate;
            }
            else if (creationDate == current_date)
            {
              auto message_id = post_oids[j];
              if (message_id < pq.top().message_id)
              {
                pq.pop();
                pq.emplace(pid, v, message_id, creationDate, true);
              }
            }
          }
        }
        for(int j=person_comment_index[i].first;j<person_comment_index[i].second;j++){
          auto cid = comment_vids[j];
          auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(comment_creationDates[0][j]).milli_second;
          if (creationDate < maxdate)
          {
            if (pq.size() < 20)
            {
              pq.emplace(cid, v, comment_oids[j],
                         creationDate, false);
              current_date = pq.top().creationdate;
            }
            else if (creationDate > current_date)
            {
              auto message_id = comment_oids[j];
              pq.pop();
              pq.emplace(cid, v, message_id, creationDate, false);
              current_date = pq.top().creationdate;
            }
            else if (creationDate == current_date)
            {
              auto message_id = comment_oids[j];
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
      std::vector<vid_t> person_vids;
      std::vector<vid_t> res_comment_vids;
      std::vector<vid_t> res_post_vids;
      std::vector<vid_t> res_post_imageFile_vids;
      std::vector<vid_t> res_post_content_vids;
      int comment_index=-1;
      int post_index=-1;
      int post_imageFile_index=-1;
      int post_content_index=-1;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        person_vids.push_back(pq.top().person_vid);
        if(pq.top().is_post){
          res_post_vids.push_back(pq.top().message_vid);
          post_index++;
        }else{
          res_comment_vids.push_back(pq.top().message_vid);
          comment_index++;
        }
        vec.emplace_back(pq.top());
        pq.pop();
      }
      auto post_lengths=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_vids, {post_length_col_});
      for(int i=0;i<res_post_vids.size();i++){
        auto length=gbp::BufferBlock::RefSingle<int>(post_lengths[0][i]);
        if(length==0){
          res_post_imageFile_vids.push_back(res_post_vids[i]);
          post_imageFile_index++;
        }else{
          res_post_content_vids.push_back(res_post_vids[i]);
          post_content_index++;
        }
      }
      auto person_firstNames=txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, {person_firstName_col_,person_lastName_col_});
      // auto person_lastNames=txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, {person_lastName_col_});
      auto comment_content=txn.BatchGetVertexPropsFromVids(comment_label_id_, res_comment_vids, {comment_content_col_});
      auto post_content=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_content_vids, {post_content_col_});
      auto post_imageFile=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_imageFile_vids, {post_imageFile_col_});
      for (size_t i = vec.size(); i > 0; i--)
      {
        auto &v = vec[i - 1];
        output.put_long(person_oids_map[v.person_vid]);
        output.put_buffer_object(person_firstNames[0][i-1]);
        output.put_buffer_object(person_firstNames[1][i-1]);

        output.put_long(v.message_id);
        if (!v.is_post)
        {
          auto content = comment_content[0][comment_index];
          output.put_buffer_object(content);
          comment_index--;
        }
        else
        {
          auto length=gbp::BufferBlock::RefSingle<int>(post_lengths[0][post_index]);
          auto content = length == 0 ? post_imageFile[0][post_imageFile_index] : post_content[0][post_content_index];
          output.put_buffer_object(content);
          if(length==0){
            post_imageFile_index--;
          }else{
            post_content_index--;
          }
          post_index--;
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

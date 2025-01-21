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
      std::vector<vid_t> person_vids;
      vid_t person_num = txn.GetVertexNum(person_label_id_);
      
      get_1d_2d_neighbors(txn,person_label_id_,knows_label_id_,root,person_num,person_vids);
      
      // auto post_hasCreator_person_in_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(person_label_id_, post_label_id_, hasCreator_label_id_, person_vids, false);
      // auto comment_hasCreator_person_in_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, person_vids, false);
      
      std::vector<std::pair<int,int>> person_post_index;
      std::vector<std::pair<int,int>> person_comment_index;
      auto post_vids=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_,post_label_id_,hasCreator_label_id_,person_vids,person_post_index,false);
      auto comment_vids=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_,comment_label_id_,hasCreator_label_id_,person_vids,person_comment_index,false);
      // person_post_index.reserve(person_vids.size());
      // std::vector<vid_t> post_vids;
      // for(int i=0;i<person_vids.size();i++){
      //   person_post_index.push_back(std::make_pair(post_vids.size(), post_vids.size()));
      //   person_post_index[i].first=post_vids.size();
      //   for(int j=0;j<post_hasCreator_person_in_items[i].size();j++){
      //     post_vids.push_back(post_hasCreator_person_in_items[i][j]);
      //   }
      //   person_post_index[i].second=post_vids.size();
      // }
      
      // person_comment_index.reserve(person_vids.size());
      // std::vector<vid_t> comment_vids;
      // for(int i=0;i<person_vids.size();i++){
      //   person_comment_index.push_back(std::make_pair(comment_vids.size(), comment_vids.size()));
      //   person_comment_index[i].first=comment_vids.size();
      //   for(int j=0;j<comment_hasCreator_person_in_items[i].size();j++){
      //     comment_vids.push_back(comment_hasCreator_person_in_items[i][j]);
      //   }
      //   person_comment_index[i].second=comment_vids.size();
      // }

      auto post_creationDate_col_items=txn.BatchGetVertexPropsFromVids(post_label_id_, post_vids, {post_creationDate_col_});
      // auto post_oids=txn.BatchGetVertexIds(post_label_id_, post_vids);
      auto comment_creationDate_col_items=txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {comment_creationDate_col_});
      // auto comment_oids=txn.BatchGetVertexIds(comment_label_id_, comment_vids);
      
      for(int i=0;i<person_vids.size();i++){
        for(int j=person_post_index[i].first;j<person_post_index[i].second;j++){
          auto creation_date=gbp::BufferBlock::Ref<Date>(post_creationDate_col_items[0][j]).milli_second;
          if(creation_date<maxdate){
            if(que.size()<20){
              auto post_oid=txn.GetVertexId(post_label_id_, post_vids[j]);
              que.emplace(post_vids[j],person_vids[i],post_oid,creation_date,false);
            }
            else{
              const auto &top=que.top();
              if(creation_date>top.creation_date){
                que.pop();
                auto post_oid=txn.GetVertexId(post_label_id_, post_vids[j]);
                que.emplace(post_vids[j],person_vids[i],post_oid,creation_date,false);
              }
              else if(creation_date==top.creation_date){
                auto post_oid=txn.GetVertexId(post_label_id_, post_vids[j]);
                if(post_oid<top.message_id){
                  que.pop();
                  que.emplace(post_vids[j],person_vids[i],post_oid,creation_date,false);
                }
              }
            }
          }
        }
        
        for(int j=person_comment_index[i].first;j<person_comment_index[i].second;j++){
          auto creation_date=gbp::BufferBlock::Ref<Date>(comment_creationDate_col_items[0][j]).milli_second;
          if(creation_date<maxdate){
            if(que.size()<20){
              auto comment_oid=txn.GetVertexId(comment_label_id_, comment_vids[j]);
              que.emplace(comment_vids[j],person_vids[i],comment_oid,creation_date,true);
            }
            else{
              const auto &top=que.top();
              if(creation_date>top.creation_date){
                que.pop();
                auto comment_oid=txn.GetVertexId(comment_label_id_, comment_vids[j]);
                que.emplace(comment_vids[j],person_vids[i],comment_oid,creation_date,true);
              }
              else if(creation_date==top.creation_date){
                auto comment_oid=txn.GetVertexId(comment_label_id_, comment_vids[j]);
                if(comment_oid<top.message_id){
                  que.pop();
                  que.emplace(comment_vids[j],person_vids[i],comment_oid,creation_date,true);
                }
              }
            }
          }
        }
      }
      
      std::vector<message_info> vec;
      vec.reserve(que.size());
      std::vector<vid_t> res_person_vids;
      std::vector<vid_t> res_comment_vids;
      std::vector<vid_t> res_post_vids;
      std::vector<vid_t> res_post_imageFile_vids;
      std::vector<vid_t> res_post_content_vids;
      int comment_index=-1;
      int post_index=-1;
      int post_imageFile_index=-1;
      int post_content_index=-1;
      while (!que.empty())
      {
        vec.emplace_back(que.top());
        res_person_vids.push_back(que.top().other_person_vid);
        if(que.top().is_comment){
          res_comment_vids.push_back(que.top().message_vid);
          comment_index++;
        }else{
          res_post_vids.push_back(que.top().message_vid);
          post_index++;
        }
        que.pop();
      }
      auto person_oids=txn.BatchGetVertexIds(person_label_id_, res_person_vids);
      auto person_props=txn.BatchGetVertexPropsFromVids(person_label_id_, res_person_vids, {person_firstName_col_, person_lastName_col_});
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
      auto comment_content=txn.BatchGetVertexPropsFromVids(comment_label_id_, res_comment_vids, {comment_content_col_});
      auto post_content=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_content_vids, {post_content_col_});
      auto post_imageFile=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_imageFile_vids, {post_imageFile_col_});
      auto post_creationDate=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_vids, {post_creationDate_col_});
      constexpr int64_t mill_per_min = 60 * 1000l;
      for (size_t idx = vec.size(); idx > 0; --idx)
      {
        auto &v = vec[idx - 1];
        output.put_long(person_oids[idx-1]);

        auto firstname = person_props[0][idx-1];//firstname
        output.put_buffer_object(firstname);

        auto lastname = person_props[1][idx-1];//lastname
        output.put_buffer_object(lastname);

        output.put_long(v.message_id);
        if (v.is_comment)
        {
          auto content = comment_content[0][comment_index];
          output.put_buffer_object(content);
          comment_index--;
        }
        else
        {
          auto length=gbp::BufferBlock::RefSingle<int>(post_lengths[0][post_index]);
          auto content = length == 0 ? post_imageFile[0][post_imageFile_index] : post_content[0][post_content_index];
          if(length==0){
            post_imageFile_index--;
          }else{
            post_content_index--;
          }
          output.put_buffer_object(content);
          post_index--;
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

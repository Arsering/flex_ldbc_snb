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
      // std::cout<<"begin query"<<std::endl;
      reply_count = 0;
      message_count=0;
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
                          comment_info_comparer>
          pq(comparer);

      auto comment_replyOf_post_in = txn.GetIncomingGraphView<grape::EmptyType>(
          post_label_id_, comment_label_id_, replyOf_label_id_);

      auto post_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, post_label_id_, hasCreator_label_id_);

      std::vector<vid_t> post_vids; 
      for (; post_ie.is_valid(); post_ie.next())
      {
        auto v = post_ie.get_neighbor();
        post_vids.push_back(v);
      }
      std::vector<std::pair<int,int>> post_reply_index;
      auto post_replies=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(post_label_id_,comment_label_id_,replyOf_label_id_,post_vids,post_reply_index,false);
      auto post_replies_creationDates=txn.BatchGetVertexPropsFromVids(comment_label_id_,post_replies,{&comment_creationDate_col_});
      auto post_replies_oids=txn.BatchGetVertexIds(comment_label_id_,post_replies);
      for(int i=0;i<post_vids.size();i++){
        message_count++;
        // auto item=comment_replyOf_post_in_item[i];
        for(auto j=post_reply_index[i].first;j<post_reply_index[i].second;j++){
          auto u=post_replies[j];
          reply_count++;
          if(pq.size()<20){
            pq.emplace(u,gbp::BufferBlock::RefSingle<gs::Date>(post_replies_creationDates[0][j]).milli_second,post_replies_oids[j]);
          }
          else{
            const auto &top = pq.top();
            int64_t creationDate=gbp::BufferBlock::RefSingle<gs::Date>(post_replies_creationDates[0][j]).milli_second;
            if(creationDate>top.creationDate){
              pq.pop();
              pq.emplace(u,creationDate,post_replies_oids[j]);
            }
            else if(creationDate==top.creationDate){
              oid_t comment_id=post_replies_oids[j];
              if(comment_id<top.comment_id){
                pq.emplace(u,creationDate,comment_id);
              }
            }
          }
        }
      }

      auto comment_replyOf_comment_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              comment_label_id_, comment_label_id_, replyOf_label_id_);
      auto comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, comment_label_id_, hasCreator_label_id_);
      std::vector<vid_t> comment_vids;
      for(;comment_ie.is_valid();comment_ie.next()){
        comment_vids.push_back(comment_ie.get_neighbor());
      }
      // auto comment_replyOf_comment_in_item=txn.BatchGetVidsNeighbors<grape::EmptyType>(comment_label_id_,comment_label_id_,replyOf_label_id_,comment_vids,false);
      // std::vector<vid_t> comment_replies;
      std::vector<std::pair<int,int>> comment_reply_index;
      auto comment_replies=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(comment_label_id_,comment_label_id_,replyOf_label_id_,comment_vids,comment_reply_index,false);
      // comment_reply_index.resize(comment_vids.size());
      // for(int i=0;i<comment_vids.size();i++){
      //   comment_reply_index[i].first=comment_replies.size();
      //   for(int j=0;j<comment_replyOf_comment_in_item[i].size();j++){
      //     comment_replies.push_back(comment_replyOf_comment_in_item[i][j]);
      //   }
      //   comment_reply_index[i].second=comment_replies.size();
      // }
      auto comment_replies_creationDates=txn.BatchGetVertexPropsFromVids(comment_label_id_,comment_replies,{&comment_creationDate_col_});
      auto comment_replies_oids=txn.BatchGetVertexIds(comment_label_id_,comment_replies);
      for(int i=0;i<comment_vids.size();i++){
        message_count++;
        // auto item=comment_replyOf_comment_in_item[i];
        for(auto j=comment_reply_index[i].first;j<comment_reply_index[i].second;j++){
          auto u=comment_replies[j];
          reply_count++;
          if(pq.size()<20){
            pq.emplace(u,gbp::BufferBlock::RefSingle<gs::Date>(comment_replies_creationDates[0][j]).milli_second,comment_replies_oids[j]);
          }
          else{
            const auto &top = pq.top();
            int64_t creationDate=gbp::BufferBlock::RefSingle<gs::Date>(comment_replies_creationDates[0][j]).milli_second;
            if(creationDate>top.creationDate){
              pq.pop();
              pq.emplace(u,creationDate,comment_replies_oids[j]);
            }
            else if(creationDate==top.creationDate){
              oid_t comment_id=comment_replies_oids[j];
              if(comment_id<top.comment_id){
                pq.emplace(u,creationDate,comment_id);
              }
            }
          }
        }
      }

      std::vector<comment_info> vec;
      std::vector<vid_t> res_comment_ids;
      std::vector<vid_t> res_person_ids;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        res_comment_ids.push_back(pq.top().comment_vid);
        pq.pop();
      }
      auto comment_oids=txn.BatchGetVertexIds(comment_label_id_,res_comment_ids);
      auto comment_content_items=txn.BatchGetVertexPropsFromVids(comment_label_id_,res_comment_ids,{&comment_content_col_});
      auto comment_creators=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(comment_label_id_,person_label_id_,hasCreator_label_id_,res_comment_ids,true);
      for(int i=0;i<res_comment_ids.size();i++){
        auto item=comment_creators[i];
        assert(txn.check_edge_exist(item));
        auto p=item[0].first;
        res_person_ids.push_back(p);
      }
      auto person_props=txn.BatchGetVertexPropsFromVids(person_label_id_,res_person_ids,{&person_firstName_col_,&person_lastName_col_});
      auto person_oids=txn.BatchGetVertexIds(person_label_id_,res_person_ids);
      
      for (auto i = vec.size(); i > 0; i--)
      {
        auto &v = vec[i - 1];
        output.put_long(person_oids[i-1]);
        auto item=person_props[0][i-1];//firstName
        output.put_buffer_object(item);
        item = person_props[1][i-1];//lastName
        output.put_buffer_object(item);
        output.put_long(v.creationDate);
        output.put_long(v.comment_id);
        item = comment_content_items[0][i-1];
        output.put_buffer_object(item);
      }

      // std::cout<<"end query"<<std::endl;
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;

    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    StringColumn &person_firstName_col_;
    StringColumn &person_lastName_col_;
    StringColumn &comment_content_col_;
    DateColumn &comment_creationDate_col_;

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

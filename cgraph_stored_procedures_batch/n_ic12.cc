#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IC12 : public AppBase
  {
  public:
    IC12(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          tagClass_label_id_(graph.schema().get_vertex_label_id("TAGCLASS")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          hasType_label_id_(graph.schema().get_edge_label_id("HASTYPE")),
          isSubClassOf_label_id_(
              graph.schema().get_edge_label_id("ISSUBCLASSOF")),
          tagClass_name_col_(graph.GetPropertyHandle(tagClass_label_id_, "name")),
          tag_name_col_(graph.GetPropertyHandle(tag_label_id_, "name")),
          person_firstName_col_(graph.GetPropertyHandle(person_label_id_, "firstName")),
          person_lastName_col_(graph.GetPropertyHandle(person_label_id_, "lastName")),
          tagClass_num_(graph.graph().vertex_num(tagClass_label_id_)),
          graph_(graph) {}
    ~IC12() {}

    void get_sub_tagClass(const ReadTransaction &txn, vid_t root)//这个dfs也不好batch
    {
      std::queue<vid_t> q;
      q.push(root);
      sub_tagClass_[root] = true;
      auto tagClass_isSubClassOf_tagClass_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              tagClass_label_id_, tagClass_label_id_, isSubClassOf_label_id_);
      while (!q.empty())
      {
        auto v = q.front();
        q.pop();
        auto ie = tagClass_isSubClassOf_tagClass_in.get_edges(v);
        for (; ie.is_valid(); ie.next())
        {
          if (!sub_tagClass_[ie.get_neighbor()])
          {
            sub_tagClass_[ie.get_neighbor()] = true;
            q.push(ie.get_neighbor());
          }
        }
      }
    }

    struct person_info
    {
      person_info(int reply_count_, vid_t person_vid_, oid_t person_id_)
          : reply_count(reply_count_),
            person_vid(person_vid_),
            person_id(person_id_) {}

      int reply_count;
      vid_t person_vid;
      oid_t person_id;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.reply_count > rhs.reply_count)
        {
          return true;
        }
        if (lhs.reply_count < rhs.reply_count)
        {
          return false;
        }
        return lhs.person_id < rhs.person_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      // std::cout<<"begin query"<<std::endl;
      auto txn = graph_.GetReadTransaction();
      person_count = 0;
      message_count = 0;
      vec_count = 0;

      oid_t personid = input.get_long();
      std::string_view tagclassname = input.get_string();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      tagClass_num_ = txn.GetVertexNum(tagClass_label_id_);
      vid_t tagClass_id = tagClass_num_;
      for (vid_t i = 0; i < tagClass_num_; ++i)
      {
        auto tagClass_name = tagClass_name_col_.getProperty(i);//这里有筛选，不batch
        std::vector<char> data(tagClass_name.Size());
        tagClass_name.Copy(data.data(), data.size());
        if (tagClass_name == tagclassname)
        {
          tagClass_id = i;
          break;
        }
      }
      assert(tagClass_id != tagClass_num_);
      sub_tagClass_.clear();
      sub_tagClass_.resize(txn.GetVertexNum(tagClass_label_id_));
      get_sub_tagClass(txn, tagClass_id);

      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);
      sub_tagClass_.clear();

      auto comment_hasCreator_person_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              person_label_id_, comment_label_id_, hasCreator_label_id_);
      auto comment_replyOf_post_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, post_label_id_, replyOf_label_id_);
      auto post_hasTag_tag_out = txn.GetOutgoingGraphView<grape::EmptyType>(
          post_label_id_, tag_label_id_, hasTag_label_id_);
      auto tag_hasType_tagClass_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              tag_label_id_, tagClass_label_id_, hasType_label_id_);

      auto oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      std::vector<vid_t> person_vids;
      for(; oe.is_valid(); oe.next()){
        person_vids.push_back(oe.get_neighbor());
      }
      auto ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for(; ie.is_valid(); ie.next()){
        person_vids.push_back(ie.get_neighbor());
      }
      // auto comment_hasCreator_person_in_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, person_vids, false);
      std::vector<std::pair<int,int>> person_comment_index;
      // std::vector<vid_t> comment_vids;
      auto comment_vids=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, person_vids, person_comment_index, false);
      // for(int i=0;i<person_vids.size();i++){
      //   person_comment_index.push_back(std::make_pair(i,0));
      //   person_comment_index[i].first=comment_vids.size();  
      //   auto comment_ie=comment_hasCreator_person_in_items[i];
      //   for(int j=0;j<comment_ie.size();j++){
      //     comment_vids.push_back(comment_ie[j]);
      //   }
      //   person_comment_index[i].second=comment_vids.size();
      // }
      auto comment_replyOf_post_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(comment_label_id_, post_label_id_, replyOf_label_id_, comment_vids, true);
      
      for (int i=0;i<person_vids.size();i++){
        person_count++;
        auto v = person_vids[i];
        int count = 0;
        bool mark = false;
        for (int j=person_comment_index[i].first;j<person_comment_index[i].second;j++){
          message_count++;
          auto item = comment_replyOf_post_out_items[j];
          if (txn.check_edge_exist(item))
          {
            auto tag_oe = post_hasTag_tag_out.get_edges(item[0].first);
            for (; tag_oe.is_valid(); tag_oe.next())
            {
              auto item = tag_hasType_tagClass_out.get_edge(tag_oe.get_neighbor());
              assert(tag_hasType_tagClass_out.exist1(item));
              auto tc = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
              if (sub_tagClass_[tc])
              {
                ++count;
                break;//这里有break，不batch
              }
            }
          }
        }
        if (count)
        {
          if (pq.size() < 20)
          {
            pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
          }
          else
          {
            const auto &top = pq.top();
            if (count > top.reply_count)
            {
              pq.pop();
              pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
            }
            else if (count == top.reply_count)
            {
              oid_t friend_id = txn.GetVertexId(person_label_id_, v);
              if (friend_id < top.person_id)
              {
                pq.pop();
                pq.emplace(count, v, friend_id);
              }
            }
          }
        }
      }

      std::vector<person_info> vec;
      std::vector<vid_t> res_person_vids;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        res_person_vids.push_back(vec.back().person_vid);
        pq.pop();
      }
      std::set<vid_t> tmp;
      vec_count = vec.size();
      auto person_props=txn.BatchGetVertexPropsFromVids(person_label_id_, res_person_vids, {person_firstName_col_,person_lastName_col_});
      
      // auto res_comment_hasCreator_person_in_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, res_person_vids, false);
      std::vector<std::pair<int,int>> res_person_comment_index;
      // std::vector<vid_t> res_comment_vids;
      auto res_comment_vids=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, res_person_vids, res_person_comment_index, false);
      // auto res_comment_hasCreator_person_in_items=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, res_person_vids, res_person_comment_index, false);
      // for(int i=0;i<res_person_vids.size();i++){
      //   res_person_comment_index.push_back(std::make_pair(res_comment_vids.size(),res_comment_vids.size()));
        // res_person_comment_index[i].first=res_comment_vids.size();
        // auto comment_ie=res_comment_hasCreator_person_in_items[i];
        // for(int j=0;j<comment_ie.size();j++){
        //   res_comment_vids.push_back(comment_ie[j]);
        // }
        // res_person_comment_index[i].second=res_comment_vids.size();
        // }
      auto res_comment_replyOf_post_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(comment_label_id_, post_label_id_, replyOf_label_id_, res_comment_vids, true);
      std::vector<vid_t> res_post_vids;
      std::map<int,int> comment_post_index;
      for(int i=0;i<res_comment_vids.size();i++){
        auto item=res_comment_replyOf_post_out_items[i];
        if(txn.check_edge_exist(item)){
          res_post_vids.push_back(item[0].first);
          comment_post_index[i]=res_post_vids.size()-1;
        }
      }
      auto res_post_hasTag_tag_out_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(post_label_id_, tag_label_id_, hasTag_label_id_, res_post_vids, true);

      for (auto i = vec.size(); i > 0; i--)
      {
        auto &v = vec[i - 1];
        output.put_long(v.person_id);
        auto item = person_props[0][i-1];
        output.put_buffer_object(item);
        item = person_props[1][i-1];
        output.put_buffer_object(item);

        tmp.clear();
        for (int j=res_person_comment_index[i-1].first;j<res_person_comment_index[i-1].second;j++){
          auto item=res_comment_replyOf_post_out_items[j];
          if(txn.check_edge_exist(item)){
            auto tag_e = res_post_hasTag_tag_out_items[comment_post_index[j]];
            auto tag_hasType_tagClass_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(tag_label_id_, tagClass_label_id_, hasType_label_id_, tag_e, true);
            for (int k=0;k<tag_e.size();k++){
              auto tag = tag_e[k];
              auto item = tag_hasType_tagClass_out_items[k];
              assert(txn.check_edge_exist(item));
              auto tc = item[0].first;
              if (sub_tagClass_[tc])
              {
              tmp.insert(tag);
              }
            }
          }
        }
        output.put_int(tmp.size());
        std::vector<vid_t> tmp_vids;
        for(auto tag:tmp){
          tmp_vids.push_back(tag);
        }
        auto tag_name_col_items=txn.BatchGetVertexPropsFromVids(tag_label_id_, tmp_vids, {tag_name_col_});
        for (int i=0;i<tmp_vids.size();i++){
          auto item = tag_name_col_items[0][i];
          output.put_buffer_object(item);
        }
        output.put_int(v.reply_count);
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t knows_label_id_;
    label_t tag_label_id_;
    label_t tagClass_label_id_;

    label_t post_label_id_;
    label_t comment_label_id_;
    label_t replyOf_label_id_;
    label_t hasCreator_label_id_;
    label_t hasTag_label_id_;
    label_t hasType_label_id_;
    label_t isSubClassOf_label_id_;
    vid_t tagClass_num_;

    cgraph::PropertyHandle tag_name_col_;
    cgraph::PropertyHandle tagClass_name_col_;
    cgraph::PropertyHandle person_firstName_col_;
    cgraph::PropertyHandle person_lastName_col_;

    std::vector<bool> sub_tagClass_;
    int person_count;
    int message_count;
    int vec_count;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC12 *app = new gs::IC12(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC12 *casted = static_cast<gs::IC12 *>(app);
    delete casted;
  }
}

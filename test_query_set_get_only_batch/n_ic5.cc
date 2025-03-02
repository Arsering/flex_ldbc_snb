#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC5 : public AppBase
  {
  public:
    IC5(GraphDBSession &graph)
        : forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasMember_label_id_(graph.schema().get_edge_label_id("HASMEMBER")),
          containerOf_label_id_(graph.schema().get_edge_label_id("CONTAINEROF")),
          forum_title_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(forum_label_id_, "title")))),
          graph_(graph) {}

    ~IC5() {}

    struct forum_info
    {
      forum_info(vid_t forum_lid_, int post_count_, oid_t forum_id_)
          : forum_lid(forum_lid_), post_count(post_count_), forum_id(forum_id_) {}

      vid_t forum_lid;
      int post_count;
      oid_t forum_id;
    };

    struct forum_info_comparer
    {
      bool operator()(const forum_info &lhs, const forum_info &rhs) const
      {
        if (lhs.post_count > rhs.post_count)
        {
          return true;
        }
        else if (lhs.post_count < rhs.post_count)
        {
          return false;
        }
        else
        {
          return lhs.forum_id < rhs.forum_id;
        }
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      // std::cout<<"begin query"<<std::endl;
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      int64_t mindate = input.get_long();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      vid_t person_num = txn.GetVertexNum(person_label_id_);
      vid_t forum_num = txn.GetVertexNum(forum_label_id_);

      post_count_.resize(forum_num, 0);
      person_forum_set_.resize(forum_num, false);

      std::vector<vid_t> friends;
      get_1d_2d_neighbors(txn, person_label_id_, knows_label_id_, root, person_num, friends); 
      
      auto forum_hasMember_person_in_items=txn.BatchGetEdgePropsFromSrcVids<Date>(person_label_id_, forum_label_id_, hasMember_label_id_, friends, false);      
      std::vector<vid_t> post_vids;
      auto post_hasCreator_person_in = txn.GetIncomingGraphView<grape::EmptyType>(
          person_label_id_, post_label_id_, hasCreator_label_id_);
      auto forum_containerOf_post_in =
          txn.GetIncomingSingleGraphView<grape::EmptyType>(
              post_label_id_, forum_label_id_, containerOf_label_id_);
      for(int i=0;i<friends.size();i++){
        for(int j=0;j<forum_hasMember_person_in_items[i].size();j++){
          auto forum=forum_hasMember_person_in_items[i][j];
          if(mindate<forum.second.milli_second){
            person_forum_set_[forum.first]=true;
            person_forum_list_.push_back(forum.first);
            if(post_count_[forum.first]==0){
              forum_list_.push_back(forum.first);
              ++post_count_[forum.first];
            }
          }
        }
        if(person_forum_list_.empty()){
          continue;
        }

        auto post_person_ie = post_hasCreator_person_in.get_edges(friends[i]);//这里这样做是因为不是所有的person都会获取他的post
        post_vids.clear();
        for(;post_person_ie.is_valid();post_person_ie.next()){
          post_vids.push_back(post_person_ie.get_neighbor());
        }
        auto forum_containerOf_post_in_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(post_label_id_, forum_label_id_, containerOf_label_id_, post_vids, false);
        for(int j=0;j<post_vids.size();j++){
          auto item=forum_containerOf_post_in_items[j];
          assert(txn.check_edge_exist(item));
          if(person_forum_set_[item[0].first]){
            ++post_count_[item[0].first];
          }
        }

        for(auto v:person_forum_list_){
          person_forum_set_[v]=false;
        }
        person_forum_list_.clear();
      }

      forum_info_comparer cmp;
      std::priority_queue<forum_info, std::vector<forum_info>,
                          forum_info_comparer>
          que(cmp);
      for (auto v : forum_list_)
      {
        if (que.size() < 20)
        {
          que.emplace(v, post_count_[v], txn.GetVertexId(forum_label_id_, v));
        }
        else
        {
          const auto &top = que.top();
          if (top.post_count < post_count_[v])
          {
            que.pop();
            que.emplace(v, post_count_[v], txn.GetVertexId(forum_label_id_, v));
          }
          else if (top.post_count == post_count_[v])
          {
            oid_t id = txn.GetVertexId(forum_label_id_, v);
            if (id < top.forum_id)
            {
              que.pop();
              que.emplace(v, post_count_[v], id);
            }
          }
        }
        post_count_[v] = 0;
      }
      forum_list_.clear();

      std::vector<forum_info> vec;
      vec.reserve(que.size());
      while (!que.empty())
      {
        vec.emplace_back(que.top());
        forum_list_.push_back(que.top().forum_lid);
        que.pop();
      }
      auto forum_titles=txn.BatchGetVertexPropsFromVids(forum_label_id_, forum_list_,{&forum_title_col_});
      for (size_t i = vec.size(); i > 0; i--)
      {
        auto &cur = vec[i - 1];
#if OV
        output.put_string_view(forum_title_col_.get_view(cur.forum_lid));
#else
        auto forum_title = forum_titles[0][i-1];
        output.put_buffer_object(forum_title);
#endif
        output.put_int(cur.post_count - 1);
      }
      // std::cout<<"end query"<<std::endl;
      return true;
    }

  private:
    label_t forum_label_id_;
    label_t person_label_id_;
    label_t post_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t hasMember_label_id_;
    label_t containerOf_label_id_;

    StringColumn &forum_title_col_;

    std::vector<int> post_count_;
    std::vector<vid_t> forum_list_;

    std::vector<bool> person_forum_set_;
    std::vector<vid_t> person_forum_list_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC5 *app = new gs::IC5(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC5 *casted = static_cast<gs::IC5 *>(app);
    delete casted;
  }
}

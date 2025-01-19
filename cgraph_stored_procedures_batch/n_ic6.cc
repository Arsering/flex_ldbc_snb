#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC6 : public AppBase
  {
  public:
    IC6(GraphDBSession &graph)
        : tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          tag_num_(graph.graph().vertex_num(tag_label_id_)),
          tag_name_col_(graph.GetPropertyHandle(tag_label_id_, "name")),
          graph_(graph) {}

    ~IC6() {}

    struct tag_info
    {
      tag_info(int count_, gbp::BufferBlock tag_name_)
          : count(count_), tag_name(tag_name_) {}
      int count;
      gbp::BufferBlock tag_name;
    };

    struct tag_info_comparer
    {
      bool operator()(const tag_info &lhs, const tag_info &rhs)
      {
        if (lhs.count > rhs.count)
        {
          return true;
        }
        if (lhs.count < rhs.count)
        {
          return false;
        }
        return lhs.tag_name < rhs.tag_name;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      std::string_view tagname = input.get_string();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      vid_t tag_id = tag_num_;
      for (vid_t i = 0; i < tag_num_; ++i)
      {
        auto tag_name_item = tag_name_col_.getProperty(i);
        if (tag_name_item == tagname)
        {
          tag_id = i;
          break;
        }
      }
      assert(tag_id != tag_num_);

      get_2d_friends(txn, person_label_id_, knows_label_id_, root,
                     txn.GetVertexNum(person_label_id_), friends_);

      std::vector<int> post_count(tag_num_, 0);

      auto posts_with_tag = txn.GetIncomingEdges<grape::EmptyType>(
          tag_label_id_, tag_id, post_label_id_, hasTag_label_id_);
      
      std::vector<vid_t> post_vids;
      std::vector<vid_t> person_vids;
      for(int i=0;i<friends_.size();i++){
        if(friends_[i]){
          person_vids.push_back(i);
        }
      }
      for(;posts_with_tag.is_valid();posts_with_tag.next()){
        post_vids.push_back(posts_with_tag.get_neighbor());
      }
      auto post_hasCreator_person_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(post_label_id_,person_label_id_,  hasCreator_label_id_, post_vids, true);
      auto post_hasTag_tag_out_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(post_label_id_,tag_label_id_,  hasTag_label_id_, post_vids, true);
      for (int i=0;i<post_vids.size();i++){
        auto item = post_hasCreator_person_out_items[i];
        assert(txn.check_edge_exist(item));

        auto creator = item[0].first;
        if (friends_[creator])
        {
          auto oe = post_hasTag_tag_out_items[i];
          for (int j=0;j<oe.size();j++){
            ++post_count[oe[j]];
          }
        }
      }

      post_count[tag_id] = 0;

      tag_info_comparer comparer;
      std::priority_queue<tag_info, std::vector<tag_info>, tag_info_comparer> que(
          comparer);
      vid_t other_tag_id = 0;
      while (que.size() < 10 && other_tag_id < tag_num_)
      {
        if (post_count[other_tag_id] > 0)
        {
          que.emplace(post_count[other_tag_id],
                      tag_name_col_.getProperty(other_tag_id));//这里的循环也有筛选，就不batch了
        }
        ++other_tag_id;
      }
      while (other_tag_id < tag_num_)
      {
        const auto &top = que.top();
        auto cur_count = post_count[other_tag_id];
        if (cur_count > top.count)
        {
          que.pop();
          que.emplace(cur_count, tag_name_col_.getProperty(other_tag_id));//这里的循环也有筛选，就不batch了
        }
        else if (cur_count == top.count)
        {
          auto tag_name_item = tag_name_col_.getProperty(other_tag_id);
          if (tag_name_item < top.tag_name)
          {
            que.pop();
            que.emplace(cur_count, tag_name_item);
          }
        }
        ++other_tag_id;
      }
      std::vector<tag_info> vec;
      vec.reserve(que.size());
      while (!que.empty())
      {
        vec.emplace_back(que.top());
        que.pop();
      }
      for (size_t i = vec.size(); i > 0; i--)
      {
        output.put_buffer_object(vec[i - 1].tag_name);
        output.put_int(vec[i - 1].count);
      }
      return true;
    }

  private:
    label_t tag_label_id_;
    label_t person_label_id_;
    label_t post_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t hasTag_label_id_;

    std::vector<bool> friends_;

    vid_t tag_num_;
    cgraph::PropertyHandle tag_name_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC6 *app = new gs::IC6(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC6 *casted = static_cast<gs::IC6 *>(app);
    delete casted;
  }
}

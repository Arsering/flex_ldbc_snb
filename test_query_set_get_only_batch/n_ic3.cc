#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IC3 : public AppBase
  {
  public:
    IC3(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          isPartOf_label_id_(graph.schema().get_edge_label_id("ISPARTOF")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          place_num_(graph.graph().vertex_num(place_label_id_)),
          graph_(graph)
    {
      place_Locatedin_.resize(place_num_);
    }
    ~IC3() {}
    void get_friends(const ReadTransaction &txn, vid_t root,
                     std::vector<vid_t> &friends)
    {
      std::vector<vid_t> neighbors;

      auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_isLocatedIn_place_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              person_label_id_, place_label_id_, isLocatedIn_label_id_);
#if OV
      const auto &ie = person_knows_person_in.get_edges(root);
      for (auto &e : ie)
      {
        neighbors.push_back(e.neighbor);
      }
      const auto &oe = person_knows_person_out.get_edges(root);
      for (auto &e : oe)
      {
        neighbors.push_back(e.neighbor);
      }
#else
      auto ie = person_knows_person_in.get_edges(root);
      for (; ie.is_valid(); ie.next())
      {
        neighbors.push_back(ie.get_neighbor());
      }
      auto oe = person_knows_person_out.get_edges(root);
      for (; oe.is_valid(); oe.next())
      {
        neighbors.push_back(oe.get_neighbor());
      }
#endif
      friends_.clear();
      friends_.resize(person_num_, false);
      if (neighbors.empty())
      {
        return;
      }

      auto person_isLocatedIn_place_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(person_label_id_, place_label_id_, isLocatedIn_label_id_, neighbors, true);
      auto person_knows_person_in_items=txn.BatchGetVidsNeighbors<gs::Date>(person_label_id_, person_label_id_, knows_label_id_, neighbors, false);
      auto person_knows_person_out_items=txn.BatchGetVidsNeighbors<gs::Date>(person_label_id_, person_label_id_, knows_label_id_, neighbors, true);
      
      for (int i=0;i<neighbors.size();i++)
      {
        auto v=neighbors[i];
        auto item = person_isLocatedIn_place_out_items[i];
        assert(txn.check_edge_exist(item));

        // auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
        // item.free();
        auto p=item[0].first;
        if (!place_Locatedin_[p] && (!friends_[v]))
        {
          friends.push_back(v);
        }
        friends_[v] = true;
        // auto iev = person_knows_person_in.get_edges(v);
        auto iev=person_knows_person_in_items[i];
        for (int j=0;j<iev.size();j++)
        {
          if (!friends_[iev[j]])
          {
            friends_[iev[j]] = true;
            auto item = person_isLocatedIn_place_out.get_edge(iev[j]);//这里会筛选再读边，就不batch了
            assert(person_isLocatedIn_place_out.exist1(item));

            auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            item.free();
            if (!place_Locatedin_[p])
            {
              friends.push_back(iev[j]);
            }
          }
        }
        auto oev=person_knows_person_out_items[i];
        for (int j=0;j<oev.size();j++)
        {
          auto neb = oev[j];
          if (!friends_[neb])
          {
            auto item = person_isLocatedIn_place_out.get_edge(neb);//这里会筛选再读边，就不batch了
            assert(person_isLocatedIn_place_out.exist1(item));

            auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            if (!place_Locatedin_[p])
            {
              friends.push_back(neb);
            }
            friends_[neb] = true;
          }
        }
      }
    }

    struct person_info
    {
      person_info(int count_, vid_t otherPerson_vid_, oid_t otherPerson_id_)
          : count(count_),
            otherPerson_vid(otherPerson_vid_),
            otherPerson_id(otherPerson_id_) {}

      int count;
      vid_t otherPerson_vid;
      oid_t otherPerson_id;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.count > rhs.count)
        {
          return true;
        }
        if (lhs.count < rhs.count)
        {
          return false;
        }
        return lhs.otherPerson_id < rhs.otherPerson_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      std::string_view countryxname = input.get_string();
      std::string_view countryyname = input.get_string();
      int64_t start_date = input.get_long();
      int durationdays = input.get_int();
      CHECK(input.empty());

      friends_.clear();

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }

      vid_t countryX = place_num_;
      vid_t countryY = place_num_;
      std::vector<vid_t> place_vids;
      for(int i=0;i<place_num_;i++){
        place_vids.push_back(i);
      }
      auto place_name_items=txn.BatchGetVertexPropsFromVids(place_label_id_, place_vids, {&place_name_col_});
      for (int i = 0; i < place_num_; ++i)
      {
        auto place_name_item=place_name_items[0][i];
        if (place_name_item == countryxname)
        {
          countryX = i;
        }
        if (place_name_item == countryyname)
        {
          countryY = i;
        }
      }
      assert(countryX != place_num_);
      assert(countryY != place_num_);

#if OV
      const auto &xe = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryX, place_label_id_, isPartOf_label_id_);
      for (auto &e : xe)
      {
        place_Locatedin_[e.neighbor] = true;
      }
      const auto &ye = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryY, place_label_id_, isPartOf_label_id_);
      for (auto &e : ye)
      {
        place_Locatedin_[e.neighbor] = true;
      }
#else
      auto place_isPartOf_place_in_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(place_label_id_, place_label_id_, isPartOf_label_id_, {countryX, countryY}, false);
      auto xe = place_isPartOf_place_in_items[0];
      for (int i=0;i<xe.size();i++)
      {
        place_Locatedin_[xe[i]] = true;
      }
      auto ye = place_isPartOf_place_in_items[1];
      for (int i=0;i<ye.size();i++)
      {
        place_Locatedin_[ye[i]] = true;
      }
#endif

      std::vector<vid_t> friends{};
      person_num_ = txn.GetVertexNum(person_label_id_);
      count_.clear();
      count_.resize(person_num_, {});

      get_friends(txn, root, friends);
      const int64_t milli_sec_per_day = 24 * 60 * 60 * 1000l;
      int64_t end_date = start_date + durationdays * milli_sec_per_day;
      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);
#if OV
      const auto &postex = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryX, post_label_id_, isLocatedIn_label_id_);
      for (auto &e : postex)
      {
        auto creationDate =
            post_creationDate_col_.get_view(e.neighbor).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          assert(post_hasCreator_person_out.exist(e.neighbor));
          auto p = post_hasCreator_person_out.get_edge(e.neighbor).neighbor;
          if (friends_[p])
          {
            count_[p].first += 1;
          }
        }
      }

      const auto &commentex = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryX, comment_label_id_, isLocatedIn_label_id_);
      for (auto &e : commentex)
      {
        auto creationDate =
            comment_creationDate_col_.get_view(e.neighbor).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          assert(comment_hasCreator_person_out.exist(e.neighbor));
          auto p = comment_hasCreator_person_out.get_edge(e.neighbor).neighbor;
          if (friends_[p])
          {
            count_[p].first += 1;
          }
        }
      }

      const auto &postey = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryY, post_label_id_, isLocatedIn_label_id_);
      for (auto &e : postey)
      {
        auto creationDate =
            post_creationDate_col_.get_view(e.neighbor).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          assert(post_hasCreator_person_out.exist(e.neighbor));
          auto p = post_hasCreator_person_out.get_edge(e.neighbor).neighbor;
          if (friends_[p])
          {
            count_[p].second += 1;
          }
        }
      }
      const auto &commentey = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryY, comment_label_id_, isLocatedIn_label_id_);
      for (auto &e : commentey)
      {
        auto creationDate =
            comment_creationDate_col_.get_view(e.neighbor).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          assert(comment_hasCreator_person_out.exist(e.neighbor));
          auto p = comment_hasCreator_person_out.get_edge(e.neighbor).neighbor;
          if (friends_[p])
          {
            count_[p].second += 1;
          }
        }
      }
#else
      auto postxy=txn.BatchGetVidsNeighbors<grape::EmptyType>(place_label_id_, post_label_id_, isLocatedIn_label_id_, {countryX, countryY}, false);
      std::vector<vid_t> post_vids;
      std::vector<vid_t> filtered_post_vids;
      int post_x_size=postxy[0].size();
      for(int i=0;i<postxy.size();i++){
        for(int j=0;j<postxy[i].size();j++){
          post_vids.push_back(postxy[i][j]);
        }
      }
      auto post_creationDate_items=txn.BatchGetVertexPropsFromVids(post_label_id_, post_vids, {&post_creationDate_col_});
      for (int i=0;i<post_x_size;i++){
        auto item=post_creationDate_items[0][i];
        auto creationDate=gbp::BufferBlock::Ref<Date>(item).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          filtered_post_vids.push_back(post_vids[i]);
        }
      }
      auto post_hasCreator_person_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(post_label_id_, person_label_id_, hasCreator_label_id_, filtered_post_vids, true);
      for (int i=0;i<filtered_post_vids.size();i++){
        auto item = post_hasCreator_person_out_items[i];
        assert(txn.check_edge_exist(item));
        auto p=item[0].first;
        if(friends_[p]){
          count_[p].first+=1;
        }
      }
      auto commentxy=txn.BatchGetVidsNeighbors<grape::EmptyType>(place_label_id_, comment_label_id_, isLocatedIn_label_id_, {countryX, countryY}, false);
      std::vector<vid_t> comment_vids;
      std::vector<vid_t> filtered_comment_vids;
      int comment_x_size=commentxy[0].size();
      for(int i=0;i<commentxy.size();i++){
        for(int j=0;j<commentxy[i].size();j++){
          comment_vids.push_back(commentxy[i][j]);
        }
      }
      auto comment_creationDate_items=txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {&comment_creationDate_col_});
      for (int i=0;i<comment_x_size;i++){
        auto item=comment_creationDate_items[0][i];
        auto creationDate=gbp::BufferBlock::Ref<Date>(item).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          filtered_comment_vids.push_back(comment_vids[i]);
        }
      }
      auto comment_hasCreator_person_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(comment_label_id_, person_label_id_, hasCreator_label_id_, filtered_comment_vids, true);
      for (int i=0;i<filtered_comment_vids.size();i++){
        auto item = comment_hasCreator_person_out_items[i];
        assert(txn.check_edge_exist(item));
        auto p=item[0].first;
        if(friends_[p]){
          count_[p].first+=1;
        }
      }
      filtered_post_vids.clear();
      filtered_comment_vids.clear();
      for (int i=post_x_size;i<post_vids.size();i++){
        auto item=post_creationDate_items[0][i];
        auto creationDate=gbp::BufferBlock::Ref<Date>(item).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          filtered_post_vids.push_back(post_vids[i]);
        }
      }
      post_hasCreator_person_out_items.clear();
      post_hasCreator_person_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(post_label_id_, person_label_id_, hasCreator_label_id_, filtered_post_vids, true);
      for (int i=0;i<filtered_post_vids.size();i++){
        auto item = post_hasCreator_person_out_items[i];
        assert(txn.check_edge_exist(item));
        auto p=item[0].first;
        if(friends_[p]){
          count_[p].second+=1;
        }
      }
      for (int i=comment_x_size;i<comment_vids.size();i++){
        auto item=comment_creationDate_items[0][i];
        auto creationDate=gbp::BufferBlock::Ref<Date>(item).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          filtered_comment_vids.push_back(comment_vids[i]);
        }
      }
      comment_hasCreator_person_out_items.clear();
      comment_hasCreator_person_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(comment_label_id_, person_label_id_, hasCreator_label_id_, filtered_comment_vids, true);
      for (int i=0;i<filtered_comment_vids.size();i++){
        auto item = comment_hasCreator_person_out_items[i];
        assert(txn.check_edge_exist(item));
        auto p=item[0].first;
        if(friends_[p]){
          count_[p].second+=1;
        }
      }
#endif

      for (auto v : friends)
      {
        if (count_[v].first && count_[v].second)
        {
          int count = count_[v].first + count_[v].second;
          if (pq.size() < 20)
          {
            pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
          }
          else
          {
            const person_info &top = pq.top();
            if (count > top.count)
            {
              pq.pop();
              pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
            }
            else if (count == top.count)
            {
              oid_t otherPerson_id = txn.GetVertexId(person_label_id_, v);
              if (otherPerson_id < top.otherPerson_id)
              {
                pq.pop();
                pq.emplace(count, v, otherPerson_id);
              }
            }
          }
        }
      }

      std::vector<person_info> vec;
      std::vector<vid_t> person_vids;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        person_vids.push_back(vec.back().otherPerson_vid);
        pq.pop();
      }
      auto person_props=txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, {&person_firstName_col_, &person_lastName_col_});
      for (size_t idx = vec.size(); idx > 0; --idx)
      {
        auto &p = vec[idx - 1];
        auto v = p.otherPerson_vid;
        output.put_long(p.otherPerson_id);
#if OV
        output.put_string_view(person_firstName_col_.get_view(v));
        output.put_string_view(person_lastName_col_.get_view(v));
        output.put_long(count_[v].first);
        output.put_long(count_[v].second);
        output.put_long(p.count);
      }
      for (auto &e : xe)
      {
        place_Locatedin_[e.neighbor] = false;
      }
      for (auto &e : ye)
      {
        place_Locatedin_[e.neighbor] = false;
      }
#else
        auto name = person_props[0][idx-1];
        output.put_buffer_object(name);

        name = person_props[1][idx-1];
        output.put_buffer_object(name);

        output.put_long(count_[v].first);
        output.put_long(count_[v].second);
        output.put_long(p.count);
      }
      for (int i=0;i<xe.size();i++)
      {
        place_Locatedin_[xe[i]] = false;
      }
      for (int i=0;i<ye.size();i++)
      {
        place_Locatedin_[ye[i]] = false;
      }
#endif
      // std::cout<<"end query"<<std::endl;
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t place_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t isLocatedIn_label_id_;
    label_t isPartOf_label_id_;

    std::vector<bool> friends_;
    std::vector<bool> place_Locatedin_;
    vid_t person_num_;
    std::vector<std::pair<int, int>> count_;

    DateColumn &post_creationDate_col_;
    DateColumn &comment_creationDate_col_;
    StringColumn &person_firstName_col_;
    StringColumn &person_lastName_col_;
    StringColumn &place_name_col_;
    vid_t place_num_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC3 *app = new gs::IC3(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC3 *casted = static_cast<gs::IC3 *>(app);
    delete casted;
  }
}

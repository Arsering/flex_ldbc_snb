#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IC10 : public AppBase
  {
  public:
    IC10(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          hasInterest_label_id_(graph.schema().get_edge_label_id("HASINTEREST")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          person_birthday_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(person_label_id_, "birthday")))),
          person_gender_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "gender")))),

          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          graph_(graph)
    {
    }
    ~IC10() {}
    void get_friends(const ReadTransaction &txn, vid_t root, int mon)
    {
      std::vector<vid_t> friends_1d;
      friends_set_[root] = true;
      auto person_knows_person_out=txn.GetOutgoingGraphView<Date>(person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in=txn.GetIncomingGraphView<Date>(person_label_id_, person_label_id_, knows_label_id_);
      auto oe = person_knows_person_out.get_edges(root);
      for (; oe.is_valid(); oe.next())
      {
        friends_1d.emplace_back(oe.get_neighbor());
        friends_set_[oe.get_neighbor()] = true;
      }
      auto ie = person_knows_person_in.get_edges(root);
      for (; ie.is_valid(); ie.next())
      {
        friends_1d.emplace_back(ie.get_neighbor());
        friends_set_[ie.get_neighbor()] = true;
      }
      auto person_knows_person_out_items=txn.BatchGetVidsNeighbors<Date>(person_label_id_, person_label_id_, knows_label_id_, friends_1d, true);
      auto person_knows_person_in_items=txn.BatchGetVidsNeighbors<Date>(person_label_id_, person_label_id_, knows_label_id_, friends_1d, false);
      std::vector<vid_t> friends_2d;
      for (int i=0;i<friends_1d.size();i++){
        auto oe1=person_knows_person_out_items[i];
        for (int j=0;j<oe1.size();j++){
          auto u=oe1[j];
          if (!friends_set_[u])
          {
            friends_set_[u] = true;
            friends_2d.emplace_back(u);
          }
        }
        auto ie1=person_knows_person_in_items[i];
        for (int j=0;j<ie1.size();j++){
          auto u=ie1[j];
          if (!friends_set_[u])
          {
            friends_set_[u] = true;
            friends_2d.emplace_back(u);
          }
        }
      }
      auto person_birthday_col_items=txn.BatchGetVertexPropsFromVids(person_label_id_, friends_2d, {&person_birthday_col_});
      for (int i=0;i<friends_2d.size();i++){
        auto item=gbp::BufferBlock::Ref<Date>(person_birthday_col_items[0][i]).milli_second;
        auto t=item/1000;
        auto tm=gmtime((time_t*)(&t));
        if ((tm->tm_mon + 1 == mon && tm->tm_mday >= 21) ||
            ((mon <= 11 && tm->tm_mon == mon && tm->tm_mday < 22) ||
             (tm->tm_mon == 0 && mon == 12 && tm->tm_mday < 22)))
        {
          friends_list_.emplace_back(friends_2d[i]);
        }
      }
    }

    struct person_info
    {
      person_info(int score_, vid_t person_vid_, oid_t person_id_)
          : score(score_), person_vid(person_vid_), person_id(person_id_) {}

      int score;
      vid_t person_vid;
      oid_t person_id;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.score > rhs.score)
        {
          return true;
        }
        if (lhs.score < rhs.score)
        {
          return false;
        }
        return lhs.person_id < rhs.person_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      int month = input.get_int();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      friends_list_.clear();
      friends_set_.clear();
      friends_set_.resize(txn.GetVertexNum(person_label_id_), false);
      get_friends(txn, root, month);

      hasInterests_.clear();
      hasInterests_.resize(txn.GetVertexNum(tag_label_id_));
#if OV
      const auto &oe = txn.GetOutgoingEdges<grape::EmptyType>(
          person_label_id_, root, tag_label_id_, hasInterest_label_id_);
      for (auto &e : oe)
      {
        hasInterests_[e.neighbor] = true;
      }
#else
      auto oe = txn.GetOutgoingEdges<grape::EmptyType>(
          person_label_id_, root, tag_label_id_, hasInterest_label_id_);
      for (; oe.is_valid(); oe.next())
      {
        hasInterests_[oe.get_neighbor()] = true;
      }
#endif

      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);
      std::vector<std::pair<int,int>> person_post_index;
      auto post_vids=txn.BatchGetVidsNeighborsWithIndex<grape::EmptyType>(person_label_id_, post_label_id_,  hasCreator_label_id_, friends_list_, person_post_index, false);
      auto post_hasTag_tag_out_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(post_label_id_, tag_label_id_, hasTag_label_id_, post_vids, true);
      for (int i=0;i<friends_list_.size();i++){
        int score = 0;
        for (int j=person_post_index[i].first;j<person_post_index[i].second;j++){
          auto tag_oe=post_hasTag_tag_out_items[j];
          int diff = -1;
          for (int k=0;k<tag_oe.size();k++){
            if (hasInterests_[tag_oe[k]]){
              diff=1;
              break;
            }
          }
          score += diff;
        }
        if (pq.size() < 10)
        {
          pq.emplace(score, friends_list_[i], txn.GetVertexId(person_label_id_, friends_list_[i]));
        }
        else
        {
          const auto &top = pq.top();
          if (score > top.score)
          {
            pq.pop();
            pq.emplace(score, friends_list_[i], txn.GetVertexId(person_label_id_, friends_list_[i]));
          }
          else if (score == top.score)
          {
            oid_t person_id = txn.GetVertexId(person_label_id_, friends_list_[i]);
            if (person_id < top.person_id)
            {
              pq.pop();
              pq.emplace(score, friends_list_[i], person_id);
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
        res_person_vids.emplace_back(pq.top().person_vid);
        pq.pop();
      }
      auto person_isLocatedIn_place_out_items=txn.BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(person_label_id_, place_label_id_, isLocatedIn_label_id_, res_person_vids, true);
      std::vector<vid_t> place_vids;
      for (int i=0;i<res_person_vids.size();i++){
        assert(txn.check_edge_exist(person_isLocatedIn_place_out_items[i]));
        place_vids.emplace_back(person_isLocatedIn_place_out_items[i][0].first);
      }
      auto place_name_col_items=txn.BatchGetVertexPropsFromVids(place_label_id_, place_vids, {&place_name_col_});
      auto person_props=txn.BatchGetVertexPropsFromVids(person_label_id_, res_person_vids, {&person_firstName_col_, &person_lastName_col_, &person_gender_col_});
      for (auto i = vec.size(); i > 0; i--)
      {
        auto &v = vec[i - 1];
        output.put_long(v.person_id);
        auto item=person_props[0][i-1];
        output.put_buffer_object(item);
        item = person_props[1][i-1];
        output.put_buffer_object(item);

        output.put_int(v.score);
        item = person_props[2][i-1];
        output.put_buffer_object(item);

        item = place_name_col_items[0][i-1];
        output.put_buffer_object(item);
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t isLocatedIn_label_id_;
    label_t place_label_id_;
    label_t knows_label_id_;
    label_t tag_label_id_;
    label_t post_label_id_;
    label_t hasTag_label_id_;
    label_t hasInterest_label_id_;
    label_t hasCreator_label_id_;

    StringColumn &person_firstName_col_;
    StringColumn &person_lastName_col_;
    DateColumn &person_birthday_col_;
    StringColumn &person_gender_col_;
    StringColumn &place_name_col_;

    std::vector<vid_t> friends_list_;
    std::vector<bool> friends_set_;

    std::vector<bool> hasInterests_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC10 *app = new gs::IC10(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC10 *casted = static_cast<gs::IC10 *>(app);
    delete casted;
  }
}

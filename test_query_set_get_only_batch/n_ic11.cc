#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{
  class IC11 : public AppBase
  {
  public:
    IC11(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          organisation_label_id_(
              graph.schema().get_vertex_label_id("ORGANISATION")),
          workAt_label_id_(graph.schema().get_edge_label_id("WORKAT")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          organisation_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(organisation_label_id_, "name")))),
          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          place_num_(graph.graph().vertex_num(place_label_id_)),
          graph_(graph) {}

    ~IC11() {}

    void mark_1d_2d_friends(const ReadTransaction &txn, vid_t root)
    {
      std::vector<vid_t> tmp;

      auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto ie = person_knows_person_in.get_edges(root);
      for (; ie.is_valid(); ie.next())
      {
        friends_[ie.get_neighbor()] = true;
        tmp.push_back(ie.get_neighbor());
      }
      auto oe = person_knows_person_out.get_edges(root);
      for (; oe.is_valid(); oe.next())
      {
        friends_[oe.get_neighbor()] = true;
        tmp.push_back(oe.get_neighbor());
      }
      auto person_knows_person_in_items=txn.BatchGetVidsNeighbors<Date>(person_label_id_, person_label_id_, knows_label_id_, tmp, false);
      auto person_knows_person_out_items=txn.BatchGetVidsNeighbors<Date>(person_label_id_, person_label_id_, knows_label_id_, tmp, true);
      for (int i=0;i<tmp.size();i++){
        auto ie2 = person_knows_person_in_items[i];
        for (int j=0;j<ie2.size();j++){
          friends_[ie2[j]] = true;
        }
        auto oe2 = person_knows_person_out_items[i];
        for (int j=0;j<oe2.size();j++){
          friends_[oe2[j]] = true;
        }
      }
      friends_[root] = false;
    }

    struct person_info
    {
#if OV
      person_info(vid_t person_vid_, int workfrom_, oid_t person_id_,
                  const std::string_view &company_name_)
          : person_vid(person_vid_),
            workfrom(workfrom_),
            person_id(person_id_),
            company_name(company_name_) {}

      vid_t person_vid;
      int workfrom;
      oid_t person_id;
      std::string_view company_name;
#else
      person_info(vid_t person_vid_, int workfrom_, oid_t person_id_,
                  gbp::BufferBlock company_name_)
          : person_vid(person_vid_),
            workfrom(workfrom_),
            person_id(person_id_),
            company_name(company_name_) {}

      vid_t person_vid;
      int workfrom;
      oid_t person_id;
      gbp::BufferBlock company_name;
#endif
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.workfrom < rhs.workfrom)
        {
          return true;
        }
        if (lhs.workfrom > rhs.workfrom)
        {
          return false;
        }
        if (lhs.person_id < rhs.person_id)
        {
          return true;
        }
        if (lhs.person_id > rhs.person_id)
        {
          return false;
        }
#if OV
        return lhs.company_name > rhs.company_name;
#else
        return lhs.company_name > rhs.company_name;
#endif
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      std::string_view countryname = input.get_string();
      int workfromyear = input.get_int();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      friends_.clear();
      friends_.resize(txn.GetVertexNum(person_label_id_));
      mark_1d_2d_friends(txn, root);
      vid_t country_id = place_num_;
      for (vid_t i = 0; i < place_num_; ++i)
      {
#if OV
        if (place_name_col_.get_view(i) == countryname)
#else
        auto place_name_item = place_name_col_.get(i);
        if (place_name_item == countryname)
#endif
        {
          country_id = i;
          break;
        }
      }
      assert(country_id != place_num_);

      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);

      auto person_workAt_organisation_in = txn.GetIncomingGraphView<int>(
          organisation_label_id_, person_label_id_, workAt_label_id_);
      auto ie = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, country_id, organisation_label_id_,
          isLocatedIn_label_id_);
      std::vector<vid_t> organisation_vids;
      for(;ie.is_valid();ie.next()){
        organisation_vids.push_back(ie.get_neighbor());
      }
      auto person_workAt_organisation_in_items=txn.BatchGetEdgePropsFromSrcVids<int>(organisation_label_id_,person_label_id_,  workAt_label_id_, organisation_vids, false);
      // auto organisation_name_col_items=txn.BatchGetVertexPropsFromVids(organisation_label_id_, organisation_vids, {organisation_name_col_});
      // auto organisation_name_col_=txn.GetVertexProp(organisation_label_id_, organisation_vids[0]);
      for(int i=0;i<organisation_vids.size();i++){
        auto company=organisation_vids[i];
        auto person_ie=person_workAt_organisation_in_items[i];
        for(int j=0;j<person_ie.size();j++){
          // auto name=organisation_name_col_items[0][i];
          auto name=organisation_name_col_.get(company);
          auto data_item=person_ie[j].second;
          auto wf=data_item;
          if (wf < workfromyear)
          {
            auto u = person_ie[j].first;
            if (friends_[u])
            {
              if (pq.size() < 10)
              {
                pq.emplace(u, wf, txn.GetVertexId(person_label_id_, u), name);
              }
              else
              {
                const auto &top = pq.top();
                if (wf < top.workfrom)
                {
                  pq.pop();
                  pq.emplace(u, wf, txn.GetVertexId(person_label_id_, u), name);
                }
                else if (wf == top.workfrom)
                {
                  oid_t other_person_id = txn.GetVertexId(person_label_id_, u);
                  if ((other_person_id < top.person_id) ||
                      (other_person_id == top.person_id &&
                       name > top.company_name))
                  {
                    pq.pop();
                    pq.emplace(u, wf, other_person_id, name);
                  }
                }
              }
            }
          }
        }
      }

      std::vector<person_info> tmp;
      tmp.reserve(pq.size());
      std::vector<vid_t> person_vids;
      while (!pq.empty())
      {
        tmp.emplace_back(pq.top());
        person_vids.push_back(pq.top().person_vid);
        pq.pop();
      }
      auto person_property_items=txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, {&person_firstName_col_, &person_lastName_col_});

      for (auto i = tmp.size(); i > 0; i--)
      {
        const auto &p = tmp[i - 1];
        output.put_long(p.person_id);
        auto firstname = person_property_items[0][i-1];
        output.put_buffer_object(firstname);
        auto lastname = person_property_items[1][i-1];
        output.put_buffer_object(lastname);
        output.put_buffer_object(p.company_name);
        output.put_int(p.workfrom);
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t place_label_id_;
    label_t isLocatedIn_label_id_;
    label_t organisation_label_id_;
    label_t workAt_label_id_;

    label_t knows_label_id_;

    StringColumn &person_firstName_col_;
    StringColumn &person_lastName_col_;
    StringColumn &organisation_name_col_;
    StringColumn &place_name_col_;

    vid_t place_num_;

    std::vector<bool> friends_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC11 *app = new gs::IC11(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC11 *casted = static_cast<gs::IC11 *>(app);
    delete casted;
  }
}

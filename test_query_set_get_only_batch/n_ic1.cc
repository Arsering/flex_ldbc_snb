#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IC1 : public AppBase
  {
  public:
    IC1(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          organisation_label_id_(
              graph.schema().get_vertex_label_id("ORGANISATION")),
          workAt_label_id_(graph.schema().get_edge_label_id("WORKAT")),
          studyAt_label_id_(graph.schema().get_edge_label_id("STUDYAT")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          person_birthday_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(person_label_id_, "birthday")))),
          person_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(person_label_id_,
                                               "creationDate")))),
          person_gender_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "gender")))),
          person_browserUsed_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_,
                                               "browserUsed")))),
          person_locationIp_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "locationIP")))),
          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          person_email_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "email")))),
          person_language_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "language")))),
          organisation_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(organisation_label_id_, "name")))),
          graph_(graph) {}
    ~IC1() {}

    struct person_info
    {
      person_info(uint8_t distance_, gbp::BufferBlock lastName_, oid_t id_,
                  vid_t vid_)
          : distance(distance_), lastName(lastName_), id(id_), vid(vid_) {}

      uint8_t distance;
      gbp::BufferBlock lastName;
      oid_t id;
      vid_t vid;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {

        if (lhs.distance < rhs.distance)
        {
          return false;
        }
        if (lhs.distance > rhs.distance)
        {
          return true;
        }

        if (lhs.lastName < rhs.lastName)
        {
          return false;
        }
        if (lhs.lastName > rhs.lastName)
        {
          return true;
        }
        return lhs.id > rhs.id;
      }
    };

    void get_friends(const ReadTransaction &txn, vid_t root,
                     const std::string_view &firstname)
    {
      std::vector<person_info> result_items;
      std::vector<vid_t> result_vids;

      // std::vector<vid_t> oneD_result_vids;
      // std::vector<vid_t> twoD_result_vids;
      person_info_comparer comparer;

      std::vector<bool> bitset(txn.GetVertexNum(person_label_id_), false);
      std::vector<vid_t> neighbors;
      bitset[root] = true;
      std::vector<size_t> level_sizes;

      bool mark = true;
      for (size_t level = 0; level < 3; level++)
      {

        if (level == 0)
        {
          auto person_knows_person_out =
              txn.GetOutgoingGraphView<Date>(person_label_id_, person_label_id_, knows_label_id_);
          auto person_knows_person_in =
              txn.GetIncomingGraphView<Date>(person_label_id_, person_label_id_, knows_label_id_);
          for (auto ie = person_knows_person_in.get_edges(root); ie.is_valid(); ie.next())
          {
            auto v = ie.get_neighbor();
            if (!bitset[v])
            {
              bitset[v] = true;
              neighbors.push_back(v);
            }
          }
          for (auto oe = person_knows_person_out.get_edges(root); oe.is_valid(); oe.next())
          {
            auto v = oe.get_neighbor();
            if (!bitset[v])
            {
              bitset[v] = true;
              neighbors.push_back(v);
            }
          }
        }
        else
        {
          auto person_knows_person_in_items = txn.BatchGetIncomingEdges<Date>(person_label_id_, person_label_id_, knows_label_id_, neighbors);
          auto person_knows_person_out_items = txn.BatchGetOutgoingEdges<Date>(person_label_id_, person_label_id_, knows_label_id_, neighbors);
          neighbors.clear();
          for (size_t i = 0; i < person_knows_person_in_items.size(); i++)
          {
            for (auto &e = person_knows_person_in_items[i]; e.is_valid(); e.next())
            {
              auto v = e.get_neighbor();
              if (!bitset[v])
              {
                bitset[v] = true;
                neighbors.push_back(v);
              }
            }
            for (auto &e = person_knows_person_out_items[i]; e.is_valid(); e.next())
            {
              auto v = e.get_neighbor();
              if (!bitset[v])
              {
                bitset[v] = true;
                neighbors.push_back(v);
              }
            }
          }
        }

        auto person_propertys_items = txn.BatchGetVertexPropsFromVids(person_label_id_, neighbors, {"firstName"});
        size_t level_size = 0;
        for (size_t i = 0; i < neighbors.size(); i++)
        {
          if (person_propertys_items[0][i] == firstname)
          {
            result_vids.emplace_back(neighbors[i]);
            level_size++;
          }
        }
        level_sizes.push_back(level_size);

        if (result_vids.size() >= 20)
        {
          break;
        }
      }

      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);
      auto person_lastName_items = txn.BatchGetVertexPropsFromVids(person_label_id_, result_vids, {"lastName"});
      size_t result_vids_idx = 0;
      for (size_t i = 0; i < level_sizes.size(); i++)
      {
        for (size_t j = 0; j < level_sizes[i]; j++)
        {
          pq.emplace(i + 2, person_lastName_items[0][result_vids_idx],
                     1, result_vids[result_vids_idx]);
          result_vids_idx++;
        }
      }

      result_vids.clear();
      if ((!pq.empty()))
      {
        result_items.emplace_back(pq.top());
        result_vids.push_back(pq.top().vid);

        pq.pop();
      }

      while (!pq.empty())
      {
        if (result_items.size() < 20 || (pq.top().distance == result_items.back().distance && pq.top().lastName == result_items.back().lastName))
        {
          result_items.emplace_back(pq.top());
          result_vids.push_back(pq.top().vid);
          pq.pop();
        }
        else
        {
          break;
        }
      }

      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq_final(comparer);

      auto person_ids = txn.BatchGetVertexIds(person_label_id_, result_vids);

      for (size_t i = 0; i < result_items.size(); i++)
      {
        result_items[i].id = person_ids[i];
        pq_final.emplace(result_items[i]);
      }
      ans_.reserve(std::min(pq_final.size(), (size_t)20));

      while (!pq_final.empty() && ans_.size() < 20)
      {
        ans_.emplace_back(pq_final.top());
        pq_final.pop();
      }
    }

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t person_id = input.get_long();
      std::string_view firstname = input.get_string();
      CHECK(input.empty());

      ans_.clear();
      distance_.clear();
      distance_.resize(txn.GetVertexNum(person_label_id_), 0);
      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, person_id, root))
      {
        return false;
      }

      get_friends(txn, root, firstname);

      std::vector<vid_t> person_vids;
      for (auto &info : ans_)
      {
        person_vids.push_back(info.vid);
      }

      std::vector<std::string> person_property_names = {"birthday", "creationDate", "gender", "browserUsed", "locationIP", "email", "language"};
      auto person_propertys_items = txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, person_property_names);
      auto person_isLocatedIn_place_out_items = txn.BatchGetOutgoingSingleEdges(person_label_id_, place_label_id_, isLocatedIn_label_id_, person_vids);
      auto person_studyAt_organisation_out_items = txn.BatchGetOutgoingEdges<int>(person_label_id_, organisation_label_id_, studyAt_label_id_, person_vids);
      auto person_workAt_organisation_out_items = txn.BatchGetOutgoingEdges<int>(person_label_id_, organisation_label_id_, workAt_label_id_, person_vids);
      std::vector<vid_t> university_vids;
      std::vector<vid_t> company_vids;
      std::vector<size_t> university_nums;
      std::vector<size_t> company_nums;

      for (size_t i = 0; i < ans_.size(); i++)
      {
        university_nums.emplace_back(0);
        auto &universities = person_studyAt_organisation_out_items[i];
        for (; universities.is_valid(); universities.next())
        {
          university_vids.push_back(universities.get_neighbor());
          university_nums.back()++;
        }
        company_nums.emplace_back(0);
        auto &companies = person_workAt_organisation_out_items[i];
        for (; companies.is_valid(); companies.next())
        {
          company_vids.push_back(companies.get_neighbor());
          company_nums.back()++;
        }
      }

      auto universitie_isLocatedIn_place_out_items = txn.BatchGetOutgoingSingleEdges(organisation_label_id_, place_label_id_, isLocatedIn_label_id_, university_vids);
      auto company_isLocatedIn_place_out_items = txn.BatchGetOutgoingSingleEdges(organisation_label_id_, place_label_id_, isLocatedIn_label_id_, company_vids);

      std::vector<vid_t> place_vids;
      place_vids.reserve(university_vids.size() + company_vids.size() + 1);
      size_t university_idx = 0, company_idx = 0;
      for (size_t i = 0; i < ans_.size(); i++)
      {
        auto &place_item = gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(person_isLocatedIn_place_out_items[i]); // get place name
        assert(place_item.timestamp.load() <= txn.timestamp());
        place_vids.push_back(place_item.neighbor);

        for (auto idx = 0; idx < university_nums[i]; idx++)
        {
          place_vids.push_back(gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(universitie_isLocatedIn_place_out_items[university_idx++]).neighbor);
        }
        for (auto idx = 0; idx < company_nums[i]; idx++)
        {
          place_vids.push_back(gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(company_isLocatedIn_place_out_items[company_idx++]).neighbor);
        }
      }

      auto place_name_items = txn.BatchGetVertexPropsFromVids(place_label_id_, place_vids, {"name"});
      auto university_name_items = txn.BatchGetVertexPropsFromVids(organisation_label_id_, university_vids, {"name"});
      auto company_names_items = txn.BatchGetVertexPropsFromVids(organisation_label_id_, company_vids, {"name"});

      size_t place_idx = 0;
      university_idx = company_idx = 0;
      for (size_t i = 0; i < ans_.size(); i++)
      {
        auto &info = ans_[i];
        auto v = info.vid;
        output.put_long(info.id);
        output.put_int(info.distance - 1);
        output.put_buffer_object(info.lastName);
        output.put_long(gbp::BufferBlock::RefSingle<gs::Date>(person_propertys_items[0][i]).milli_second);
        output.put_long(gbp::BufferBlock::RefSingle<gs::Date>(person_propertys_items[1][i]).milli_second);
        output.put_buffer_object(person_propertys_items[2][i]);     // gender
        output.put_buffer_object(person_propertys_items[3][i]);     // browserUsed
        output.put_buffer_object(person_propertys_items[4][i]);     // locationIp
        output.put_buffer_object(place_name_items[0][place_idx++]); // end get place name
        output.put_buffer_object(person_propertys_items[5][i]);     // email
        output.put_buffer_object(person_propertys_items[6][i]);     // language

        size_t un_offset = output.skip_int();
        auto &universities = person_studyAt_organisation_out_items[i];
        for (universities.recover(); universities.is_valid(); universities.next())
        {
          output.put_buffer_object(university_name_items[0][university_idx++]);
          output.put_int(*((int *)universities.get_data()));
          output.put_buffer_object(place_name_items[0][place_idx++]);
        }
        output.put_int_at(un_offset, university_nums[i]);

        size_t cn_offset = output.skip_int();
        auto &companies = person_workAt_organisation_out_items[i];
        for (companies.recover(); companies.is_valid(); companies.next())
        {
          output.put_buffer_object(company_names_items[0][company_idx++]);
          output.put_int(*((int *)companies.get_data()));
          output.put_buffer_object(place_name_items[0][place_idx++]);
        }
        output.put_int_at(cn_offset, company_nums[i]);
      }
      ans_.clear();
      return true;
    }

  private:
    label_t person_label_id_;
    label_t place_label_id_;
    label_t knows_label_id_;
    label_t isLocatedIn_label_id_;
    label_t organisation_label_id_;
    label_t workAt_label_id_;
    label_t studyAt_label_id_;

    std::vector<person_info> ans_;
    std::vector<uint8_t> distance_;
    StringColumn &person_firstName_col_;
    StringColumn &person_lastName_col_;
    DateColumn &person_birthday_col_;
    DateColumn &person_creationDate_col_;
    StringColumn &person_gender_col_;
    StringColumn &person_browserUsed_col_;
    StringColumn &person_locationIp_col_;
    StringColumn &place_name_col_;
    StringColumn &person_email_col_;
    StringColumn &person_language_col_;
    StringColumn &organisation_name_col_;

    GraphDBSession &graph_;

#ifdef ZED_PROFILE
    int person_count = 0;
    int edge_count = 0;
#endif
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC1 *app = new gs::IC1(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC1 *casted = static_cast<gs::IC1 *>(app);
    delete casted;
  }
}
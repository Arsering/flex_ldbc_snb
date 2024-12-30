#include <fstream>
#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

// #define ZED_PROFILE

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
          person_firstName_col_(graph.GetPropertyHandle(person_label_id_, "firstName")),
          person_lastName_col_(graph.GetPropertyHandle(person_label_id_, "lastName")),
          person_birthday_col_(graph.GetPropertyHandle(person_label_id_, "birthday")),
          person_creationDate_col_(graph.GetPropertyHandle(person_label_id_, "creationDate")),
          person_gender_col_(graph.GetPropertyHandle(person_label_id_, "gender")),
          person_browserUsed_col_(graph.GetPropertyHandle(person_label_id_, "browserUsed")),
          person_locationIp_col_(graph.GetPropertyHandle(person_label_id_, "locationIP")),
          place_name_col_(graph.GetPropertyHandle(place_label_id_, "name")),
          person_email_col_(graph.GetPropertyHandle(person_label_id_, "email")),
          person_language_col_(graph.GetPropertyHandle(person_label_id_, "language")),
          organisation_name_col_(graph.GetPropertyHandle(organisation_label_id_, "name")),
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
      bool operator()(person_info &lhs,person_info &rhs)
      {

        if (lhs.distance < rhs.distance)
        {
          return true;
        }
        if (lhs.distance > rhs.distance)
        {
          return false;
        }

        if (lhs.lastName < rhs.lastName)
        {
          return true;
        }
        if (lhs.lastName > rhs.lastName)
        {
          return false;
        }
        return lhs.id < rhs.id;
      }
    };

    void get_friends( ReadTransaction &txn, vid_t root,
                      std::string_view &firstname)
    {
      auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);

      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);
      uint8_t dep = 1;
      distance_[root] = 1;
      std::queue<vid_t> q;
      q.push(root);

      // 用于记录每层访问的节点
      std::vector<vid_t> current_level_nodes;
      int current_depth = 1;
      
      while (!q.empty())
      {
        auto u = q.front();
        q.pop();
        if (dep != distance_[u])
        {
          if (pq.size() == 20)
            break;
          dep = distance_[u];

          // 打印上一层访问的节点
          // std::ofstream ofs("level_nodes.txt", std::ios::app);
          // ofs << "Depth " << current_depth << ": ";
          // for ( auto& vid : current_level_nodes) {
          //   ofs << vid << " ";
          // }
          // ofs << "\n";
          // ofs.close();

          current_level_nodes.clear();
          current_depth++;
        }
        auto ie = person_knows_person_in.get_edges(u);
        for (; ie.is_valid(); ie.next())
        {
          auto v = ie.get_neighbor();
          if (distance_[v])
            continue;
          #ifdef ZED_PROFILE
          person_count+=1;
          #endif
          distance_[v] = distance_[u] + 1;
          if (distance_[v] < 4)
          {
            q.push(v);
          }
          auto person_firstName_item = person_firstName_col_.getProperty(v);
          if (person_firstName_item == firstname)
          {
            if (pq.size() < 20)
            {
              pq.emplace(distance_[v], person_lastName_col_.getProperty(v),
                         txn.GetVertexId(person_label_id_, v), v);
            }
            else
            {
              const person_info &top = pq.top();
              uint8_t distance = distance_[v];
              if (distance < top.distance)
              {
                pq.emplace(distance, person_lastName_col_.getProperty(v),
                           txn.GetVertexId(person_label_id_, v), v);
              }
              else if (distance == top.distance)
              {
                auto lastName_item = person_lastName_col_.getProperty(v);

                if (lastName_item < top.lastName)
                {
                  pq.pop();
                  pq.emplace(distance, lastName_item,
                             txn.GetVertexId(person_label_id_, v), v);
                }
                else if (lastName_item == top.lastName)
                {
                  oid_t id = txn.GetVertexId(person_label_id_, v);
                  if (id < top.id)
                  {
                    pq.pop();
                    pq.emplace(distance, lastName_item, id, v);
                  }
                }
              }
            }
          }
        }
        auto oe = person_knows_person_out.get_edges(u);
        for (; oe.is_valid(); oe.next())
        {
          auto v = oe.get_neighbor();
          if (distance_[v])
            continue;
          distance_[v] = distance_[u] + 1;
          if (distance_[v] < 4)
          {
            q.push(v);
          }
          auto person_firstName_item = person_firstName_col_.getProperty(v);
          if (person_firstName_item == firstname)
          {
            if (pq.size() < 20)
            {
              pq.emplace(distance_[v], person_lastName_col_.getProperty(v),
                         txn.GetVertexId(person_label_id_, v), v);
            }
            else
            {
              const person_info &top = pq.top();
              uint8_t distance = distance_[v];
              if (distance < top.distance)
              {
                pq.pop();
                pq.emplace(distance, person_lastName_col_.getProperty(v),
                           txn.GetVertexId(person_label_id_, v), v);
              }
              else if (distance == top.distance)
              {
                auto lastName_item = person_lastName_col_.getProperty(v);
                if (lastName_item < top.lastName)
                {
                  pq.pop();
                  pq.emplace(distance, lastName_item,
                             txn.GetVertexId(person_label_id_, v), v);
                }
                else if (lastName_item == top.lastName)
                {
                  oid_t id = txn.GetVertexId(person_label_id_, v);
                  if (id < top.id)
                  {
                    pq.pop();
                    pq.emplace(distance, lastName_item, id, v);
                  }
                }
              }
            }
          }
        }

        // 记录当前访问的节点
        current_level_nodes.push_back(u);
      }

      ans_.reserve(pq.size());
      while (!pq.empty())
      {
        ans_.emplace_back(pq.top());
        pq.pop();
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
        assert(false);
        return false;
      }

      get_friends(txn, root, firstname);

      auto person_isLocatedIn_place_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              person_label_id_, place_label_id_, isLocatedIn_label_id_);
      auto person_studyAt_organisation_out = txn.GetOutgoingGraphView<int>(
          person_label_id_, organisation_label_id_, studyAt_label_id_);
      auto person_workAt_organisation_out = txn.GetOutgoingGraphView<int>(
          person_label_id_, organisation_label_id_, workAt_label_id_);
      auto organisation_isLocatedIn_place_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              organisation_label_id_, place_label_id_, isLocatedIn_label_id_);
      for (size_t i = ans_.size(); i > 0; i--)
      {
        auto &info = ans_[i - 1];
        auto v = info.vid;
        output.put_long(info.id);
        output.put_int(info.distance - 1);

        output.put_buffer_object(info.lastName);
        auto item = person_birthday_col_.getProperty(v);
        output.put_long(gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second);
        item = person_creationDate_col_.getProperty(v);
        output.put_long(gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second);
        item = person_gender_col_.getProperty(v);
        output.put_buffer_object(item);
        item = person_browserUsed_col_.getProperty(v);
        output.put_buffer_object(item);
        item = person_locationIp_col_.getProperty(v);
        output.put_buffer_object(item);
        item = person_isLocatedIn_place_out.get_edge(v);
        assert(person_isLocatedIn_place_out.exist1(item));
        auto person_place = gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(item).neighbor;
        item = place_name_col_.getProperty(person_place);
        output.put_buffer_object(item);
        item = person_email_col_.getProperty(v);
        output.put_buffer_object(item);
        item = person_language_col_.getProperty(v);
        output.put_buffer_object(item);
        int university_num = 0;
        size_t un_offset = output.skip_int();
        auto universities = person_studyAt_organisation_out.get_edges(v);
        for (; universities.is_valid(); universities.next())
        {
          auto item = organisation_name_col_.getProperty(universities.get_neighbor());
          output.put_buffer_object(item);
          auto item_t = universities.get_data();
          output.put_int(*((int *)item_t));
          item = organisation_isLocatedIn_place_out.get_edge(universities.get_neighbor());
          assert(organisation_isLocatedIn_place_out.exist1(item));

          auto univ_place =
              gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(item).neighbor;
          item = place_name_col_.getProperty(univ_place);
          output.put_buffer_object(item);
          university_num++;
        }
        output.put_int_at(un_offset, university_num);
        int company_num = 0;
        size_t cn_offset = output.skip_int();
        auto companies = person_workAt_organisation_out.get_edges(v);
        for (; companies.is_valid(); companies.next())
        {
          auto item = organisation_name_col_.getProperty(companies.get_neighbor());
          output.put_buffer_object(item);
          auto item_t = companies.get_data();
          output.put_int(*((int *)item_t));
          item = organisation_isLocatedIn_place_out.get_edge(companies.get_neighbor());
          assert(organisation_isLocatedIn_place_out.exist1(item));

          auto company_place =
              gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(item).neighbor;
          item = place_name_col_.getProperty(company_place);
          output.put_buffer_object(item);
          company_num++;
        }
        output.put_int_at(cn_offset, company_num);
      }
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

    cgraph::PropertyHandle person_firstName_col_;
    cgraph::PropertyHandle person_lastName_col_;
    cgraph::PropertyHandle person_birthday_col_;
    cgraph::PropertyHandle person_creationDate_col_;
    cgraph::PropertyHandle person_gender_col_;
    cgraph::PropertyHandle person_browserUsed_col_;
    cgraph::PropertyHandle person_locationIp_col_;
    cgraph::PropertyHandle place_name_col_;
    cgraph::PropertyHandle person_email_col_;
    cgraph::PropertyHandle person_language_col_;
    cgraph::PropertyHandle organisation_name_col_;

    GraphDBSession &graph_;

    #ifdef ZED_PROFILE
    int person_count=0;
    int edge_count=0;
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

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
#if OV
      person_info(uint8_t distance_, const std::string_view &lastName_, oid_t id_,
                  vid_t vid_)
          : distance(distance_), lastName(lastName_), id(id_), vid(vid_) {}
#else
      person_info(uint8_t distance_, std::string lastName_, oid_t id_,
                  vid_t vid_)
          : distance(distance_), lastName(std::move(lastName_)), id(id_), vid(vid_) {}
#endif
      uint8_t distance;
#if OV
      std::string_view lastName;
#else
      std::string lastName;
#endif
      oid_t id;
      vid_t vid;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
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

    void get_friends(const ReadTransaction &txn, vid_t root,
                     const std::string_view &firstname)
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
      while (!q.empty())
      {
        auto u = q.front();
        q.pop();
        if (dep != distance_[u])
        {
          if (pq.size() == 20)
            break;
          dep = distance_[u];
        }

#if OV
        const auto &ie = person_knows_person_in.get_edges(u);
        for (auto &e : ie)
        {
          auto v = e.neighbor;
#else
        auto ie = person_knows_person_in.get_edges(u);
        for (; ie.is_valid(); ie.next())
        {
          auto v = ie.get_neighbor();
#endif

          if (distance_[v])
            continue;
          distance_[v] = distance_[u] + 1;
          if (distance_[v] < 4)
          {
            q.push(v);
          }
#if OV
          if (person_firstName_col_.get_view(v) == firstname)
#else
          const auto firstname_in_db = person_firstName_col_.get(root);
          std::string_view str1{firstname_in_db.Data(), firstname_in_db.Size()};
          if (str1 == firstname)
#endif
          {
            if (pq.size() < 20)
            {
#if OV
              pq.emplace(distance_[v], person_lastName_col_.get_view(v),
                         txn.GetVertexId(person_label_id_, v), v);
#else
              auto lastName_in_db = person_lastName_col_.get(v);
              // TODO:这是一个浅复制还是深复制？？
              std::string str1{lastName_in_db.Data(), lastName_in_db.Size()};
              pq.emplace(distance_[v], std::move(str1),
                         txn.GetVertexId(person_label_id_, v), v);
#endif
            }
            else
            {
              const person_info &top = pq.top();
              uint8_t distance = distance_[v];
              if (distance < top.distance)
              {
#if OV
                pq.emplace(distance, person_lastName_col_.get_view(v),
                           txn.GetVertexId(person_label_id_, v), v);
#else
                auto lastName_in_db = person_lastName_col_.get(v);
                // TODO:这是一个浅复制还是深复制？？
                std::string str1{lastName_in_db.Data(), lastName_in_db.Size()};
                pq.emplace(distance, std::move(str1),
                           txn.GetVertexId(person_label_id_, v), v);
#endif
              }
              else if (distance == top.distance)
              {

#if OV
                std::string_view lastName = person_lastName_col_.get_view(v);
#else
                auto lastName_in_db = person_lastName_col_.get(v);
                std::string lastName = {lastName_in_db.Data(), lastName_in_db.Size()};
#endif
                if (lastName < top.lastName)
                {
                  pq.pop();
                  pq.emplace(distance, std::move(lastName),
                             txn.GetVertexId(person_label_id_, v), v);
                }
                else if (lastName == top.lastName)
                {
                  oid_t id = txn.GetVertexId(person_label_id_, v);
                  if (id < top.id)
                  {
                    pq.pop();
                    pq.emplace(distance, std::move(lastName), id, v);
                  }
                }
              }
            }
          }
        }

#if OV
        const auto &oe = person_knows_person_out.get_edges(u);
        for (auto &e : oe)
        {
          auto v = e.neighbor;
#else
        auto oe = person_knows_person_out.get_edges(u);
        for (; oe.is_valid(); oe.next())
        {
          auto v = oe.get_neighbor();
#endif
          if (distance_[v])
            continue;
          distance_[v] = distance_[u] + 1;
          if (distance_[v] < 4)
          {
            q.push(v);
          }
#if OV
          if (person_firstName_col_.get_view(v) == firstname)
#else
          auto firstName_in_db = person_firstName_col_.get(v);
          // TODO:这是一个浅复制还是深复制？？
          std::string_view str1{firstName_in_db.Data(), firstName_in_db.Size()};
          if (str1 == firstname)
#endif
          {
            if (pq.size() < 20)
            {
#if OV
              pq.emplace(distance_[v], person_lastName_col_.get_view(v),
                         txn.GetVertexId(person_label_id_, v), v);
#else
              auto lastName_in_db = person_lastName_col_.get(v);
              // TODO:这是一个浅复制还是深复制？？
              std::string str1{lastName_in_db.Data(), lastName_in_db.Size()};
              pq.emplace(distance_[v], std::move(str1),
                         txn.GetVertexId(person_label_id_, v), v);
#endif
            }
            else
            {
              const person_info &top = pq.top();
              uint8_t distance = distance_[v];
              if (distance < top.distance)
              {
                pq.pop();
#if OV
                pq.emplace(distance, person_lastName_col_.get_view(v),
                           txn.GetVertexId(person_label_id_, v), v);
#else
                auto lastName_in_db = person_lastName_col_.get(v);
                // TODO:这是一个浅复制还是深复制？？
                std::string str1{lastName_in_db.Data(), lastName_in_db.Size()};
                pq.emplace(distance, std::move(str1),
                           txn.GetVertexId(person_label_id_, v), v);
#endif
              }
              else if (distance == top.distance)
              {
#if OV
                std::string_view lastName = person_lastName_col_.get_view(v);
#else
                auto lastName_in_db = person_lastName_col_.get(v);
                std::string lastName = {lastName_in_db.Data(), lastName_in_db.Size()};
#endif
                if (lastName < top.lastName)
                {
                  pq.pop();
                  pq.emplace(distance, std::move(lastName),
                             txn.GetVertexId(person_label_id_, v), v);
                }
                else if (lastName == top.lastName)
                {
                  oid_t id = txn.GetVertexId(person_label_id_, v);
                  if (id < top.id)
                  {
                    pq.pop();
                    pq.emplace(distance, std::move(lastName), id, v);
                  }
                }
              }
            }
          }
        }
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
#if !OV
      gbp::BufferObject tmp;
#endif
      for (size_t i = ans_.size(); i > 0; i--)
      {
        auto &info = ans_[i - 1];
        auto v = info.vid;
        output.put_long(info.id);
        output.put_int(info.distance - 1);
        output.put_string_view(info.lastName);
#if OV
        output.put_long(person_birthday_col_.get_view(v).milli_second);
        output.put_long(person_creationDate_col_.get_view(v).milli_second);
        output.put_string_view(person_gender_col_.get_view(v));
        output.put_string_view(person_browserUsed_col_.get_view(v));
        output.put_string_view(person_locationIp_col_.get_view(v));
#else
        tmp = person_birthday_col_.get(v);
        output.put_long(gbp::Decode<gs::Date>(tmp).milli_second);
        tmp = person_creationDate_col_.get(v);
        output.put_long(gbp::Decode<gs::Date>(tmp).milli_second);
        tmp = person_gender_col_.get(v);
        output.put_string_view({tmp.Data(), tmp.Size()});
        tmp = person_browserUsed_col_.get(v);
        output.put_string_view({tmp.Data(), tmp.Size()});
        tmp = person_locationIp_col_.get(v);
        output.put_string_view({tmp.Data(), tmp.Size()});
#endif
        assert(person_isLocatedIn_place_out.exist(v));

#if OV
        auto person_place = person_isLocatedIn_place_out.get_edge(v).neighbor;
        output.put_string_view(place_name_col_.get_view(person_place));
        output.put_string_view(person_email_col_.get_view(v));
        output.put_string_view(person_language_col_.get_view(v));
#else
        tmp = person_isLocatedIn_place_out.get_edge(v);
        auto person_place = gbp::Decode<MutableNbr<grape::EmptyType>>(tmp).neighbor;
        tmp = place_name_col_.get(person_place);
        output.put_string_view({tmp.Data(), tmp.Size()});
        tmp = person_email_col_.get(v);
        output.put_string_view({tmp.Data(), tmp.Size()});
        tmp = person_language_col_.get(v);
        output.put_string_view({tmp.Data(), tmp.Size()});
#endif

        int university_num = 0;
        size_t un_offset = output.skip_int();
#if OV
        const auto &universities = person_studyAt_organisation_out.get_edges(v);
        for (auto &e1 : universities)
        {
#else
        auto universities = person_studyAt_organisation_out.get_edges(v);
        for (; universities.is_valid(); universities.next())
        {
#endif
#if OV
          output.put_string_view(organisation_name_col_.get_view(e1.neighbor));
          output.put_int(e1.data);
          assert(organisation_isLocatedIn_place_out.exist(e1.neighbor));
          auto univ_place =
              organisation_isLocatedIn_place_out.get_edge(e1.neighbor).neighbor;
          output.put_string_view(place_name_col_.get_view(univ_place));
#else
          tmp = organisation_name_col_.get(universities.get_neighbor());
          output.put_string_view({tmp.Data(), tmp.Size()});
          tmp = universities.get_data();
          output.put_int(gbp::Decode<int>(tmp));
          assert(organisation_isLocatedIn_place_out.exist(universities.get_neighbor()));
          tmp = organisation_isLocatedIn_place_out.get_edge(universities.get_neighbor());
          auto univ_place =
              gbp::Decode<MutableNbr<grape::EmptyType>>(tmp).neighbor;
          tmp = place_name_col_.get(univ_place);
          output.put_string_view({tmp.Data(), tmp.Size()});
#endif
          university_num++;
        }
        output.put_int_at(un_offset, university_num);
        int company_num = 0;
        size_t cn_offset = output.skip_int();
#if OV
        const auto &companies = person_workAt_organisation_out.get_edges(v);
        for (auto &e1 : companies)
        {
          output.put_string_view(organisation_name_col_.get_view(e1.neighbor));
          output.put_int(e1.data);
          assert(organisation_isLocatedIn_place_out.exist(e1.neighbor));
          auto company_place =
              organisation_isLocatedIn_place_out.get_edge(e1.neighbor).neighbor;
          output.put_string_view(place_name_col_.get_view(company_place));
#else
        auto companies = person_workAt_organisation_out.get_edges(v);
        for (; companies.is_valid(); companies.next())
        {
          tmp = organisation_name_col_.get(companies.get_neighbor());
          output.put_string_view({tmp.Data(), tmp.Size()});
          tmp = companies.get_data();
          output.put_int(gbp::Decode<int>(tmp));
          assert(organisation_isLocatedIn_place_out.exist(companies.get_neighbor()));
          tmp = organisation_isLocatedIn_place_out.get_edge(companies.get_neighbor());
          auto company_place =
              gbp::Decode<MutableNbr<grape::EmptyType>>(tmp).neighbor;
          tmp = place_name_col_.get(company_place);
          output.put_string_view({tmp.Data(), tmp.Size()});
#endif
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
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const DateColumn &person_birthday_col_;
    const DateColumn &person_creationDate_col_;
    const StringColumn &person_gender_col_;
    const StringColumn &person_browserUsed_col_;
    const StringColumn &person_locationIp_col_;
    const StringColumn &place_name_col_;
    const StringColumn &person_email_col_;
    const StringColumn &person_language_col_;
    const StringColumn &organisation_name_col_;

    GraphDBSession &graph_;
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

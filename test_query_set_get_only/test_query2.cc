#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class TestQuery2 : public AppBase
  {
  public:
    TestQuery2(GraphDBSession &graph)
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
    ~TestQuery2() {}

    struct person_info
    {
#if OV
      person_info(uint8_t distance_, const std::string_view &lastName_, oid_t id_,
                  vid_t vid_)
          : distance(distance_), lastName(lastName_), id(id_), vid(vid_) {}

      uint8_t distance;

      std::string_view lastName;

      oid_t id;
      vid_t vid;
#else
      person_info(uint8_t distance_, oid_t id_,
                  vid_t vid_)
          : distance(distance_), id(id_), vid(vid_) {}

      uint8_t distance;
      oid_t id;
      vid_t vid;
#endif
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
        return lhs.id < rhs.id;
      }
    };

    void get_friends(const ReadTransaction &txn, vid_t root)
    {
      auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);

      uint8_t distance_upperbound = 3;
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
          if (distance_[v] < distance_upperbound)
          {
            q.push(v);
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
          if (distance_[v] < distance_upperbound)
          {
            q.push(v);
          }
        }
      }
    }

    bool
    Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t person_id = input.get_long();
      CHECK(input.empty());

      ans_.clear();
      distance_.clear();
      distance_.resize(txn.GetVertexNum(person_label_id_), 0);
      vid_t root{};

      if (!txn.GetVertexIndex(person_label_id_, person_id, root))
      {
        return false;
      }

      get_friends(txn, root);
      ans_.clear();

      return true;
      assert(false);
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
#if OV
        output.put_string_view(info.lastName);
        output.put_long(person_birthday_col_.get_view(v).milli_second);
        output.put_long(person_creationDate_col_.get_view(v).milli_second);
        output.put_string_view(person_gender_col_.get_view(v));
        output.put_string_view(person_browserUsed_col_.get_view(v));
        output.put_string_view(person_locationIp_col_.get_view(v));
#else

        auto item = person_birthday_col_.get(v);
        output.put_long(gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second);
        item = person_creationDate_col_.get(v);
        output.put_long(gbp::BufferBlock::RefSingle<gs::Date>(item).milli_second);
        item = person_gender_col_.get(v);
        output.put_buffer_object(item);
        item = person_browserUsed_col_.get(v);
        output.put_buffer_object(item);
        item = person_locationIp_col_.get(v);
        output.put_buffer_object(item);
#endif
#if OV
        assert(person_isLocatedIn_place_out.exist(v));
        auto person_place = person_isLocatedIn_place_out.get_edge(v).neighbor;
        output.put_string_view(place_name_col_.get_view(person_place));
        output.put_string_view(person_email_col_.get_view(v));
        output.put_string_view(person_language_col_.get_view(v));
#else
        item = person_isLocatedIn_place_out.get_edge(v);
        assert(person_isLocatedIn_place_out.exist1(item));
        auto person_place = gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(item).neighbor;
        item = place_name_col_.get(person_place);
        output.put_buffer_object(item);
        item = person_email_col_.get(v);
        output.put_buffer_object(item);
        item = person_language_col_.get(v);
        output.put_buffer_object(item);
#endif
        int university_num = 0;
        size_t un_offset = output.skip_int();
#if OV
        const auto &universities = person_studyAt_organisation_out.get_edges(v);
        for (auto &e1 : universities)
        {
          output.put_string_view(organisation_name_col_.get_view(e1.neighbor));
          output.put_int(e1.data);
          assert(organisation_isLocatedIn_place_out.exist(e1.neighbor));
          auto univ_place =
              organisation_isLocatedIn_place_out.get_edge(e1.neighbor).neighbor;
          output.put_string_view(place_name_col_.get_view(univ_place));
          university_num++;
        }
#else
        auto universities = person_studyAt_organisation_out.get_edges(v);
        for (; universities.is_valid(); universities.next())
        {
          auto item = organisation_name_col_.get(universities.get_neighbor());
          output.put_buffer_object(item);
          auto item_t = universities.get_data();
          output.put_int(*((int *)item_t));
          item = organisation_isLocatedIn_place_out.get_edge(universities.get_neighbor());
          assert(organisation_isLocatedIn_place_out.exist1(item));

          auto univ_place =
              gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(item).neighbor;
          item = place_name_col_.get(univ_place);
          output.put_buffer_object(item);
          university_num++;
        }
#endif
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
          company_num++;
        }
#else
        auto companies = person_workAt_organisation_out.get_edges(v);
        for (; companies.is_valid(); companies.next())
        {
          auto item = organisation_name_col_.get(companies.get_neighbor());
          output.put_buffer_object(item);
          auto item_t = companies.get_data();
          output.put_int(*((int *)item_t));
          item = organisation_isLocatedIn_place_out.get_edge(companies.get_neighbor());
          assert(organisation_isLocatedIn_place_out.exist1(item));

          auto company_place =
              gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(item).neighbor;
          item = place_name_col_.get(company_place);
          output.put_buffer_object(item);
          company_num++;
        }
#endif
        output.put_int_at(cn_offset, company_num);
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
    gs::TestQuery2 *app = new gs::TestQuery2(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::TestQuery2 *casted = static_cast<gs::TestQuery2 *>(app);
    delete casted;
  }
}

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IS1 : public AppBase
  {
  public:
    IS1(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
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
          graph_(graph) {}
    ~IS1() {}

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t req = input.get_long();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, req, root))
      {
        return false;
      }
      // const auto& firstname = person_firstName_col_.get_view(root);
      // const auto& lastname = person_lastName_col_.get_view(root);
      // const auto& gender = person_gender_col_.get_view(root);

      std::vector<char> item_v;
      auto person_isLocatedIn_place_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              person_label_id_, place_label_id_, isLocatedIn_label_id_);

      // output.put_string_view(firstname);
      // output.put_string_view(lastname);
      person_firstName_col_.read(root, item_v);
      output.put_string_view({item_v.data(), item_v.size()});
      person_lastName_col_.read(root, item_v);
      output.put_string_view({item_v.data(), item_v.size()});

      // output.put_string_view(person_locationIp_col_.get_view(root));
      person_locationIp_col_.read(root, item_v);
      output.put_string_view({item_v.data(), item_v.size()});
      // output.put_string_view(gender);
      person_gender_col_.read(root, item_v);
      output.put_string_view({item_v.data(), item_v.size()});

      // output.put_long(person_creationDate_col_.get_view(root).milli_second);
      // output.put_long(person_birthday_col_.get_view(root).milli_second);
      // output.put_string_view(person_browserUsed_col_.get_view(root));
      person_creationDate_col_.read(root, item_v);
      output.put_long(reinterpret_cast<long *>(item_v.data())[0]);
      person_birthday_col_.read(root, item_v);
      output.put_long(reinterpret_cast<long *>(item_v.data())[0]);
      person_browserUsed_col_.read(root, item_v);
      output.put_string_view({item_v.data(), item_v.size()});

      std::vector<gs::MutableNbr<grape::EmptyType>> item_e;
      person_isLocatedIn_place_out.read(root, item_e);
      assert(item_e[0].timestamp <= txn.GetTimeStamp());
      // assert(person_isLocatedIn_place_out.exist(root));
      // auto person_place = person_isLocatedIn_place_out.get_edge(root).neighbor;
      auto person_place = item_e[0].neighbor;
      output.put_long(txn.GetVertexId(place_label_id_, person_place));
      return true;
    }

  private:
    label_t person_label_id_;
    label_t place_label_id_;
    label_t isLocatedIn_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const DateColumn &person_birthday_col_;
    const DateColumn &person_creationDate_col_;
    const StringColumn &person_gender_col_;
    const StringColumn &person_browserUsed_col_;
    const StringColumn &person_locationIp_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IS1 *app = new gs::IS1(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IS1 *casted = static_cast<gs::IS1 *>(app);
    delete casted;
  }
}

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
#if OV
      const auto &firstname = person_firstName_col_.get_view(root);
      const auto &lastname = person_lastName_col_.get_view(root);
      const auto &gender = person_gender_col_.get_view(root);
#else
      const auto firstname = person_firstName_col_.get(root);
      const auto lastname = person_lastName_col_.get(root);
      const auto gender = person_gender_col_.get(root);
#endif
      auto person_isLocatedIn_place_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              person_label_id_, place_label_id_, isLocatedIn_label_id_);
#if OV
      output.put_string_view(firstname);
      output.put_string_view(lastname);
      output.put_string_view(person_locationIp_col_.get_view(root));
      output.put_string_view(gender);
      output.put_long(person_creationDate_col_.get_view(root).milli_second);
      output.put_long(person_birthday_col_.get_view(root).milli_second);
      output.put_string_view(person_browserUsed_col_.get_view(root));
      assert(person_isLocatedIn_place_out.exist(root));
      auto person_place = person_isLocatedIn_place_out.get_edge(root).neighbor;
#else
      const auto locationIp = person_locationIp_col_.get(root);
      auto creationdate = person_creationDate_col_.get(root);
      auto birthday = person_birthday_col_.get(root);
      auto browser_used = person_browserUsed_col_.get(root);

      output.put_buffer_object(firstname);
      output.put_buffer_object(lastname);
      output.put_buffer_object(locationIp);
      output.put_buffer_object(gender);

      output.put_long(gbp::BufferBlock::Ref<gs::Date>(creationdate).milli_second);
      output.put_long(gbp::BufferBlock::Ref<gs::Date>(birthday).milli_second);
      output.put_buffer_object(browser_used);

      auto person_place_bo = person_isLocatedIn_place_out.exist(root, exist_mark);
      assert(exist_mark);
      auto person_place = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(person_place_bo).neighbor;
#endif
      output.put_long(txn.GetVertexId(place_label_id_, person_place));

      return true;
    }

  private:
#if !OV
    bool exist_mark = false;
#endif

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

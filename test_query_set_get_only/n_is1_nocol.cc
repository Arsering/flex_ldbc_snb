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
          person_firstName_col_id_(graph.get_vertex_property_column_id(person_label_id_, "firstName")),
          person_lastName_col_id_(graph.get_vertex_property_column_id(person_label_id_, "lastName")),
          person_birthday_col_id_(graph.get_vertex_property_column_id(person_label_id_, "birthday")),
          person_creationDate_col_id_(graph.get_vertex_property_column_id(person_label_id_, "creationDate")),
          person_gender_col_id_(graph.get_vertex_property_column_id(person_label_id_, "gender")),
          person_browserUsed_col_id_(graph.get_vertex_property_column_id(person_label_id_, "browserUsed")),
          person_locationIp_col_id_(graph.get_vertex_property_column_id(person_label_id_, "locationIP")),
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
      const auto firstname = txn.GetVertexProp(person_label_id_, root, person_firstName_col_id_);
      const auto lastname = txn.GetVertexProp(person_label_id_, root, person_lastName_col_id_);
      const auto gender = txn.GetVertexProp(person_label_id_, root, person_gender_col_id_);
      auto person_isLocatedIn_place_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              person_label_id_, place_label_id_, isLocatedIn_label_id_);
      const auto locationIp = txn.GetVertexProp(person_label_id_, root, person_locationIp_col_id_);
      auto creationdate = txn.GetVertexProp(person_label_id_, root, person_creationDate_col_id_);
      auto birthday = txn.GetVertexProp(person_label_id_, root, person_birthday_col_id_);
      auto browser_used = txn.GetVertexProp(person_label_id_, root, person_browserUsed_col_id_);

      output.put_buffer_object(firstname);
      output.put_buffer_object(lastname);
      output.put_buffer_object(locationIp);
      output.put_buffer_object(gender);

      output.put_long(gbp::BufferBlock::Ref<gs::Date>(creationdate).milli_second);
      output.put_long(gbp::BufferBlock::Ref<gs::Date>(birthday).milli_second);
      output.put_buffer_object(browser_used);

      auto person_place_bo = person_isLocatedIn_place_out.get_edge(root);
      assert(person_isLocatedIn_place_out.exist1(person_place_bo));

      auto person_place = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(person_place_bo).neighbor;
      output.put_long(txn.GetVertexId(place_label_id_, person_place));

      return true;
    }

  private:
    label_t person_label_id_;
    label_t place_label_id_;
    label_t isLocatedIn_label_id_;

    int person_firstName_col_id_;
    int person_lastName_col_id_;
    int person_birthday_col_id_;
    int person_creationDate_col_id_;
    int person_gender_col_id_;
    int person_browserUsed_col_id_;
    int person_locationIp_col_id_;

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

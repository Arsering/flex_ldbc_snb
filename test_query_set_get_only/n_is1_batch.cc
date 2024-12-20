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

      // 批量获取 person 的所有属性
      auto person_props = txn.BatchGetVertexPropsFromVid(
          person_label_id_, {root},
          std::vector<std::string>{
              "firstName", "lastName", "locationIP", "gender",
              "creationDate", "birthday", "browserUsed"
          });

      // 使用 GetOtherVertices 获取 person 所在的地点
      auto places = txn.GetOtherVertices(
          person_label_id_, place_label_id_, isLocatedIn_label_id_,
          {root}, "out");

      CHECK(places[0].size() == 1) << "Person should have exactly one location";
      vid_t person_place = places[0][0];

      // 输出结果
      output.put_buffer_object(person_props[0]);  // firstName
      output.put_buffer_object(person_props[1]);  // lastName
      output.put_buffer_object(person_props[2]);  // locationIP
      output.put_buffer_object(person_props[3]);  // gender
      
      // creationDate
      output.put_long(gbp::BufferBlock::Ref<Date>(person_props[4]).milli_second);
      // birthday
      output.put_long(gbp::BufferBlock::Ref<Date>(person_props[5]).milli_second);
      
      output.put_buffer_object(person_props[6]);  // browserUsed
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

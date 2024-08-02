#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IS5 : public AppBase
  {
  public:
    IS5(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}
    ~IS5() {}

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t id = input.get_long();
      CHECK(input.empty());

      vid_t lid{};
      vid_t v;
      if (txn.GetVertexIndex(post_label_id_, id, lid))
      {
        auto post_hasCreator_person_out =
            txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                post_label_id_, person_label_id_, hasCreator_label_id_);
        assert(post_hasCreator_person_out.exist(lid));

        assert(post_hasCreator_person_out.exist(lid));
        v = post_hasCreator_person_out.get_edge(lid).neighbor;
      }
      else if (txn.GetVertexIndex(comment_label_id_, id, lid))
      {
        auto comment_hasCreator_person_out =
            txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                comment_label_id_, person_label_id_, hasCreator_label_id_);
        assert(comment_hasCreator_person_out.exist(lid));

        v = comment_hasCreator_person_out.get_edge(lid).neighbor;
      }
      else
      {
        return false;
      }
      output.put_long(txn.GetVertexId(person_label_id_, v));

      const auto &firstname = person_firstName_col_.get_view(v);
      output.put_string_view(firstname);
      const auto &lastname = person_lastName_col_.get_view(v);
      output.put_string_view(lastname);
      return true;
    }

  private:
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t person_label_id_;
    label_t hasCreator_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IS5 *app = new gs::IS5(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IS5 *casted = static_cast<gs::IS5 *>(app);
    delete casted;
  }
}

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class IS3 : public AppBase
  {
  public:
    IS3(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}

    ~IS3() {}

    struct person_info
    {
      person_info(vid_t v_, int64_t creationdate_, oid_t person_id_)
          : v(v_), creationdate(creationdate_), person_id(person_id_) {}
      vid_t v;
      int64_t creationdate;
      oid_t person_id;
    };

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
      const auto &oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      std::vector<person_info> vec;

      for (auto &e : oe)
      {
        vec.emplace_back(e.neighbor, e.data.milli_second,
                         txn.GetVertexId(person_label_id_, e.neighbor));
      }

      const auto &ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);

      for (auto &e : ie)
      {
        vec.emplace_back(e.neighbor, e.data.milli_second,
                         txn.GetVertexId(person_label_id_, e.neighbor));
      }

      sort(vec.begin(), vec.end(),
           [&](const person_info &x, const person_info &y)
           {
             if (x.creationdate != y.creationdate)
             {
               return x.creationdate > y.creationdate;
             }
             return x.person_id < y.person_id;
           });

      for (auto &v : vec)
      {
        output.put_long(v.person_id);
        output.put_long(v.creationdate);
        const auto &firstname = person_firstName_col_.get_view(v.v);
        output.put_string_view(firstname);
        const auto &lastname = person_lastName_col_.get_view(v.v);
        output.put_string_view(lastname);
      }

#else
      auto oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      std::vector<person_info> vec;

      // for (auto &e : oe)
      for (; oe.is_valid(); oe.next())
      {
        auto data = oe.get_data();

        vec.emplace_back(oe.get_neighbor(), ((gs::Date *)data)->milli_second,
                         txn.GetVertexId(person_label_id_, oe.get_neighbor()));
      }

      auto ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);

      // for (auto &e : ie)
      for (; ie.is_valid(); ie.next())
      {
        auto data = ie.get_data();
        vec.emplace_back(ie.get_neighbor(), ((gs::Date *)data)->milli_second,
                         txn.GetVertexId(person_label_id_, ie.get_neighbor()));
      }

      sort(vec.begin(), vec.end(),
           [&](const person_info &x, const person_info &y)
           {
             if (x.creationdate != y.creationdate)
             {
               return x.creationdate > y.creationdate;
             }
             return x.person_id < y.person_id;
           });

      for (auto &v : vec)
      {
        output.put_long(v.person_id);
        output.put_long(v.creationdate);
        auto firstname = person_firstName_col_.get(v.v);
        output.put_buffer_object(firstname);
        auto lastname = person_lastName_col_.get(v.v);
        output.put_buffer_object(lastname);
      }

#endif
      return true;
    }

  private:
    label_t person_label_id_;
    label_t knows_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IS3 *app = new gs::IS3(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IS3 *casted = static_cast<gs::IS3 *>(app);
    delete casted;
  }
}

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class INS1 : public AppBase
  {
  public:
    INS1(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          organisation_label_id_(
              graph.schema().get_vertex_label_id("ORGANISATION")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          workAt_label_id_(graph.schema().get_edge_label_id("WORKAT")),
          studyAt_label_id_(graph.schema().get_edge_label_id("STUDYAT")),
          hasInterest_label_id_(graph.schema().get_edge_label_id("HASINTEREST")),
          graph_(graph) {}

    bool Query(Decoder &input, Encoder &output) override
    {
      oid_t personid = input.get_long();
      std::string_view firstname = input.get_string();
      std::string_view lastname = input.get_string();
      std::string_view gender = input.get_string();
      int64_t birthday = input.get_long();
      int64_t creationdate = input.get_long();
      std::string_view ip_addr = input.get_string();
      std::string_view browser = input.get_string();
      oid_t cityid = input.get_long();
      std::string_view language = input.get_string();
      std::string_view email = input.get_string();

      auto txn = graph_.GetSingleVertexInsertTransaction();
      if (!txn.AddVertex(
              person_label_id_, personid,
              {Any::From(firstname), Any::From(lastname), Any::From(gender),
               Any::From(Date(birthday)), Any::From(Date(creationdate)),
               Any::From(ip_addr), Any::From(browser), Any::From(language),
               Any::From(email)}))
      {
        LOG(ERROR) << "Adding vertex PERSON[" << personid << "] failed...";
        txn.Abort();
        return false;
      }

      if (!txn.AddEdge(person_label_id_, personid, place_label_id_, cityid,
                       isLocatedIn_label_id_, Any()))
      {
        LOG(ERROR) << "Adding edge PERSON[" << personid << "] isLocatedIn PLACE[" << cityid << "] failed...";
        txn.Abort();
        return false;
      }

      int tagnum = input.get_int();
      for (int i = 0; i < tagnum; ++i)
      {
        oid_t tag = input.get_long();
        if (!txn.AddEdge(person_label_id_, personid, tag_label_id_, tag,
                         hasInterest_label_id_, Any()))
        {
          LOG(ERROR) << "Adding edge PERSON[" << personid << "] hasInterest TAG[" << tag << "] failed...";
          txn.Abort();
          return false;
        }
      }

      int studyatnum = input.get_int();
      for (int i = 0; i < studyatnum; ++i)
      {
        oid_t organizationid = input.get_long();
        int year = input.get_int();
        if (!txn.AddEdge(person_label_id_, personid, organisation_label_id_,
                         organizationid, studyAt_label_id_, Any::From(year)))
        {
          LOG(ERROR) << "Adding edge PERSON[" << personid << "] studyAt(" << year << ") ORGANISATION[" << organizationid << "] failed...";
          txn.Abort();
          return false;
        }
      }
      int workatnum = input.get_int();
      for (int i = 0; i < workatnum; ++i)
      {
        oid_t organizationid = input.get_long();
        int year = input.get_int();
        if (!txn.AddEdge(person_label_id_, personid, organisation_label_id_,
                         organizationid, workAt_label_id_, Any::From(year)))
        {
          LOG(ERROR) << "Adding edge PERSON[" << personid << "] woekAt(" << year << ") ORGANISATION[" << organizationid << "] failed...";
          txn.Abort();
          return false;
        }
      }
      CHECK(input.empty());

      txn.Commit();
      return true;
    }

  private:
    label_t person_label_id_;
    label_t place_label_id_;
    label_t tag_label_id_;
    label_t organisation_label_id_;

    label_t isLocatedIn_label_id_;
    label_t workAt_label_id_;
    label_t studyAt_label_id_;
    label_t hasInterest_label_id_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::INS1 *app = new gs::INS1(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::INS1 *casted = static_cast<gs::INS1 *>(app);
    delete casted;
  }
}

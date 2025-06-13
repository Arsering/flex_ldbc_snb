#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  class IS7 : public AppBase
  {
  public:
    IS7(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          graph_(graph) {}

    ~IS7() {}

    struct comment_info
    {
      comment_info(vid_t commentid, vid_t pid, oid_t personid,
                   int64_t creationdate)
          : commentid(commentid),
            pid(pid),
            personid(personid),
            creationdate(creationdate) {}
      vid_t commentid;
      vid_t pid;
      oid_t personid;
      int64_t creationdate;
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t req = input.get_long();
      CHECK(input.empty());

      vid_t v{};
      vid_t message_author_id;
      std::vector<comment_info> comments;
      friends_.clear();
      friends_.resize(txn.GetVertexNum(person_label_id_));

      auto post_hasCreator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              post_label_id_, person_label_id_, hasCreator_label_id_);
      auto comment_hasCreator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, person_label_id_, hasCreator_label_id_);
      auto comment_replyOf_post_in = txn.GetIncomingGraphView<grape::EmptyType>(
          post_label_id_, comment_label_id_, replyOf_label_id_);
      auto comment_replyOf_comment_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              comment_label_id_, comment_label_id_, replyOf_label_id_);
      std::vector<gs::MutableNbr<grape::EmptyType>> item_e;
      std::vector<char> item_v;
      if (txn.GetVertexIndex(post_label_id_, req, v))
      {
        post_hasCreator_person_out.read(v, item_e);
        // assert(post_hasCreator_person_out.exist(v));
        // message_author_id = post_hasCreator_person_out.get_edge(v).neighbor;
        assert(item_e[0].timestamp.load() <= txn.GetTimeStamp());
        message_author_id = item_e[0].neighbor;

        // const auto& ie = comment_replyOf_post_in.get_edges(v);
        std::vector<gs::MutableNbr<grape::EmptyType>> ie;
        comment_replyOf_post_in.read(v, ie);
        for (auto &e : ie)
        {
          auto comment_v = e.neighbor;
          comment_hasCreator_person_out.read(comment_v, item_e);
          // assert(comment_hasCreator_person_out.exist(comment_v));
          // auto comment_creator =
          //     comment_hasCreator_person_out.get_edge(comment_v).neighbor;
          assert(item_e[0].timestamp.load() <= txn.GetTimeStamp());
          auto comment_creator = item_e[0].neighbor;
          // auto creationdate =
          //     comment_creationDate_col_.get_view(comment_v).milli_second;
          comment_creationDate_col_.read(comment_v, item_v);
          auto creationdate = reinterpret_cast<gs::Date *>(item_v.data())->milli_second;
          comments.emplace_back(
              comment_v, comment_creator,
              txn.GetVertexId(person_label_id_, comment_creator), creationdate);
        }
      }
      else if (txn.GetVertexIndex(comment_label_id_, req, v))
      {
        comment_hasCreator_person_out.read(v, item_e);
        // assert(comment_hasCreator_person_out.exist(v));
        assert(item_e[0].timestamp.load() <= txn.GetTimeStamp());
        message_author_id = item_e[0].neighbor;
        // message_author_id = comment_hasCreator_person_out.get_edge(v).neighbor;
        // const auto &ie = comment_replyOf_comment_in.get_edges(v);
        std::vector<gs::MutableNbr<grape::EmptyType>> ie;
        comment_replyOf_comment_in.read(v, ie);
        for (auto &e : ie)
        {
          auto comment_v = e.neighbor;
          comment_hasCreator_person_out.read(comment_v, item_e);
          assert(item_e[0].timestamp.load() <= txn.GetTimeStamp());
          auto comment_creator = item_e[0].neighbor;
          // assert(comment_hasCreator_person_out.exist(comment_v));
          // auto comment_creator =
          //     comment_hasCreator_person_out.get_edge(comment_v).neighbor;
          comment_creationDate_col_.read(comment_v, item_v);
          // auto creationdate =
          //     comment_creationDate_col_.get_view(comment_v).milli_second;
          auto creationdate = reinterpret_cast<gs::Date *>(item_v.data())[0].milli_second;
          comments.emplace_back(
              comment_v, comment_creator,
              txn.GetVertexId(person_label_id_, comment_creator), creationdate);
        }
      }
      else
      {
        return false;
      }
      // const auto &oe = txn.GetOutgoingEdges<Date>(
      //     person_label_id_, message_author_id, person_label_id_, knows_label_id_);
      std::vector<gs::MutableNbr<gs::Date>> oe;
      txn.GetOutgoingGraphView<Date>(
             person_label_id_, person_label_id_, knows_label_id_)
          .read(message_author_id, oe);
      for (auto &e : oe)
      {
        friends_[e.neighbor] = true;
      }
      oe.clear();
      oe.shrink_to_fit();
      // const auto &ie = txn.GetIncomingEdges<Date>(
      //     person_label_id_, message_author_id, person_label_id_, knows_label_id_);
      std::vector<gs::MutableNbr<gs::Date>> ie;
      txn.GetIncomingGraphView<Date>(
             person_label_id_, person_label_id_, knows_label_id_)
          .read(message_author_id, ie);
      for (auto &e : ie)
      {
        friends_[e.neighbor] = true;
      }
      ie.clear();
      ie.shrink_to_fit();
      sort(comments.begin(), comments.end(),
           [](const comment_info &a, const comment_info &b)
           {
             if (a.creationdate != b.creationdate)
             {
               return a.creationdate > b.creationdate;
             }
             return a.personid < b.personid;
           });
      for (const auto &a : comments)
      {
        output.put_long(txn.GetVertexId(comment_label_id_, a.commentid));
        output.put_long(a.creationdate);
        comment_content_col_.read(a.commentid, item_v);
        // output.put_string_view(comment_content_col_.get_view(a.commentid));
        output.put_string_view({item_v.data(), item_v.size()});

        output.put_long(a.personid);
        // output.put_string_view(person_firstName_col_.get_view(a.pid));
        // output.put_string_view(person_lastName_col_.get_view(a.pid));
        person_firstName_col_.read(a.pid, item_v);
        output.put_string_view({item_v.data(), item_v.size()});
        person_lastName_col_.read(a.pid, item_v);
        output.put_string_view({item_v.data(), item_v.size()});
        output.put_byte(friends_[a.pid] ? static_cast<uint8_t>(1)
                                        : static_cast<uint8_t>(0));
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t replyOf_label_id_;
    label_t hasCreator_label_id_;
    label_t knows_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    const DateColumn &comment_creationDate_col_;
    const StringColumn &comment_content_col_;
    std::vector<bool> friends_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IS7 *app = new gs::IS7(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IS7 *casted = static_cast<gs::IS7 *>(app);
    delete casted;
  }
}

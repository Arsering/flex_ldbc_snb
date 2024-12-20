#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{
  class IC8 : public AppBase
  {
  public:
    IC8(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          graph_(graph) {}

    ~IC8() {}

    struct comment_info
    {
      comment_info(vid_t comment_vid_, int64_t creationDate_, oid_t comment_id_)
          : comment_vid(comment_vid_),
            creationDate(creationDate_),
            comment_id(comment_id_) {}

      vid_t comment_vid;
      int64_t creationDate;
      oid_t comment_id;
    };

    struct comment_info_comparer
    {
      bool operator()(const comment_info &lhs, const comment_info &rhs)
      {
        if (lhs.creationDate > rhs.creationDate)
        {
          return true;
        }
        if (lhs.creationDate < rhs.creationDate)
        {
          return false;
        }
        return lhs.comment_id < rhs.comment_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      // std::cout<<"begin query"<<std::endl;
      reply_count = 0;
      message_count=0;
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }

      comment_info_comparer comparer;
      std::priority_queue<comment_info, std::vector<comment_info>,
                          comment_info_comparer>
          pq(comparer);

      auto comment_replyOf_post_in = txn.GetIncomingGraphView<grape::EmptyType>(
          post_label_id_, comment_label_id_, replyOf_label_id_);
#if OV
      const auto &post_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, post_label_id_, hasCreator_label_id_);

      for (auto &e : post_ie)
      {
        auto v = e.neighbor;
        const auto &ie = comment_replyOf_post_in.get_edges(v);
        for (auto &e1 : ie)
        {
          auto u = e1.neighbor;
          if (pq.size() < 20)
          {
            pq.emplace(u, comment_creationDate_col_.get_view(u).milli_second,
                       txn.GetVertexId(comment_label_id_, u));
          }
          else
          {
            const auto &top = pq.top();
            int64_t creationDate =
                comment_creationDate_col_.get_view(u).milli_second;
#else
      auto post_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, post_label_id_, hasCreator_label_id_);

      for (; post_ie.is_valid(); post_ie.next())
      {
        auto v = post_ie.get_neighbor();
        message_count++;
        auto ie = comment_replyOf_post_in.get_edges(v);
        for (; ie.is_valid(); ie.next())
        {
          auto u = ie.get_neighbor();
          reply_count++;
          if (pq.size() < 20)
          {
            auto item = comment_creationDate_col_.get(u);
            pq.emplace(u, gbp::BufferBlock::Ref<Date>(item).milli_second,
                       txn.GetVertexId(comment_label_id_, u));
          }
          else
          {
            const auto &top = pq.top();
            auto item = comment_creationDate_col_.get(u);
            int64_t creationDate =
                gbp::BufferBlock::Ref<Date>(item).milli_second;
#endif
            if (creationDate > top.creationDate)
            {
              pq.pop();
              pq.emplace(u, creationDate, txn.GetVertexId(comment_label_id_, u));
            }
            else if (creationDate == top.creationDate)
            {
              oid_t comment_id = txn.GetVertexId(comment_label_id_, u);
              if (comment_id < top.comment_id)
              {
                pq.emplace(u, creationDate, comment_id);
              }
            }
          }
        }
      }

      auto comment_replyOf_comment_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              comment_label_id_, comment_label_id_, replyOf_label_id_);
#if OV
      const auto &comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, comment_label_id_, hasCreator_label_id_);
      for (auto &e : comment_ie)
      {
        auto v = e.neighbor;
        const auto &ie = comment_replyOf_comment_in.get_edges(v);
        for (auto &e1 : ie)
        {
          auto u = e1.neighbor;
#else
      auto comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, comment_label_id_, hasCreator_label_id_);
      for (; comment_ie.is_valid(); comment_ie.next())
      {
        auto v = comment_ie.get_neighbor();
        message_count++;
        auto ie = comment_replyOf_comment_in.get_edges(v);
        for (; ie.is_valid(); ie.next())
        {
          auto u = ie.get_neighbor();
          reply_count++;
#endif
          if (pq.size() < 20)
          {
#if OV
            pq.emplace(u, comment_creationDate_col_.get_view(u).milli_second,
                       txn.GetVertexId(comment_label_id_, u));
#else
            auto item = comment_creationDate_col_.get(u);
            pq.emplace(u, gbp::BufferBlock::Ref<Date>(item).milli_second,
                       txn.GetVertexId(comment_label_id_, u));
            item.free();
#endif
          }
          else
          {
            const auto &top = pq.top();
#if OV
            int64_t creationDate =
                comment_creationDate_col_.get_view(u).milli_second;
#else
            auto item = comment_creationDate_col_.get(u);
            int64_t creationDate =
                gbp::BufferBlock::Ref<Date>(item).milli_second;
#endif
            if (creationDate > top.creationDate)
            {
              pq.pop();
              pq.emplace(u, creationDate, txn.GetVertexId(comment_label_id_, u));
            }
            else if (creationDate == top.creationDate)
            {
              oid_t comment_id = txn.GetVertexId(comment_label_id_, u);
              if (comment_id < top.comment_id)
              {
                pq.emplace(u, creationDate, comment_id);
              }
            }
          }
        }
      }

      std::vector<comment_info> vec;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        pq.pop();
      }

      auto comment_hasCreator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, person_label_id_, hasCreator_label_id_);
      // std::ofstream outfile1;
      // outfile1.open("/data-1/yichengzhang/data/latest_gs_bp/graphscope-flex/experiment_space/LDBC_SNB/query_access_log/100_ic8_log",std::ios::app);
      // outfile1 << vec.size()<<"|"<<message_count << "|" << reply_count << std::endl;
      for (auto i = vec.size(); i > 0; i--)
      {
        auto &v = vec[i - 1];
#if OV
        assert(comment_hasCreator_person_out.exist(v.comment_vid));
        auto p = comment_hasCreator_person_out.get_edge(v.comment_vid).neighbor;
        output.put_long(txn.GetVertexId(person_label_id_, p));
        output.put_string_view(person_firstName_col_.get_view(p));
        output.put_string_view(person_lastName_col_.get_view(p));
        output.put_long(v.creationDate);
        output.put_long(v.comment_id);
        output.put_string_view(comment_content_col_.get_view(v.comment_vid));
#else
        auto item = comment_hasCreator_person_out.get_edge(v.comment_vid);
        assert(comment_hasCreator_person_out.exist1(item));

        auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
        output.put_long(txn.GetVertexId(person_label_id_, p));
        item = person_firstName_col_.get(p);
        output.put_buffer_object(item);
        item = person_lastName_col_.get(p);
        output.put_buffer_object(item);
        output.put_long(v.creationDate);
        output.put_long(v.comment_id);
        item = comment_content_col_.get(v.comment_vid);
        output.put_buffer_object(item);
#endif
      }

      // std::cout<<"end query"<<std::endl;
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;

    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;

    const StringColumn &comment_content_col_;
    const DateColumn &comment_creationDate_col_;

    GraphDBSession &graph_;
    int reply_count = 0;
    int message_count=0;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC8 *app = new gs::IC8(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC8 *casted = static_cast<gs::IC8 *>(app);
    delete casted;
  }
}

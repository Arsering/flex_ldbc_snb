#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC12 : public AppBase
  {
  public:
    IC12(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          tagClass_label_id_(graph.schema().get_vertex_label_id("TAGCLASS")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          hasType_label_id_(graph.schema().get_edge_label_id("HASTYPE")),
          isSubClassOf_label_id_(
              graph.schema().get_edge_label_id("ISSUBCLASSOF")),
          tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tag_label_id_, "name")))),
          tagClass_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tagClass_label_id_, "name")))),
          tagClass_num_(graph.graph().vertex_num(tagClass_label_id_)),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          graph_(graph) {}
    ~IC12() {}

    void get_sub_tagClass(const ReadTransaction &txn, vid_t root)
    {
      std::queue<vid_t> q;
      q.push(root);
      sub_tagClass_[root] = true;
      auto tagClass_isSubClassOf_tagClass_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              tagClass_label_id_, tagClass_label_id_, isSubClassOf_label_id_);
      while (!q.empty())
      {
        auto v = q.front();
        q.pop();

        const auto &ie = tagClass_isSubClassOf_tagClass_in.get_edges(v);
        for (auto &e : ie)
        {
          if (!sub_tagClass_[e.neighbor])
          {
            sub_tagClass_[e.neighbor] = true;
            q.push(e.neighbor);
          }
        }
      }
    }

    struct person_info
    {
      person_info(int reply_count_, vid_t person_vid_, oid_t person_id_)
          : reply_count(reply_count_),
            person_vid(person_vid_),
            person_id(person_id_) {}

      int reply_count;
      vid_t person_vid;
      oid_t person_id;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.reply_count > rhs.reply_count)
        {
          return true;
        }
        if (lhs.reply_count < rhs.reply_count)
        {
          return false;
        }
        return lhs.person_id < rhs.person_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      std::string_view tagclassname = input.get_string();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      vid_t tagClass_id = tagClass_num_;
      for (vid_t i = 0; i < tagClass_num_; ++i)
      {

        if (tagClass_name_col_.get_view(i) == tagclassname)
        {
          tagClass_id = i;
          break;
        }
      }
      assert(tagClass_id != tagClass_num_);
      sub_tagClass_.clear();
      sub_tagClass_.resize(txn.GetVertexNum(tagClass_label_id_));
      get_sub_tagClass(txn, tagClass_id);

      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);
      sub_tagClass_.clear();

      auto comment_hasCreator_person_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              person_label_id_, comment_label_id_, hasCreator_label_id_);
      auto comment_replyOf_post_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, post_label_id_, replyOf_label_id_);
      auto post_hasTag_tag_out = txn.GetOutgoingGraphView<grape::EmptyType>(
          post_label_id_, tag_label_id_, hasTag_label_id_);
      auto tag_hasType_tagClass_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              tag_label_id_, tagClass_label_id_, hasType_label_id_);

      const auto &oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (auto &e : oe)
      {
        auto v = e.neighbor;
        int count = 0;
        const auto &comment_ie = comment_hasCreator_person_in.get_edges(v);
        for (auto &e1 : comment_ie)
        {
          if (comment_replyOf_post_out.exist(e1.neighbor))
          {
            const auto &e2 = comment_replyOf_post_out.get_edge(e1.neighbor);
            const auto &tag_oe = post_hasTag_tag_out.get_edges(e2.neighbor);
            for (auto &te : tag_oe)
            {
              auto tc = tag_hasType_tagClass_out.get_edge(te.neighbor).neighbor;
              if (sub_tagClass_[tc])
              {
                ++count;
                break;
              }
            }
          }
        }
        if (count)
        {
          if (pq.size() < 20)
          {
            pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
          }
          else
          {
            const auto &top = pq.top();
            if (count > top.reply_count)
            {
              pq.pop();
              pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
            }
            else if (count == top.reply_count)
            {
              oid_t friend_id = txn.GetVertexId(person_label_id_, v);
              if (friend_id < top.person_id)
              {
                pq.pop();
                pq.emplace(count, v, friend_id);
              }
            }
          }
        }
      }

      const auto &ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (auto &e : ie)
      {
        auto v = e.neighbor;
        int count = 0;
        const auto &comment_ie = comment_hasCreator_person_in.get_edges(v);
        for (auto &e1 : comment_ie)
        {
          if (comment_replyOf_post_out.exist(e1.neighbor))
          {
            const auto &e2 = comment_replyOf_post_out.get_edge(e1.neighbor);
            const auto &tag_oe = post_hasTag_tag_out.get_edges(e2.neighbor);
            for (auto &te : tag_oe)
            {
              assert(tag_hasType_tagClass_out.exist(te.neighbor));
              auto tc = tag_hasType_tagClass_out.get_edge(te.neighbor).neighbor;
              if (sub_tagClass_[tc])
              {
                ++count;
                break;
              }
            }
          }
        }
        if (count)
        {
          if (pq.size() < 20)
          {
            pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
          }
          else
          {
            const auto &top = pq.top();
            if (count > top.reply_count)
            {
              pq.pop();
              pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
            }
            else if (count == top.reply_count)
            {
              oid_t friend_id = txn.GetVertexId(person_label_id_, v);
              if (friend_id < top.person_id)
              {
                pq.pop();
                pq.emplace(count, v, friend_id);
              }
            }
          }
        }
      }
      std::vector<person_info> vec;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        pq.pop();
      }
      std::set<vid_t> tmp;
      for (auto i = vec.size(); i > 0; i--)
      {
        auto &v = vec[i - 1];
        output.put_long(v.person_id);

        output.put_string_view(person_firstName_col_.get_view(v.person_vid));
        output.put_string_view(person_lastName_col_.get_view(v.person_vid));
        tmp.clear();

        const auto &comment_ie =
            comment_hasCreator_person_in.get_edges(v.person_vid);
        for (auto &e1 : comment_ie)
        {
          if (comment_replyOf_post_out.exist(e1.neighbor))
          {
            auto &e2 = comment_replyOf_post_out.get_edge(e1.neighbor);
            const auto &tag_e = txn.GetOutgoingEdges<grape::EmptyType>(
                post_label_id_, e2.neighbor, tag_label_id_, hasTag_label_id_);
            for (auto &te : tag_e)
            {
              auto tag = te.neighbor;
              assert(tag_hasType_tagClass_out.exist(tag));
              auto tc = tag_hasType_tagClass_out.get_edge(tag).neighbor;
              if (sub_tagClass_[tc])
              {
                tmp.insert(tag);
              }
            }
          }
        }
        output.put_int(tmp.size());
        for (auto tag : tmp)
        {

          output.put_string_view(tag_name_col_.get_view(tag));
        }
        output.put_int(v.reply_count);
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t knows_label_id_;
    label_t tag_label_id_;
    label_t tagClass_label_id_;

    label_t post_label_id_;
    label_t comment_label_id_;
    label_t replyOf_label_id_;
    label_t hasCreator_label_id_;
    label_t hasTag_label_id_;
    label_t hasType_label_id_;
    label_t isSubClassOf_label_id_;

    const StringColumn &tag_name_col_;
    const StringColumn &tagClass_name_col_;
    vid_t tagClass_num_;

    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    std::vector<bool> sub_tagClass_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC12 *app = new gs::IC12(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC12 *casted = static_cast<gs::IC12 *>(app);
    delete casted;
  }
}

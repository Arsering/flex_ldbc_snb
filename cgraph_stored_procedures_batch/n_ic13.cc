#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class IC13 : public AppBase
  {
  public:
    IC13(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          graph_(graph) {}

    ~IC13() {}

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t person1id = input.get_long();
      oid_t person2id = input.get_long();
      CHECK(input.empty());

      auto person_num = txn.GetVertexNum(person_label_id_);
      vid_t src{}, dst{};
      if (!txn.GetVertexIndex(person_label_id_, person1id, src))
      {
        output.put_int(0);
        return false;
      }
      if (!txn.GetVertexIndex(person_label_id_, person2id, dst))
      {
        output.put_int(0);
        return false;
      }
      if (src == dst)
      {
        output.put_int(0);
        return true;
      }
      dis_.clear();
      dis_.resize(person_num);
      dis_[src] = 1;
      dis_[dst] = -1;

      std::queue<vid_t> q1, q2;
      q1.push(src);
      q2.push(dst);
      std::queue<vid_t> tmp;
      // uint8_t dep = 1;
      auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      while (true)
      {
        if (q1.size() <= q2.size())
        {
          if (q1.empty())
            break;
          while (!q1.empty())
          {
            auto x = q1.front();
            q1.pop();
            auto oe = person_knows_person_out.get_edges(x);
            for (; oe.is_valid(); oe.next())
            {
              auto v = oe.get_neighbor();
              if (dis_[v] == 0)
              {
                dis_[v] = dis_[x] + 1;
                tmp.push(v);
              }
              else if (dis_[v] < 0)
              {
                int8_t a = dis_[x] - dis_[v];
                output.put_int(a - 1);
                return true;
              }
            }
            auto ie = person_knows_person_in.get_edges(x);
            for (; ie.is_valid(); ie.next())
            {
              auto v = ie.get_neighbor();
              if (dis_[v] == 0)
              {
                dis_[v] = dis_[x] + 1;
                tmp.push(v);
              }
              else if (dis_[v] < 0)
              {
                int8_t a = dis_[x] - dis_[v];
                output.put_int(a - 1);
                return true;
              }
            }
          }
          std::swap(q1, tmp);
        }
        else
        {
          if (q2.empty())
            break;
          while (!q2.empty())
          {
            auto x = q2.front();
            q2.pop();
            auto oe = person_knows_person_out.get_edges(x);
            for (; oe.is_valid(); oe.next())
            {
              auto v = oe.get_neighbor();
              if (dis_[v] == 0)
              {
                dis_[v] = dis_[x] - 1;
                tmp.push(v);
              }
              else if (dis_[v] > 0)
              {
                int8_t a = dis_[v] - dis_[x];
                output.put_int(a - 1);
                return true;
              }
            }
            auto ie = person_knows_person_in.get_edges(x);
            for (; ie.is_valid(); ie.next())
            {
              auto v = ie.get_neighbor();
              if (dis_[v] == 0)
              {
                dis_[v] = dis_[x] - 1;
                tmp.push(v);
              }
              else if (dis_[v] > 0)
              {
                int8_t a = dis_[v] - dis_[x];
                output.put_int(a - 1);
                return true;
              }
            }
          }
          std::swap(q2, tmp);
        }
      }
      output.put_int(-1);
      return true;
    }

  private:
    label_t person_label_id_;
    label_t knows_label_id_;
    std::vector<int8_t> dis_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC13 *app = new gs::IC13(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC13 *casted = static_cast<gs::IC13 *>(app);
    delete casted;
  }
}

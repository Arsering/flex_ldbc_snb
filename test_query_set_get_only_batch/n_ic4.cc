#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC4 : public AppBase
  {
  public:
    IC4(GraphDBSession &graph)
        : tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tag_label_id_, "name")))),
          graph_(graph) {}

    ~IC4() {}

    struct tag_info
    {
      tag_info(int count_, gbp::BufferBlock tag_name_)
          : count(count_), tag_name(tag_name_) {}
      int count;
      gbp::BufferBlock tag_name;
    };

    struct tag_info_comparer
    {
      bool operator()(const tag_info &lhs, const tag_info &rhs)
      {
        if (lhs.count > rhs.count)
        {
          return true;
        }
        if (lhs.count < rhs.count)
        {
          return false;
        }
        return lhs.tag_name < rhs.tag_name;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      int64_t start_date = input.get_long();
      int duration_days = input.get_int();
      CHECK(input.empty());

      friends_.clear();
      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      const int64_t milli_sec_per_day = 24 * 60 * 60 * 1000l;
      int64_t end_date = start_date + 1l * duration_days * milli_sec_per_day;
      get_1d_friends(txn, person_label_id_, knows_label_id_, root, friends_);
      tag_num_ = txn.GetVertexNum(tag_label_id_);
      std::vector<int> post_count(tag_num_, 0);
      std::vector<bool> neg(tag_num_, false);

      auto post_hasCreator_person_in = txn.GetIncomingGraphView<grape::EmptyType>(
          person_label_id_, post_label_id_, hasCreator_label_id_);
      auto post_hasTag_tag_out = txn.GetOutgoingGraphView<grape::EmptyType>(
          post_label_id_, tag_label_id_, hasTag_label_id_);

      for (auto v : friends_)
      {
        auto ie = post_hasCreator_person_in.get_edges(v);
        for (; ie.is_valid(); ie.next())
        {
          auto item = post_creationDate_col_.get(ie.get_neighbor());
          auto creationDate = gbp::BufferBlock::Ref<Date>(item).milli_second;
          auto post_id = ie.get_neighbor();
          if (creationDate < end_date)
          {
            auto oe = post_hasTag_tag_out.get_edges(post_id);
            if (start_date <= creationDate)
            {
              for (; oe.is_valid(); oe.next())
              {
                if (!neg[oe.get_neighbor()])
                {
                  post_count[oe.get_neighbor()] += 1;
                }
              }
            }
            else
            {
              for (; oe.is_valid(); oe.next())
              {
                neg[oe.get_neighbor()] = true;
                post_count[oe.get_neighbor()] = 0;
              }
            }
          }
        }
      }

      tag_info_comparer comparer;
      std::priority_queue<tag_info, std::vector<tag_info>, tag_info_comparer> que(
          comparer);
      vid_t tag_id = 0;
      while (que.size() < 10 && tag_id < tag_num_)
      {
        int count = post_count[tag_id];
        if (count)
        {
          que.emplace(count, tag_name_col_.get(tag_id));
        }
        ++tag_id;
      }
      while (tag_id < tag_num_)
      {
        int count = post_count[tag_id];
        if (count)
        {
          const auto &top = que.top();
          if (count > top.count)
          {
            que.pop();
            que.emplace(count, tag_name_col_.get(tag_id));
          }
          else if (count == top.count)
          {
            auto tag_name_item = tag_name_col_.get(tag_id);
            if (tag_name_item < top.tag_name)
            {
              que.pop();
              que.emplace(count, tag_name_item);
            }
          }
        }
        ++tag_id;
      }
      std::vector<tag_info> vec;
      vec.reserve(que.size());
      while (!que.empty())
      {
        vec.emplace_back(que.top());
        que.pop();
      }
      for (auto i = vec.size(); i > 0; i--)
      {
        auto &t = vec[i - 1];
        output.put_buffer_object(t.tag_name);
        output.put_int(t.count);
      }
      return true;
    }

  private:
    label_t tag_label_id_;
    label_t person_label_id_;
    label_t post_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t hasTag_label_id_;

    std::vector<vid_t> friends_;

    vid_t tag_num_;
    const DateColumn &post_creationDate_col_;
    const StringColumn &tag_name_col_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC4 *app = new gs::IC4(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC4 *casted = static_cast<gs::IC4 *>(app);
    delete casted;
  }
}

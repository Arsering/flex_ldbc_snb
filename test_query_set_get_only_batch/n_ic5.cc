#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC5 : public AppBase
  {
  public:
    IC5(GraphDBSession &graph)
        : forum_label_id_(graph.schema().get_vertex_label_id("FORUM")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          hasMember_label_id_(graph.schema().get_edge_label_id("HASMEMBER")),
          containerOf_label_id_(graph.schema().get_edge_label_id("CONTAINEROF")),
          forum_title_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(forum_label_id_, "title")))),
          graph_(graph) {}

    ~IC5() {}

    struct forum_info
    {
      forum_info(vid_t forum_lid_, int post_count_, oid_t forum_id_)
          : forum_lid(forum_lid_), post_count(post_count_), forum_id(forum_id_) {}

      vid_t forum_lid;
      int post_count;
      oid_t forum_id;
    };

    struct forum_info_comparer
    {
      bool operator()(const forum_info &lhs, const forum_info &rhs) const
      {
        if (lhs.post_count > rhs.post_count)
        {
          return true;
        }
        else if (lhs.post_count < rhs.post_count)
        {
          return false;
        }
        else
        {
          return lhs.forum_id < rhs.forum_id;
        }
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      int64_t mindate = input.get_long();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      vid_t person_num = txn.GetVertexNum(person_label_id_);
      vid_t forum_num = txn.GetVertexNum(forum_label_id_);

      post_count_.resize(forum_num, 0);
      person_forum_set_.resize(forum_num, false);

      auto forum_hasMember_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, forum_label_id_, hasMember_label_id_);
      auto post_hasCreator_person_in = txn.GetIncomingGraphView<grape::EmptyType>(
          person_label_id_, post_label_id_, hasCreator_label_id_);
      auto forum_containerOf_post_in =
          txn.GetIncomingSingleGraphView<grape::EmptyType>(
              post_label_id_, forum_label_id_, containerOf_label_id_);

      std::vector<vid_t> person_vids_all;
      std::vector<vid_t> person_vids_tmp;
      get_1d_2d_neighbors(txn, person_label_id_, knows_label_id_, root, txn.GetVertexNum(person_label_id_), person_vids_all);

      size_t batch_size = 300;
      std::vector<vid_t> person_vids;
      std::vector<size_t> person_vids_index;
      person_vids_tmp.reserve(batch_size);
      for (size_t i = 0; i < person_vids_all.size(); i += batch_size)
      {
        person_vids.clear();
        person_vids_tmp.clear();
        person_vids_index.clear();
        if (i + batch_size > person_vids_all.size())
          person_vids_tmp.assign(person_vids_all.begin() + i, person_vids_all.end());
        else
          person_vids_tmp.assign(person_vids_all.begin() + i, person_vids_all.begin() + i + batch_size);

        auto forum_hasMember_person_in_items = txn.BatchGetIncomingEdges<Date>(person_label_id_, forum_label_id_, hasMember_label_id_, person_vids_tmp);
        bool mark = false;
        for (size_t i = 0; i < person_vids_tmp.size(); i++)
        {
          mark = false;
          for (auto &item = forum_hasMember_person_in_items[i]; item.is_valid(); item.next())
          {
            if (mindate < ((Date *)item.get_data())->milli_second)
            {
              mark = true;
              break;
            }
          }
          if (mark)
          {
            person_vids.push_back(person_vids_tmp[i]);
            person_vids_index.push_back(i);
          }
        }
        std::vector<vid_t> post_vids;
        auto post_hasCreator_person_in_items = txn.BatchGetIncomingEdges<grape::EmptyType>(person_label_id_, post_label_id_, hasCreator_label_id_, person_vids);
        for (size_t i = 0; i < person_vids.size(); i++)
        {
          for (post_hasCreator_person_in_items[i].recover(); post_hasCreator_person_in_items[i].is_valid(); post_hasCreator_person_in_items[i].next())
          {
            post_vids.push_back(post_hasCreator_person_in_items[i].get_neighbor());
          }
        }
        auto forum_containerOf_post_in_items = txn.BatchGetIncomingSingleEdges(post_label_id_, forum_label_id_, containerOf_label_id_, post_vids);
        size_t post_count = 0;
        for (size_t i = 0; i < person_vids.size(); i++)
        {
          auto &item = forum_hasMember_person_in_items[person_vids_index[i]];
          for (item.recover(); item.is_valid(); item.next())
          {
            if (mindate < ((Date *)item.get_data())->milli_second)
            {
              auto forum = item.get_neighbor();
              person_forum_set_[forum] = true;
              person_forum_list_.push_back(forum);
              if (post_count_[forum] == 0)
              {
                forum_list_.push_back(forum);
                ++post_count_[forum];
              }
            }
          }
          if (person_forum_list_.empty())
          {
            assert(false);
            continue;
          }
          for (post_hasCreator_person_in_items[i].recover(); post_hasCreator_person_in_items[i].is_valid(); post_hasCreator_person_in_items[i].next())
          {
            auto item = forum_containerOf_post_in_items[post_count++];
            // assert(forum_containerOf_post_in_items.exist1(item));
            auto f = gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(item).neighbor;
            if (person_forum_set_[f])
            {
              ++post_count_[f];
            }
          }
          for (auto v : person_forum_list_)
          {
            person_forum_set_[v] = false;
          }
          person_forum_list_.clear();
        }
      }

      forum_info_comparer cmp;
      std::priority_queue<forum_info, std::vector<forum_info>,
                          forum_info_comparer>
          que(cmp);
      auto forum_oids = txn.BatchGetVertexIds(forum_label_id_, forum_list_);
      for (size_t i = 0; i < forum_list_.size(); i++)
      {
        if (que.size() < 20)
        {
          que.emplace(forum_list_[i], post_count_[forum_list_[i]], forum_oids[i]);
        }
        else
        {
          const auto &top = que.top();
          if (top.post_count < post_count_[forum_list_[i]])
          {
            que.pop();
            que.emplace(forum_list_[i], post_count_[forum_list_[i]], forum_oids[i]);
          }
          else if (top.post_count == post_count_[forum_list_[i]])
          {
            oid_t id = forum_oids[i];
            if (id < top.forum_id)
            {
              que.pop();
              que.emplace(forum_list_[i], post_count_[forum_list_[i]], id);
            }
          }
        }
        post_count_[forum_list_[i]] = 0;
      }
      forum_list_.clear();

      std::vector<forum_info> vec;
      std::vector<vid_t> forum_vids;
      vec.reserve(que.size());
      forum_vids.reserve(que.size());
      while (!que.empty())
      {
        vec.emplace_back(que.top());
        forum_vids.push_back(que.top().forum_lid);
        que.pop();
      }
      auto forum_title_col_items = txn.BatchGetVertexPropsFromVids(forum_label_id_, forum_vids, {"title"});
      for (size_t i = vec.size(); i > 0; i--)
      {
        auto &cur = vec[i - 1];
        output.put_buffer_object(forum_title_col_items[0][i - 1]);
        output.put_int(cur.post_count - 1);
      }
      return true;
    }

  private:
    label_t forum_label_id_;
    label_t person_label_id_;
    label_t post_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t hasMember_label_id_;
    label_t containerOf_label_id_;

    const StringColumn &forum_title_col_;

    std::vector<int> post_count_;
    std::vector<vid_t> forum_list_;

    std::vector<bool> person_forum_set_;
    std::vector<vid_t> person_forum_list_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC5 *app = new gs::IC5(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC5 *casted = static_cast<gs::IC5 *>(app);
    delete casted;
  }
}

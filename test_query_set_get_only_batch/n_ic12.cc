#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

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
        auto ie = tagClass_isSubClassOf_tagClass_in.get_edges(v);
        for (; ie.is_valid(); ie.next())
        {
          if (!sub_tagClass_[ie.get_neighbor()])
          {
            sub_tagClass_[ie.get_neighbor()] = true;
            q.push(ie.get_neighbor());
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
        if (lhs.reply_count < rhs.reply_count)
        {
          return true;
        }
        if (lhs.reply_count > rhs.reply_count)
        {
          return false;
        }
        return lhs.person_id > rhs.person_id;
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
        auto tagClass_name = tagClass_name_col_.get(i);
        if (tagClass_name == tagclassname)
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

      std::vector<vid_t> neighbor_list;
      auto person_knows_person_in = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; person_knows_person_in.is_valid(); person_knows_person_in.next())
      {
        neighbor_list.push_back(person_knows_person_in.get_neighbor());
      }
      auto person_knows_person_out = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; person_knows_person_out.is_valid(); person_knows_person_out.next())
      {
        neighbor_list.push_back(person_knows_person_out.get_neighbor());
      }

      auto comment_hasCreator_person_in_items = txn.BatchGetIncomingEdges<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, neighbor_list);
      std::vector<vid_t> comment_vids_tmp;
      std::vector<vid_t> comment_vids;
      std::vector<size_t> comment_range_per_person;
      comment_range_per_person.reserve(neighbor_list.size());

      for (auto item_idx = 0; item_idx < comment_hasCreator_person_in_items.size(); ++item_idx)
      {
        auto &item = comment_hasCreator_person_in_items[item_idx];
        for (; item.is_valid(); item.next())
        {
          comment_vids_tmp.push_back(item.get_neighbor());
        }
        comment_range_per_person.push_back(comment_vids_tmp.size());
      }
      auto comment_replyOf_post_out_items = txn.BatchGetOutgoingSingleEdges(comment_label_id_, post_label_id_, replyOf_label_id_, comment_vids_tmp);
      std::vector<vid_t> post_vids;
      size_t person_idx_cur = 0;

      for (auto comment_idx = 0; comment_idx < comment_replyOf_post_out_items.size(); ++comment_idx)
      {
        if (comment_idx >= comment_range_per_person[person_idx_cur])
        {
          comment_range_per_person[person_idx_cur] = post_vids.size();
          person_idx_cur++;
        }
        auto &item = gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(comment_replyOf_post_out_items[comment_idx]);
        if (item.timestamp.load() <= txn.timestamp())
        {
          post_vids.push_back(item.neighbor);
          comment_vids.push_back(comment_vids_tmp[comment_idx]);
        }
      }
      comment_range_per_person.back() = post_vids.size();

      auto post_hasTag_tag_out_items = txn.BatchGetOutgoingEdges<grape::EmptyType>(post_label_id_, tag_label_id_, hasTag_label_id_, post_vids);
      assert(post_hasTag_tag_out_items.size() == post_vids.size());
      size_t post_idx = 0;
      std::vector<size_t> count_per_person;
      count_per_person.resize(neighbor_list.size(), 0);
      auto tag_hasType_tagClass_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              tag_label_id_, tagClass_label_id_, hasType_label_id_);
      for (auto person_idx = 0; person_idx < neighbor_list.size(); ++person_idx)
      {
        for (; post_idx < comment_range_per_person[person_idx]; post_idx++)
        {
          auto &item = post_hasTag_tag_out_items[post_idx];
          for (; item.is_valid(); item.next())
          {
            auto tag_item = tag_hasType_tagClass_out.get_edge(item.get_neighbor());
            auto tc = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(tag_item).neighbor;
            if (sub_tagClass_[tc])
            {
              count_per_person[person_idx]++;
              break;
            }
          }
        }
      }
      std::vector<std::pair<int, vid_t>> result_list_final;
      for (auto i = 0; i < neighbor_list.size(); ++i)
      {
        if (count_per_person[i] > 0)
        {
          result_list_final.emplace_back(count_per_person[i], neighbor_list[i]);
        }
      }
      if (result_list_final.size() == 0)
        return true;

      // 使用 lambda 表达式对数据进行排序，同时更新索引
      std::sort(result_list_final.begin(), result_list_final.end(), [](const std::pair<int, vid_t> &a, const std::pair<int, vid_t> &b)
                {
                  return a.first < b.first; // 按照第一个元素排序
                });
      for (auto i = 0; i < result_list_final.size() && (pq.size() < 20 || result_list_final[i].first == result_list_final[i - 1].first); ++i)
      {
        pq.emplace(result_list_final[i].first, result_list_final[i].second, txn.GetVertexId(person_label_id_, result_list_final[i].second));
      }

      std::vector<person_info> vec;
      vec.reserve(pq.size());
      while (!pq.empty() && vec.size() < 20)
      {
        vec.emplace_back(pq.top());
        pq.pop();
      }

      auto comment_hasCreator_person_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              person_label_id_, comment_label_id_, hasCreator_label_id_);
      auto comment_replyOf_post_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, post_label_id_, replyOf_label_id_);
      std::set<vid_t> tmp;
      for (auto i = 0; i < vec.size(); ++i)
      {
        auto &v = vec[i];
        output.put_long(v.person_id);
        auto item = person_firstName_col_.get(v.person_vid);
        output.put_buffer_object(item);

        item = person_lastName_col_.get(v.person_vid);
        output.put_buffer_object(item);
        tmp.clear();
        auto comment_ie =
            comment_hasCreator_person_in.get_edges(v.person_vid);

        for (; comment_ie.is_valid(); comment_ie.next())
        {
          auto e2 = comment_replyOf_post_out.get_edge(comment_ie.get_neighbor());
          if (comment_replyOf_post_out.exist1(e2))
          {
            auto tag_e = txn.GetOutgoingEdges<grape::EmptyType>(
                post_label_id_, gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e2).neighbor, tag_label_id_, hasTag_label_id_);
            for (; tag_e.is_valid(); tag_e.next())
            {
              auto tag = tag_e.get_neighbor();

              auto item = tag_hasType_tagClass_out.get_edge(tag);
              assert(tag_hasType_tagClass_out.exist1(item));

              auto tc = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
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
          auto item = tag_name_col_.get(tag);
          output.put_buffer_object(item);
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

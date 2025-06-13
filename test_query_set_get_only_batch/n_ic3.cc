#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC3 : public AppBase
  {
  public:
    IC3(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
          isPartOf_label_id_(graph.schema().get_edge_label_id("ISPARTOF")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(place_label_id_, "name")))),
          place_num_(graph.graph().vertex_num(place_label_id_)),
          graph_(graph)
    {
      // const char* ratio_str = std::getenv("BATCH_SIZE_RATIO");
      // if (ratio_str != nullptr) {
      //   BATCH_SIZE_RATIO = std::atof(ratio_str);
      // }
    }
    ~IC3() {}
    struct person_info
    {
      person_info(int count_, vid_t otherPerson_vid_, oid_t otherPerson_id_)
          : count(count_),
            otherPerson_vid(otherPerson_vid_),
            otherPerson_id(otherPerson_id_) {}

      int count;
      vid_t otherPerson_vid;
      oid_t otherPerson_id;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.count < rhs.count)
        {
          return true;
        }
        if (lhs.count > rhs.count)
        {
          return false;
        }
        return lhs.otherPerson_id > rhs.otherPerson_id;
      }
    };

    void get_friends(const ReadTransaction &txn, vid_t root,
                     std::vector<vid_t> &friends)
    {
      std::vector<vid_t> person_vids;
      {
        auto person_knows_person_out =
            txn.GetOutgoingGraphView<Date>(person_label_id_, person_label_id_, knows_label_id_);
        auto person_knows_person_in =
            txn.GetIncomingGraphView<Date>(person_label_id_, person_label_id_, knows_label_id_);
        for (auto ie = person_knows_person_in.get_edges(root); ie.is_valid(); ie.next())
        {
          auto v = ie.get_neighbor();
          if (!friends_[v])
          {
            friends_[v] = true;
            person_vids.push_back(v);
          }
        }
        for (auto oe = person_knows_person_out.get_edges(root); oe.is_valid(); oe.next())
        {
          auto v = oe.get_neighbor();
          if (!friends_[v])
          {
            friends_[v] = true;
            person_vids.push_back(v);
          }
        }
        auto person_knows_person_in_items = txn.BatchGetIncomingEdges<Date>(person_label_id_, person_label_id_, knows_label_id_, person_vids);
        auto person_knows_person_out_items = txn.BatchGetOutgoingEdges<Date>(person_label_id_, person_label_id_, knows_label_id_, person_vids);
        for (int i = 0; i < person_knows_person_in_items.size(); i++)
        {
          for (auto &e = person_knows_person_in_items[i]; e.is_valid(); e.next())
          {
            if (!friends_[e.get_neighbor()])
            {
              friends_[e.get_neighbor()] = true;
              person_vids.push_back(e.get_neighbor());
            }
          }
          for (auto &e = person_knows_person_out_items[i]; e.is_valid(); e.next())
          {
            if (!friends_[e.get_neighbor()])
            {
              friends_[e.get_neighbor()] = true;
              person_vids.push_back(e.get_neighbor());
            }
          }
        }
      }
      auto person_isLocatedIn_place_out_items = txn.BatchGetOutgoingSingleEdges(person_label_id_, place_label_id_, isLocatedIn_label_id_, person_vids);
      for (size_t i = 0; i < person_vids.size(); i++)
      {
        auto p = gbp::BufferBlock::RefSingle<MutableNbr<grape::EmptyType>>(person_isLocatedIn_place_out_items[i]).neighbor;
        if (!place_Locatedin_[p])
        {
          friends.push_back(person_vids[i]);
        }
      }
    }

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      std::string_view countryxname = input.get_string();
      std::string_view countryyname = input.get_string();
      int64_t start_date = input.get_long();
      int durationdays = input.get_int();
      CHECK(input.empty());

      place_Locatedin_.clear();
      place_Locatedin_.resize(txn.GetVertexNum(place_label_id_), false);
      person_num_ = txn.GetVertexNum(person_label_id_);
      friends_.clear();
      friends_.resize(person_num_, false);
      count_.clear();
      count_.resize(person_num_, {});

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }

      vid_t countryX = place_num_;
      vid_t countryY = place_num_;
      {
        // size_t scan_batch_size = 100;
        // scan_batch_size = scan_batch_size * BATCH_SIZE_RATIO;
        size_t scan_batch_size = BATCH_SIZE;
        std::vector<vid_t> place_vids;
        place_vids.reserve(scan_batch_size);
        for (vid_t i = 0; i < place_num_; i += scan_batch_size)
        {
          for (vid_t j = 0; j < scan_batch_size && i + j < place_num_; ++j)
          {
            place_vids.push_back(i + j);
          }
          auto place_name_items = txn.BatchGetVertexPropsFromVids(place_label_id_, place_vids, {"name"});
          for (size_t j = 0; j < place_vids.size(); ++j)
          {
            if (place_name_items[0][j] == countryxname)
            {
              countryX = place_vids[j];
            }
            if (place_name_items[0][j] == countryyname)
            {
              countryY = place_vids[j];
            }
          }
          if (countryX != place_num_ && countryY != place_num_)
          {
            break;
          }
        }
        assert(countryX != place_num_);
        assert(countryY != place_num_);
      }

      auto xe = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryX, place_label_id_, isPartOf_label_id_);
      for (; xe.is_valid(); xe.next())
      {
        place_Locatedin_[xe.get_neighbor()] = true;
      }

      auto ye = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryY, place_label_id_, isPartOf_label_id_);
      for (; ye.is_valid(); ye.next())
      {
        place_Locatedin_[ye.get_neighbor()] = true;
      }

      std::vector<vid_t> friends{};
      get_friends(txn, root, friends);

      const int64_t milli_sec_per_day = 24 * 60 * 60 * 1000l;
      int64_t end_date = start_date + durationdays * milli_sec_per_day;
      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);

      auto postex = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryX, post_label_id_, isLocatedIn_label_id_);
      auto commentex = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryX, comment_label_id_, isLocatedIn_label_id_);
      auto postey = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryY, post_label_id_, isLocatedIn_label_id_);
      auto commentey = txn.GetIncomingEdges<grape::EmptyType>(
          place_label_id_, countryY, comment_label_id_, isLocatedIn_label_id_);
      std::vector<vid_t> post_vids;
      std::vector<vid_t> comment_vids;
      size_t postx_num = 0;
      size_t commentx_num = 0;

      for (; postex.is_valid(); postex.next())
      {
        post_vids.push_back(postex.get_neighbor());
        postx_num++;
      }
      postx_num = post_vids.size();
      for (; postey.is_valid(); postey.next())
      {
        post_vids.push_back(postey.get_neighbor());
      }

      for (; commentex.is_valid(); commentex.next())
      {
        comment_vids.push_back(commentex.get_neighbor());
      }
      commentx_num = comment_vids.size();
      for (; commentey.is_valid(); commentey.next())
      {
        comment_vids.push_back(commentey.get_neighbor());
      }
      auto post_creationDate_items = txn.BatchGetVertexPropsFromVids(post_label_id_, post_vids, {"creationDate"});
      std::vector<vid_t> post_vids_new;
      size_t postx_num_new = 0;

      for (size_t i = 0; i < post_vids.size(); i++)
      {
        auto creationDate = gbp::BufferBlock::Ref<Date>(post_creationDate_items[0][i]).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          post_vids_new.push_back(post_vids[i]);
          if (i < postx_num)
          {
            postx_num_new++;
          }
        }
      }

      auto comment_creationDate_items = txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {"creationDate"});
      std::vector<vid_t> comment_vids_new;
      size_t commentx_num_new = 0;
      for (size_t i = 0; i < comment_vids.size(); i++)
      {
        auto creationDate = gbp::BufferBlock::Ref<Date>(comment_creationDate_items[0][i]).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          comment_vids_new.push_back(comment_vids[i]);
          if (i < commentx_num)
          {
            commentx_num_new++;
          }
        }
      }
      auto post_hasCreator_person_out_items = txn.BatchGetOutgoingSingleEdges(post_label_id_, person_label_id_, hasCreator_label_id_, post_vids_new);
      for (size_t i = 0; i < post_vids_new.size(); i++)
      {
        auto item = post_hasCreator_person_out_items[i];
        auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
        if (friends_[p])
        {
          if (i < postx_num_new)
          {
            count_[p].first += 1;
          }
          else
          {
            count_[p].second += 1;
          }
        }
      }

      auto comment_hasCreator_person_out_items = txn.BatchGetOutgoingSingleEdges(comment_label_id_, person_label_id_, hasCreator_label_id_, comment_vids_new);
      for (size_t i = 0; i < comment_vids_new.size(); i++)
      {
        auto item = comment_hasCreator_person_out_items[i];
        auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
        if (friends_[p])
        {
          if (i < commentx_num_new)
          {
            count_[p].first += 1;
          }
          else
          {
            count_[p].second += 1;
          }
        }
      }

      std::vector<std::pair<vid_t, size_t>> result_vids;
      for (auto friend_idx = 0; friend_idx < friends.size(); friend_idx++)
      {
        auto v = friends[friend_idx];
        if (count_[v].first && count_[v].second)
        {
          result_vids.emplace_back(v, count_[v].first + count_[v].second);
        }
      }
      // 使用 std::sort 根据第一个 int 排序
      std::sort(result_vids.begin(), result_vids.end(), [](const std::pair<vid_t, size_t> &a, const std::pair<vid_t, size_t> &b)
                {
                  return a.second > b.second; // 根据第一个元素排序
                });
      const size_t num_limit = 20;
      for (size_t i = 0; i < result_vids.size(); i++)
      {
        if (i < num_limit || result_vids[i].second == result_vids[i - 1].second)
        {
          pq.emplace(result_vids[i].second, result_vids[i].first, txn.GetVertexId(person_label_id_, result_vids[i].first));
        }
        else
        {
          break;
        }
      }

      std::vector<vid_t> neighbors;
      std::vector<person_info> vec;
      vec.reserve(pq.size());
      while (!pq.empty() && vec.size() < num_limit)
      {
        vec.emplace_back(pq.top());
        neighbors.push_back(vec.back().otherPerson_vid);
        pq.pop();
      }
      auto person_propertys_items = txn.BatchGetVertexPropsFromVids(person_label_id_, neighbors, {"firstName", "lastName"});

      for (size_t idx = 0; idx < vec.size(); idx++)
      {
        auto &p = vec[idx];
        auto v = p.otherPerson_vid;
        output.put_long(p.otherPerson_id);
        auto name = person_propertys_items[0][idx];
        output.put_buffer_object(name);

        name = person_propertys_items[1][idx];
        output.put_buffer_object(name);

        output.put_long(count_[v].first);
        output.put_long(count_[v].second);
        output.put_long(p.count);
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t place_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;
    label_t isLocatedIn_label_id_;
    label_t isPartOf_label_id_;

    std::vector<bool> friends_;
    std::vector<bool> place_Locatedin_;
    vid_t person_num_;
    std::vector<std::pair<int, int>> count_;

    const DateColumn &post_creationDate_col_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &place_name_col_;
    vid_t place_num_;

    GraphDBSession &graph_;
    // float BATCH_SIZE_RATIO = 1.0;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC3 *app = new gs::IC3(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC3 *casted = static_cast<gs::IC3 *>(app);
    delete casted;
  }
}

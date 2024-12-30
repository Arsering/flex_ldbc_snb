#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

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
          post_creationDate_col_(graph.GetPropertyHandle(post_label_id_, "creationDate")),
          comment_creationDate_col_(graph.GetPropertyHandle(comment_label_id_, "creationDate")),
          person_firstName_col_(graph.GetPropertyHandle(person_label_id_, "firstName")),
          person_lastName_col_(graph.GetPropertyHandle(person_label_id_, "lastName")),
          place_name_col_(graph.GetPropertyHandle(place_label_id_, "name")),
          place_num_(graph.graph().vertex_num(place_label_id_)),
          graph_(graph)
    {
      place_Locatedin_.resize(place_num_);
    }
    ~IC3() {}
    void get_friends(const ReadTransaction &txn, vid_t root,
                     std::vector<vid_t> &friends)
    {
      std::vector<vid_t> neighbors;

      auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_isLocatedIn_place_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              person_label_id_, place_label_id_, isLocatedIn_label_id_);
#if OV
      const auto &ie = person_knows_person_in.get_edges(root);
      for (auto &e : ie)
      {
        neighbors.push_back(e.neighbor);
      }
      const auto &oe = person_knows_person_out.get_edges(root);
      for (auto &e : oe)
      {
        neighbors.push_back(e.neighbor);
      }
#else
      auto ie = person_knows_person_in.get_edges(root);
      for (; ie.is_valid(); ie.next())
      {
        neighbors.push_back(ie.get_neighbor());
      }
      auto oe = person_knows_person_out.get_edges(root);
      for (; oe.is_valid(); oe.next())
      {
        neighbors.push_back(oe.get_neighbor());
      }
#endif
      friends_.clear();
      friends_.resize(person_num_, false);
      if (neighbors.empty())
      {
        return;
      }

      for (auto v : neighbors)
      {
#if OV
        assert(person_isLocatedIn_place_out.exist(v));
        auto p = person_isLocatedIn_place_out.get_edge(v).neighbor;
#else
        auto item = person_isLocatedIn_place_out.get_edge(v);
        assert(person_isLocatedIn_place_out.exist1(item));

        auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
        item.free();
#endif
        if (!place_Locatedin_[p] && (!friends_[v]))
        {
          friends.push_back(v);
        }
        friends_[v] = true;
#if OV
        const auto &iev = person_knows_person_in.get_edges(v);
        for (auto &e : iev)
        {
          if (!friends_[e.neighbor])
          {
            friends_[e.neighbor] = true;
            assert(person_isLocatedIn_place_out.exist(e.neighbor));
            auto p = person_isLocatedIn_place_out.get_edge(e.neighbor).neighbor;
            if (!place_Locatedin_[p])
            {
              friends.push_back(e.neighbor);
            }
#else
        auto iev = person_knows_person_in.get_edges(v);
        for (; iev.is_valid(); iev.next())
        {
          if (!friends_[iev.get_neighbor()])
          {
            friends_[iev.get_neighbor()] = true;
            auto item = person_isLocatedIn_place_out.get_edge(iev.get_neighbor());
            assert(person_isLocatedIn_place_out.exist1(item));

            auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            item.free();
            if (!place_Locatedin_[p])
            {
              friends.push_back(iev.get_neighbor());
            }
#endif
          }
        }
#if OV
        const auto &oev = person_knows_person_out.get_edges(v);
        for (auto &e : oev)
        {
          if (!friends_[e.neighbor])
          {
            assert(person_isLocatedIn_place_out.exist(e.neighbor));
            auto p = person_isLocatedIn_place_out.get_edge(e.neighbor).neighbor;
            if (!place_Locatedin_[p])
            {
              friends.push_back(e.neighbor);
            }
            friends_[e.neighbor] = true;
          }
        }
#else
        auto oev = person_knows_person_out.get_edges(v);
        for (; oev.is_valid(); oev.next())
        {
          auto neb = oev.get_neighbor();
          if (!friends_[neb])
          {
            auto item = person_isLocatedIn_place_out.get_edge(neb);
            assert(person_isLocatedIn_place_out.exist1(item));

            auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            if (!place_Locatedin_[p])
            {
              friends.push_back(neb);
            }
            friends_[neb] = true;
          }
        }
#endif
      }
    }

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
        if (lhs.count > rhs.count)
        {
          return true;
        }
        if (lhs.count < rhs.count)
        {
          return false;
        }
        return lhs.otherPerson_id < rhs.otherPerson_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      // std::cout<<"begin query"<<std::endl;
      person_count=0;
      message_count=0;
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      std::string_view countryxname = input.get_string();
      std::string_view countryyname = input.get_string();
      int64_t start_date = input.get_long();
      int durationdays = input.get_int();
      CHECK(input.empty());

      friends_.clear();

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }

      vid_t countryX = place_num_;
      vid_t countryY = place_num_;
      for (vid_t i = 0; i < place_num_; ++i)
      {
        auto place_name_item = place_name_col_.getProperty(i);

        if (place_name_item == countryxname)
        {
          countryX = i;
        }
        if (place_name_item == countryyname)
        {
          countryY = i;
        }
      }
      assert(countryX != place_num_);
      assert(countryY != place_num_);

      auto place_isPartOf_place_in=txn.GetIncomingGraphView<grape::EmptyType>(place_label_id_, place_label_id_, isPartOf_label_id_);

      // auto xe = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryX, place_label_id_, isPartOf_label_id_);
      auto xe = place_isPartOf_place_in.get_edges(countryX);
      for (; xe.is_valid(); xe.next())
      {
        place_Locatedin_[xe.get_neighbor()] = true;
      }
      // auto ye = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryY, place_label_id_, isPartOf_label_id_);
      auto ye = place_isPartOf_place_in.get_edges(countryY);
      for (; ye.is_valid(); ye.next())
      {
        place_Locatedin_[ye.get_neighbor()] = true;
      }
      
      std::vector<vid_t> friends{};
      person_num_ = txn.GetVertexNum(person_label_id_);
      count_.clear();
      count_.resize(person_num_, {});

      get_friends(txn, root, friends);
      const int64_t milli_sec_per_day = 24 * 60 * 60 * 1000l;
      int64_t end_date = start_date + durationdays * milli_sec_per_day;
      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);

      auto post_hasCreator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              post_label_id_, person_label_id_, hasCreator_label_id_);
      auto comment_hasCreator_person_out =
          txn.GetOutgoingSingleGraphView<grape::EmptyType>(
              comment_label_id_, person_label_id_, hasCreator_label_id_);

      auto post_isLocatedIn_place_in=txn.GetIncomingGraphView<grape::EmptyType>(place_label_id_, post_label_id_, isLocatedIn_label_id_);
      auto comment_isLocatedIn_place_in=txn.GetIncomingGraphView<grape::EmptyType>(place_label_id_,comment_label_id_,  isLocatedIn_label_id_);

      // auto postex = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryX, post_label_id_, isLocatedIn_label_id_);
      auto postex = post_isLocatedIn_place_in.get_edges(countryX);
      for (; postex.is_valid(); postex.next())
      {
        auto item = post_creationDate_col_.getProperty(postex.get_neighbor());
        auto creationDate = gbp::BufferBlock::Ref<Date>(item).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          auto item = post_hasCreator_person_out.get_edge(postex.get_neighbor());
          assert(post_hasCreator_person_out.exist1(item));

          auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (friends_[p])
          {
            count_[p].first += 1;
          }
        }
      }

      // auto commentex = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryX, comment_label_id_, isLocatedIn_label_id_);
      auto commentex = comment_isLocatedIn_place_in.get_edges(countryX);
      for (; commentex.is_valid(); commentex.next())
      {
        auto item = comment_creationDate_col_.getProperty(commentex.get_neighbor());
        auto creationDate = gbp::BufferBlock::Ref<Date>(item).milli_second;
        if (start_date <= creationDate && creationDate < end_date)
        {
          auto item = comment_hasCreator_person_out.get_edge(commentex.get_neighbor());
          assert(comment_hasCreator_person_out.exist1(item));

          auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (friends_[p])
          {
            count_[p].first += 1;
          }
        }
      }

      // auto postey = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryY, post_label_id_, isLocatedIn_label_id_);
      auto postey = post_isLocatedIn_place_in.get_edges(countryY);
      for (; postey.is_valid(); postey.next())
      {
        auto item = post_creationDate_col_.getProperty(postey.get_neighbor());
        auto creationDate = gbp::BufferBlock::Ref<Date>(item).milli_second;
        item.free();
        if (start_date <= creationDate && creationDate < end_date)
        {
          item = post_hasCreator_person_out.get_edge(postey.get_neighbor());
          assert(post_hasCreator_person_out.exist1(item));

          auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          item.free();
          if (friends_[p])
          {
            count_[p].second += 1;
          }
        }
      }
      // auto commentey = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryY, comment_label_id_, isLocatedIn_label_id_);
      auto commentey = comment_isLocatedIn_place_in.get_edges(countryY);
      for (; commentey.is_valid(); commentey.next())
      {
        auto item = comment_creationDate_col_.getProperty(commentey.get_neighbor());
        auto creationDate = gbp::BufferBlock::Ref<Date>(item).milli_second;
        item.free();
        if (start_date <= creationDate && creationDate < end_date)
        {
          item = comment_hasCreator_person_out.get_edge(commentey.get_neighbor());
          assert(comment_hasCreator_person_out.exist1(item));

          auto p = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          item.free();

          if (friends_[p])
          {
            count_[p].second += 1;
          }
        }
      }

      for (auto v : friends)
      {
        if (count_[v].first && count_[v].second)
        {
          int count = count_[v].first + count_[v].second;
          if (pq.size() < 20)
          {
            pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
          }
          else
          {
            const person_info &top = pq.top();
            if (count > top.count)
            {
              pq.pop();
              pq.emplace(count, v, txn.GetVertexId(person_label_id_, v));
            }
            else if (count == top.count)
            {
              oid_t otherPerson_id = txn.GetVertexId(person_label_id_, v);
              if (otherPerson_id < top.otherPerson_id)
              {
                pq.pop();
                pq.emplace(count, v, otherPerson_id);
              }
            }
          }
        }
      }

      std::vector<person_info> vec;
      // std::ofstream outfile1;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        pq.pop();
      }
      // outfile1.open("/data-1/yichengzhang/data/latest_gs_bp/graphscope-flex/experiment_space/LDBC_SNB/query_access_log/90_ic3_log",std::ios::app);
      // outfile1<<vec.size()<<std::endl;
      for (size_t idx = vec.size(); idx > 0; --idx)
      {
        auto &p = vec[idx - 1];
        auto v = p.otherPerson_vid;
        output.put_long(p.otherPerson_id);

        auto name = person_firstName_col_.getProperty(v);
        output.put_buffer_object(name);

        name = person_lastName_col_.getProperty(v);
        output.put_buffer_object(name);

        output.put_long(count_[v].first);
        output.put_long(count_[v].second);
        output.put_long(p.count);
      }
      // auto xe_1 = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryX, place_label_id_, isPartOf_label_id_);
      // auto ye_1 = txn.GetIncomingEdges<grape::EmptyType>(
      //     place_label_id_, countryY, place_label_id_, isPartOf_label_id_);
      auto xe_1 = place_isPartOf_place_in.get_edges(countryX);
      for (; xe_1.is_valid(); xe_1.next())
      {
        place_Locatedin_[xe_1.get_neighbor()] = false;
      }
      auto ye_1 = place_isPartOf_place_in.get_edges(countryY);
      for (; ye_1.is_valid(); ye_1.next())
      {
        place_Locatedin_[ye_1.get_neighbor()] = false;
      }
      // std::cout<<"end query"<<std::endl;
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

    cgraph::PropertyHandle post_creationDate_col_;
    cgraph::PropertyHandle comment_creationDate_col_;
    cgraph::PropertyHandle person_firstName_col_;
    cgraph::PropertyHandle person_lastName_col_;
    cgraph::PropertyHandle place_name_col_;

    vid_t place_num_;

    GraphDBSession &graph_;
    int person_count;
    int message_count=0;
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

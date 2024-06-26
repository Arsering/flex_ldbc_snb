#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
// #include "utils.h"

namespace gs
{

  inline uint64_t get_pair_value(vid_t src, vid_t dst)
  {
    return src > dst
               ? (static_cast<uint64_t>(src) | (static_cast<uint64_t>(dst) << 32))
               : (static_cast<uint64_t>(dst) |
                  (static_cast<uint64_t>(src) << 32));
  }

  class IC14Impl
  {
  public:
    IC14Impl(const ReadTransaction &txn, label_t person_label_id,
             label_t post_label_id, label_t comment_label_id,
             label_t hasCreator_label_id, label_t replyOf_label_id)
        : post_hasCreator_person_out_(
              txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                  post_label_id, person_label_id, hasCreator_label_id)),
          comment_hasCreator_person_out_(
              txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                  comment_label_id, person_label_id, hasCreator_label_id)),
          post_hasCreator_person_in_(txn.GetIncomingGraphView<grape::EmptyType>(
              person_label_id, post_label_id, hasCreator_label_id)),
          comment_hasCreator_person_in_(
              txn.GetIncomingGraphView<grape::EmptyType>(
                  person_label_id, comment_label_id, hasCreator_label_id)),
          comment_replyOf_post_out_(
              txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                  comment_label_id, post_label_id, replyOf_label_id)),
          comment_replyOf_comment_out_(
              txn.GetOutgoingSingleGraphView<grape::EmptyType>(
                  comment_label_id, comment_label_id, replyOf_label_id)),
          comment_replyOf_post_in_(txn.GetIncomingGraphView<grape::EmptyType>(
              post_label_id, comment_label_id, replyOf_label_id)),
          comment_replyOf_comment_in_(txn.GetIncomingGraphView<grape::EmptyType>(
              comment_label_id, comment_label_id, replyOf_label_id)) {}

    int get_score(vid_t x, vid_t y) const
    {
#if !OV
      bool exist_mark = false;
#endif
      int score = 0;
#if OV
      const auto &x_ie = comment_hasCreator_person_in_.get_edges(x);
      for (auto &e : x_ie)
      {
        auto v = e.neighbor;
        if (comment_replyOf_post_out_.exist(v))
        {
          auto &e1 = comment_replyOf_post_out_.get_edge(v);
          assert(post_hasCreator_person_out_.exist(e1.neighbor));
          auto n = post_hasCreator_person_out_.get_edge(e1.neighbor).neighbor;
          if (n == y)
          {
            score += 2;
          }
        }
        else if (comment_replyOf_comment_out_.exist(v))
        {
          auto &e2 = comment_replyOf_comment_out_.get_edge(v);
          assert(comment_hasCreator_person_out_.exist(e2.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(e2.neighbor).neighbor;
          if (n == y)
          {
            score += 1;
          }
        }
      }
      const auto &y_ie = comment_hasCreator_person_in_.get_edges(y);
      for (auto &e : y_ie)
      {
        auto v = e.neighbor;
        if (comment_replyOf_post_out_.exist(v))
        {
          auto &e1 = comment_replyOf_post_out_.get_edge(v);
          assert(post_hasCreator_person_out_.exist(e1.neighbor));
          auto n = post_hasCreator_person_out_.get_edge(e1.neighbor).neighbor;
          if (n == x)
          {
            score += 2;
          }
        }
        else if (comment_replyOf_comment_out_.exist(v))
        {
          auto &e2 = comment_replyOf_comment_out_.get_edge(v);
          assert(comment_hasCreator_person_out_.exist(e2.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(e2.neighbor).neighbor;
          if (n == x)
          {
            score += 1;
          }
        }
      }
#else
      auto x_ie = comment_hasCreator_person_in_.get_edges(x);
      for (; x_ie.is_valid(); x_ie.next())
      {
        auto v = x_ie.get_neighbor();
        auto e1 = comment_replyOf_post_out_.exist(v, exist_mark);
        if (exist_mark)
        {
          auto e1_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e1).neighbor;

          auto item = post_hasCreator_person_out_.exist(e1_neb, exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (n == y)
          {
            score += 2;
          }
        }
        else
        {
          auto e2 = comment_replyOf_comment_out_.exist(v, exist_mark);
          if (exist_mark)
          {
            auto e2_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e2).neighbor;

            auto item = comment_hasCreator_person_out_.exist(e2_neb, exist_mark);
            assert(exist_mark);
            auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            if (n == y)
            {
              score += 1;
            }
          }
        }
      }
      auto y_ie = comment_hasCreator_person_in_.get_edges(y);
      for (; y_ie.is_valid(); y_ie.next())
      {
        auto v = y_ie.get_neighbor();
        auto e1 = comment_replyOf_post_out_.exist(v, exist_mark);
        if (exist_mark)
        {
          auto e1_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e1).neighbor;

          auto item = post_hasCreator_person_out_.exist(e1_neb, exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (n == x)
          {
            score += 2;
          }
        }
        else
        {
          auto e2 = comment_replyOf_comment_out_.exist(v, exist_mark);
          if (exist_mark)
          {
            auto e2_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e2).neighbor;

            auto item = comment_hasCreator_person_out_.exist(e2_neb, exist_mark);
            assert(exist_mark);
            auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            if (n == x)
            {
              score += 1;
            }
          }
        }
      }
#endif
      return score;
    }

    int get_score_1(vid_t root, vid_t x) const
    {

#if !OV
      bool exist_mark = false;
#endif
      int root_degree =
          comment_hasCreator_person_in_.get_edges(root).estimated_degree() +
          post_hasCreator_person_in_.get_edges(root).estimated_degree();
      int x_degree =
          comment_hasCreator_person_in_.get_edges(x).estimated_degree() +
          post_hasCreator_person_in_.get_edges(x).estimated_degree();
      vid_t u, v;
      if (root_degree > x_degree)
      {
        u = x;
        v = root;
      }
      else
      {
        u = root;
        v = x;
      }

      int ret = 0;
#if OV
      const auto &comment_ie = comment_hasCreator_person_in_.get_edges(u);
      for (auto &e : comment_ie)
      {
        if (comment_replyOf_post_out_.exist(e.neighbor))
        {
          auto &e1 = comment_replyOf_post_out_.get_edge(e.neighbor);
          assert(post_hasCreator_person_out_.exist(e1.neighbor));
          auto n = post_hasCreator_person_out_.get_edge(e1.neighbor).neighbor;
          if (n == v)
          {
            ret += 2;
          }
        }
        else if (comment_replyOf_comment_out_.exist(e.neighbor))
        {
          auto &e2 = comment_replyOf_comment_out_.get_edge(e.neighbor);
          assert(comment_hasCreator_person_out_.exist(e2.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(e2.neighbor).neighbor;
          if (n == v)
          {
            ret += 1;
          }
        }

        const auto &follows_ie =
            comment_replyOf_comment_in_.get_edges(e.neighbor);
        for (auto &f : follows_ie)
        {
          assert(comment_hasCreator_person_out_.exist(f.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(f.neighbor).neighbor;
          if (n == v)
          {
            ret += 1;
          }
        }
      }
      const auto &post_ie = post_hasCreator_person_in_.get_edges(u);
      for (auto &e : post_ie)
      {
        const auto &follows_ie = comment_replyOf_post_in_.get_edges(e.neighbor);
        for (auto &f : follows_ie)
        {
          assert(comment_hasCreator_person_out_.exist(f.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(f.neighbor).neighbor;
          if (n == v)
          {
            ret += 2;
          }
        }
      }
#else
      auto comment_ie = comment_hasCreator_person_in_.get_edges(u);
      for (; comment_ie.is_valid(); comment_ie.next())
      {
        auto e1 = comment_replyOf_post_out_.exist(comment_ie.get_neighbor(), exist_mark);
        if (exist_mark)
        {
          auto e1_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e1).neighbor;

          auto item = post_hasCreator_person_out_.exist(e1_neb, exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (n == v)
          {
            ret += 2;
          }
        }
        else
        {
          auto e2 = comment_replyOf_comment_out_.exist(comment_ie.get_neighbor(), exist_mark);
          if (exist_mark)
          {
            auto e2_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e2).neighbor;
            auto item = comment_hasCreator_person_out_.exist(e2_neb, exist_mark);
            assert(exist_mark);
            auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            if (n == v)
            {
              ret += 1;
            }
          }
        }

        auto follows_ie =
            comment_replyOf_comment_in_.get_edges(comment_ie.get_neighbor());
        for (; follows_ie.is_valid(); follows_ie.next())
        {
          auto item = comment_hasCreator_person_out_.exist(follows_ie.get_neighbor(), exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (n == v)
          {
            ret += 1;
          }
        }
      }
      auto post_ie = post_hasCreator_person_in_.get_edges(u);
      for (; post_ie.is_valid(); post_ie.next())

      {
        auto follows_ie = comment_replyOf_post_in_.get_edges(post_ie.get_neighbor());
        for (; follows_ie.is_valid(); follows_ie.next())
        {
          auto item = comment_hasCreator_person_out_.exist(follows_ie.get_neighbor(), exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (n == v)
          {
            ret += 2;
          }
        }
      }
#endif
      return ret;
    }

    void calc_scores(vid_t root, std::vector<int> &count) const
    {
#if !OV
      bool exist_mark = false;
#endif

#if OV
      const auto &comment_ie = comment_hasCreator_person_in_.get_edges(root);
      for (auto &e : comment_ie)
      {
        if (comment_replyOf_post_out_.exist(e.neighbor))
        {
          auto &e1 = comment_replyOf_post_out_.get_edge(e.neighbor);
          assert(post_hasCreator_person_out_.exist(e1.neighbor));
          auto n = post_hasCreator_person_out_.get_edge(e1.neighbor).neighbor;
          if (count[n] != 0)
          {
            count[n] += 2;
          }
        }
        else if (comment_replyOf_comment_out_.exist(e.neighbor))
        {
          auto &e2 = comment_replyOf_comment_out_.get_edge(e.neighbor);
          assert(comment_hasCreator_person_out_.exist(e2.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(e2.neighbor).neighbor;
          if (count[n] != 0)
          {
            count[n] += 1;
          }
        }
        const auto &follows_ie =
            comment_replyOf_comment_in_.get_edges(e.neighbor);
        for (auto &e1 : follows_ie)
        {
          assert(comment_hasCreator_person_out_.exist(e1.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(e1.neighbor).neighbor;
          if (count[n] != 0)
          {
            count[n] += 1;
          }
        }
      }
      const auto &post_ie = post_hasCreator_person_in_.get_edges(root);
      for (auto &e : post_ie)
      {
        const auto &follows_ie = comment_replyOf_post_in_.get_edges(e.neighbor);
        for (auto &f : follows_ie)
        {
          assert(comment_hasCreator_person_out_.exist(f.neighbor));
          auto n = comment_hasCreator_person_out_.get_edge(f.neighbor).neighbor;
          if (count[n] != 0)
          {
            count[n] += 2;
          }
        }
      }
#else
      auto comment_ie = comment_hasCreator_person_in_.get_edges(root);
      for (; comment_ie.is_valid(); comment_ie.next())
      {
        auto e1 = comment_replyOf_post_out_.exist(comment_ie.get_neighbor(), exist_mark);
        if (exist_mark)
        {
          auto e1 = comment_replyOf_post_out_.get_edge(comment_ie.get_neighbor());
          auto e1_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e1).neighbor;

          auto item = post_hasCreator_person_out_.exist(e1_neb, exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (count[n] != 0)
          {
            count[n] += 2;
          }
        }
        else
        {
          auto e2 = comment_replyOf_comment_out_.exist(comment_ie.get_neighbor(), exist_mark);
          if (exist_mark)
          {
            auto e2_neb = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e2).neighbor;

            auto item = comment_hasCreator_person_out_.exist(e2_neb, exist_mark);
            assert(exist_mark);
            auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
            if (count[n] != 0)
            {
              count[n] += 1;
            }
          }
        }
        auto follows_ie =
            comment_replyOf_comment_in_.get_edges(comment_ie.get_neighbor());
        for (; follows_ie.is_valid(); follows_ie.next())
        {
          auto item = comment_hasCreator_person_out_.exist(follows_ie.get_neighbor(), exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (count[n] != 0)
          {
            count[n] += 1;
          }
        }
      }
      auto post_ie = post_hasCreator_person_in_.get_edges(root);
      for (; post_ie.is_valid(); post_ie.next())
      {
        auto follows_ie = comment_replyOf_post_in_.get_edges(post_ie.get_neighbor());
        for (; follows_ie.is_valid(); follows_ie.next())
        {
          auto item = comment_hasCreator_person_out_.exist(follows_ie.get_neighbor(), exist_mark);
          assert(exist_mark);
          auto n = gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(item).neighbor;
          if (count[n] != 0)
          {
            count[n] += 2;
          }
        }
      }
#endif
    }

  private:
    SingleGraphView<grape::EmptyType> post_hasCreator_person_out_;
    SingleGraphView<grape::EmptyType> comment_hasCreator_person_out_;
    GraphView<grape::EmptyType> comment_hasCreator_person_in_;
    GraphView<grape::EmptyType> post_hasCreator_person_in_;

    SingleGraphView<grape::EmptyType> comment_replyOf_post_out_;
    SingleGraphView<grape::EmptyType> comment_replyOf_comment_out_;
    GraphView<grape::EmptyType> comment_replyOf_post_in_;
    GraphView<grape::EmptyType> comment_replyOf_comment_in_;
  };

  class IC14 : public AppBase
  {
  public:
    IC14(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          graph_(graph) {}
    ~IC14() {}

    void generate_scores(const ReadTransaction &txn)
    {
      IC14Impl impl(txn, person_label_id_, post_label_id_, comment_label_id_,
                    hasCreator_label_id_, replyOf_label_id_);

      if (paths_.size() == 1)
      {
        auto &path = paths_[0];
        size_t length = path.size();
        int score = 0;
        for (size_t i = 1; i < length; ++i)
        {
          vid_t src = path[i - 1];
          vid_t dst = path[i];
          score += impl.get_score(src, dst);
        }
        scores_.push_back(score);
      }
      else
      {
        std::map<vid_t, std::set<vid_t>> person_pairs;
        std::unordered_map<uint64_t, int> score_cache;
        for (auto &path : paths_)
        {
          size_t len = path.size();
          for (size_t i = 1; i < len; ++i)
          {
            vid_t src = path[i - 1];
            vid_t dst = path[i];
            person_pairs[src].insert(dst);
            person_pairs[dst].insert(src);
          }
        }
        vid_t person_num = txn.GetVertexNum(person_label_id_);
        std::vector<int> count(person_num, 0);
        while (!person_pairs.empty())
        {
          vid_t root = std::numeric_limits<vid_t>::max();
          int max_degree = 0;
          for (auto &pair : person_pairs)
          {
            if (static_cast<int>(pair.second.size()) > max_degree)
            {
              max_degree = pair.second.size();
              root = pair.first;
            }
          }

          if (root == std::numeric_limits<vid_t>::max())
          {
            break;
          }
          auto &nbr_set = person_pairs[root];
          if (nbr_set.empty())
          {
            break;
          }
          auto nbr_num = nbr_set.size();
          if (nbr_num == 1)
          {
            auto v = *nbr_set.begin();
            score_cache[get_pair_value(root, v)] += impl.get_score_1(root, v);
            person_pairs[v].erase(root);
          }
          else
          {
            for (auto v : nbr_set)
            {
              count[v] = 1;
            }
            impl.calc_scores(root, count);
            for (auto v : nbr_set)
            {
              score_cache[get_pair_value(root, v)] += (count[v] - 1);
              count[v] = 0;
              person_pairs[v].erase(root);
            }
          }

          person_pairs.erase(root);
        }

        for (auto &path : paths_)
        {
          size_t length = path.size();
          int score = 0;
          for (size_t i = 1; i < length; ++i)
          {
            score += score_cache.at(get_pair_value(path[i - 1], path[i]));
          }
          scores_.push_back(score);
        }
      }
    }

    void dfs(const GraphView<Date> &person_knows_person_out,
             const GraphView<Date> &person_knows_person_in, vid_t src, vid_t dst,
             std::vector<vid_t> &vec)
    {
      vec.push_back(src);
      if (src == dst)
      {
        paths_.push_back(vec);
        vec.pop_back();
        return;
      }
#if OV
      const auto &oe = person_knows_person_out.get_edges(src);
      for (auto &e : oe)
      {
        auto v = e.neighbor;
        if (persons_[v] && dis_from_src_[v] == dis_from_src_[src] + 1)
        {
          dfs(person_knows_person_out, person_knows_person_in, v, dst, vec);
        }
      }
      const auto &ie = person_knows_person_in.get_edges(src);
      for (auto &e : ie)
      {
        auto v = e.neighbor;
        if (persons_[v] && dis_from_src_[v] == dis_from_src_[src] + 1)
        {
          dfs(person_knows_person_out, person_knows_person_in, v, dst, vec);
        }
      }
#else
      auto oe = person_knows_person_out.get_edges(src);
      for (; oe.is_valid(); oe.next())
      {
        auto v = oe.get_neighbor();
        if (persons_[v] && dis_from_src_[v] == dis_from_src_[src] + 1)
        {
          dfs(person_knows_person_out, person_knows_person_in, v, dst, vec);
        }
      }
      auto ie = person_knows_person_in.get_edges(src);
      for (; ie.is_valid(); ie.next())
      {
        auto v = ie.get_neighbor();
        if (persons_[v] && dis_from_src_[v] == dis_from_src_[src] + 1)
        {
          dfs(person_knows_person_out, person_knows_person_in, v, dst, vec);
        }
      }
#endif
      vec.pop_back();
    }

    void next_tire(const GraphView<Date> &person_knows_person_out,
                   const GraphView<Date> &person_knows_person_in, int8_t depth,
                   std::queue<vid_t> &curr, std::queue<vid_t> &next,
                   std::vector<int8_t> &dis0, std::vector<int8_t> &dis1,
                   std::vector<vid_t> &vec, bool dir = true)
    {
      while (!curr.empty())
      {
        auto x = curr.front();
        curr.pop();
#if OV
        const auto &oe = person_knows_person_out.get_edges(x);
        for (auto &e : oe)
        {
          auto v = e.neighbor;
#else
        auto oe = person_knows_person_out.get_edges(x);
        for (; oe.is_valid(); oe.next())
        {
          auto v = oe.get_neighbor();
#endif
          if (dis0[v] == -1)
          {
            dis0[v] = depth;
            next.push(v);
            if (dis1[v] != -1)
            {
              vec.push_back(v);
            }
          }
        }
#if OV
        const auto &ie = person_knows_person_in.get_edges(x);
        for (auto &e : ie)
        {
          auto v = e.neighbor;
#else
        auto ie = person_knows_person_in.get_edges(x);
        for (; ie.is_valid(); ie.next())
        {
          auto v = ie.get_neighbor();
#endif
          if (dis0[v] == -1)
          {
            dis0[v] = depth;
            next.push(v);
            if (dis1[v] != -1)
            {
              vec.push_back(v);
            }
          }
        }
      }
    }

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
        return false;
      }
      if (!txn.GetVertexIndex(person_label_id_, person2id, dst))
      {
        return false;
      }
      dis_from_src_.clear();
      dis_from_src_.resize(person_num, -1);
      dis_from_dst_.clear();
      dis_from_dst_.resize(person_num, -1);
      dis_from_src_[src] = 0;
      dis_from_dst_[dst] = 0;
      persons_.clear();
      paths_.clear();
      scores_.clear();
      std::queue<vid_t> q1, q2;
      q1.push(src);
      q2.push(dst);
      std::queue<vid_t> tmp;
      std::vector<vid_t> vec;
      int8_t src_dep = 0, dst_dep = 0;

      auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);
      auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
          person_label_id_, person_label_id_, knows_label_id_);

      while (true)
      {
        if (!q1.empty() && (q1.size() <= q2.size()))
        {
          ++src_dep;
          next_tire(person_knows_person_out, person_knows_person_in, src_dep, q1,
                    tmp, dis_from_src_, dis_from_dst_, vec, true);
          if (!vec.empty())
          {
            break;
          }
          std::swap(q1, tmp);
        }
        else
        {
          ++dst_dep;
          next_tire(person_knows_person_out, person_knows_person_in, dst_dep, q2,
                    tmp, dis_from_dst_, dis_from_src_, vec, false);
          if (!vec.empty())
          {
            break;
          }
          std::swap(q2, tmp);
        }
        if (q1.empty() || q2.empty())
          break;
      }

      while (!q1.empty())
        q1.pop();
      if (vec.empty())
        return true;
      persons_.clear();
      persons_.resize(txn.GetVertexNum(person_label_id_));
      for (auto v : vec)
      {
        q1.push(v);
        persons_[v] = true;
      }

      while (!q1.empty())
      {
        auto v = q1.front();
        q1.pop();
#if OV
        const auto &oe = person_knows_person_out.get_edges(v);
        for (auto &e : oe)
        {
          if (persons_[e.neighbor])
          {
            continue;
          }
          if ((dis_from_src_[e.neighbor] != -1) &&
              (dis_from_src_[e.neighbor] + 1 == dis_from_src_[v]))
          {
            q1.push(e.neighbor);
            persons_[e.neighbor] = true;
          }
          if ((dis_from_dst_[e.neighbor] != -1) &&
              (dis_from_dst_[e.neighbor] + 1 == dis_from_dst_[v]))
          {
            q1.push(e.neighbor);
            persons_[e.neighbor] = true;
            dis_from_src_[e.neighbor] = dis_from_src_[v] + 1;
          }
        }
#else
        auto oe = person_knows_person_out.get_edges(v);
        for (; oe.is_valid(); oe.next())
        {
          auto e_neb = oe.get_neighbor();
          if (persons_[e_neb])
          {
            continue;
          }
          if ((dis_from_src_[e_neb] != -1) &&
              (dis_from_src_[e_neb] + 1 == dis_from_src_[v]))
          {
            q1.push(e_neb);
            persons_[e_neb] = true;
          }
          if ((dis_from_dst_[e_neb] != -1) &&
              (dis_from_dst_[e_neb] + 1 == dis_from_dst_[v]))
          {
            q1.push(e_neb);
            persons_[e_neb] = true;
            dis_from_src_[e_neb] = dis_from_src_[v] + 1;
          }
        }
#endif
#if OV
        const auto &ie = person_knows_person_in.get_edges(v);
        for (auto &e : ie)
        {
          if (persons_[e.neighbor])
          {
            continue;
          }
          if ((dis_from_src_[e.neighbor] != -1) &&
              (dis_from_src_[e.neighbor] + 1 == dis_from_src_[v]))
          {
            q1.push(e.neighbor);
            persons_[e.neighbor] = true;
          }
          if ((dis_from_dst_[e.neighbor] != -1) &&
              (dis_from_dst_[e.neighbor] + 1 == dis_from_dst_[v]))
          {
            q1.push(e.neighbor);
            persons_[e.neighbor] = true;
            dis_from_src_[e.neighbor] = dis_from_src_[v] + 1;
          }
        }
#else
        auto ie = person_knows_person_in.get_edges(v);
        for (; ie.is_valid(); ie.next())
        {
          auto e_neb = ie.get_neighbor();
          if (persons_[e_neb])
          {
            continue;
          }
          if ((dis_from_src_[e_neb] != -1) &&
              (dis_from_src_[e_neb] + 1 == dis_from_src_[v]))
          {
            q1.push(e_neb);
            persons_[e_neb] = true;
          }
          if ((dis_from_dst_[e_neb] != -1) &&
              (dis_from_dst_[e_neb] + 1 == dis_from_dst_[v]))
          {
            q1.push(e_neb);
            persons_[e_neb] = true;
            dis_from_src_[e_neb] = dis_from_src_[v] + 1;
          }
        }
#endif
      }

      std::vector<vid_t> v;
      dfs(person_knows_person_out, person_knows_person_in, src, dst, v);
      generate_scores(txn);

      for (size_t i = 0; i < paths_.size(); i++)
      {
        v.push_back(i);
      }
      sort(v.begin(), v.end(),
           [&](const int a, const int b)
           { return scores_[b] < scores_[a]; });

      for (size_t i = 0; i < v.size(); i++)
      {
        auto x = v[i];
        output.put_int(paths_[x].size());
        for (auto i : paths_[x])
        {
          output.put_long(txn.GetVertexId(person_label_id_, i));
        }
        double score = scores_[x];
        score = score / 2.0f;
        output.put_long(*reinterpret_cast<const long *>(&score));
      }
      return true;
    }

  private:
#if !OV
    bool exist_mark = false;
#endif

    label_t person_label_id_;
    label_t knows_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t hasCreator_label_id_;
    label_t replyOf_label_id_;

    std::vector<int8_t> dis_from_src_;
    std::vector<int8_t> dis_from_dst_;
    std::vector<bool> persons_;
    std::vector<std::vector<vid_t>> paths_;
    std::vector<int> scores_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC14 *app = new gs::IC14(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC14 *casted = static_cast<gs::IC14 *>(app);
    delete casted;
  }
}

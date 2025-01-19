#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs
{

  class IC7 : public AppBase
  {
  public:
    IC7(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          likes_label_id_(graph.schema().get_edge_label_id("LIKES")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          person_firstName_col_(graph.GetPropertyHandle(person_label_id_, "firstName")),
          person_lastName_col_(graph.GetPropertyHandle(person_label_id_, "lastName")),
          post_content_col_(graph.GetPropertyHandle(post_label_id_, "content")),
          post_imageFile_col_(graph.GetPropertyHandle(post_label_id_, "imageFile")),
          post_length_col_(graph.GetPropertyHandle(post_label_id_, "length")),
          comment_content_col_(graph.GetPropertyHandle(comment_label_id_, "content")),
          post_creationDate_col_(graph.GetPropertyHandle(post_label_id_, "creationDate")),
          comment_creationDate_col_(graph.GetPropertyHandle(comment_label_id_, "creationDate")),
          graph_(graph) {}
    ~IC7() {}

    struct person_info
    {
      person_info(vid_t v, vid_t mid, int64_t creationDate, oid_t person_id,
                  oid_t message_id, bool is_post)
          : v(v),
            mid(mid),
            creationDate(creationDate),
            person_id(person_id),
            message_id(message_id),
            is_post(is_post) {}

      vid_t v;
      vid_t mid;
      int64_t creationDate;
      oid_t person_id;
      oid_t message_id;
      bool is_post;
    };

    struct person_info_comparer
    {
      bool operator()(const person_info &lhs, const person_info &rhs)
      {
        if (lhs.creationDate > rhs.creationDate)
        {
          return true;
        }
        if (lhs.creationDate < rhs.creationDate)
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
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      auto person_num = txn.GetVertexNum(person_label_id_);
      friends_.clear();
      friends_.resize(person_num);
      auto ie = txn.GetIncomingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; ie.is_valid(); ie.next())
      {
        friends_[ie.get_neighbor()] = true;
      }
      auto oe = txn.GetOutgoingEdges<Date>(
          person_label_id_, root, person_label_id_, knows_label_id_);
      for (; oe.is_valid(); oe.next())
      {
        friends_[oe.get_neighbor()] = true;
      }
      std::vector<person_info> vec;

      auto person_likes_post_in = txn.GetIncomingGraphView<Date>(
          post_label_id_, person_label_id_, likes_label_id_);

      auto post_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, post_label_id_, hasCreator_label_id_);
      std::vector<vid_t> post_vids;
      for(;post_ie.is_valid();post_ie.next()){
        post_vids.push_back(post_ie.get_neighbor());
      }
      // auto post_person_likes_in_items=txn.BatchGetVidsNeighbors<grape::EmptyType>(post_label_id_,person_label_id_,  likes_label_id_, post_vids, false);
      auto post_person_likes_in_items=txn.BatchGetEdgePropsFromSrcVids<Date>(post_label_id_,person_label_id_,  likes_label_id_, post_vids, false);
      auto post_oids=txn.BatchGetVertexIds(post_label_id_, post_vids);
      for (int i=0;i<post_vids.size();i++){
        auto likes_persons = post_person_likes_in_items[i];
        auto pid = post_vids[i];
        auto message_id = post_oids[i];
        auto person_ie = person_likes_post_in.get_edges(pid);
        for (int j=0;j<likes_persons.size();j++){
          auto item = likes_persons[j];
          vec.emplace_back(item.first, pid, item.second.milli_second,
                           txn.GetVertexId(person_label_id_, item.first),
                           message_id, true);
        }
      }
      auto person_likes_comment_in = txn.GetIncomingGraphView<Date>(
          comment_label_id_, person_label_id_, likes_label_id_);
      auto comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
          person_label_id_, root, comment_label_id_, hasCreator_label_id_);
      std::vector<vid_t> comment_vids;
      for(;comment_ie.is_valid();comment_ie.next()){
        comment_vids.push_back(comment_ie.get_neighbor());
      }
      auto comment_person_likes_in_items=txn.BatchGetEdgePropsFromSrcVids<Date>(comment_label_id_,person_label_id_,  likes_label_id_, comment_vids, false);
      auto comment_oids=txn.BatchGetVertexIds(comment_label_id_, comment_vids);
      for (int i=0;i<comment_vids.size();i++){
        auto likes_persons = comment_person_likes_in_items[i];
        auto cid = comment_vids[i];
        auto message_id = comment_oids[i];
        auto person_ie = person_likes_comment_in.get_edges(cid);
        for (int j=0;j<likes_persons.size();j++){
          auto item = likes_persons[j];
          vec.emplace_back(item.first, cid, item.second.milli_second,
                           txn.GetVertexId(person_label_id_, item.first),
                           message_id, false);
        }
      }

      sort(vec.begin(), vec.end(),
           [&](const person_info &a, const person_info &b)
           {
             if (a.person_id != b.person_id)
             {
               return a.person_id < b.person_id;
             }
             if (a.creationDate != b.creationDate)
             {
               return a.creationDate > b.creationDate;
             }
             return a.message_id < b.message_id;
           });
      person_info_comparer comparer;
      std::priority_queue<person_info, std::vector<person_info>,
                          person_info_comparer>
          pq(comparer);

      for (size_t i = 0; i < vec.size(); ++i)
      {
        if (i && vec[i].person_id == vec[i - 1].person_id)
          continue;

        if (pq.size() < 20)
        {
          pq.emplace(vec[i]);
        }
        else if (comparer(vec[i], pq.top()))
        {
          pq.pop();
          pq.emplace(vec[i]);
        }
      }

      std::vector<person_info> tmp;
      tmp.reserve(pq.size());
      std::vector<vid_t> person_vids;
      std::vector<vid_t> res_comment_vids;
      std::vector<vid_t> res_post_vids;
      std::vector<vid_t> res_post_imageFile_vids;
      std::vector<vid_t> res_post_content_vids;
      int comment_index=-1;
      int post_index=-1;
      int post_imageFile_index=-1;
      int post_content_index=-1;
      while (!pq.empty())
      {
        tmp.emplace_back(pq.top());
        person_vids.push_back(pq.top().v);
        if(pq.top().is_post){
          res_post_vids.push_back(pq.top().mid);
          post_index++;
        }else{
          res_comment_vids.push_back(pq.top().mid);
          comment_index++;
        }
        pq.pop();
      }
      auto person_props=txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, {person_firstName_col_, person_lastName_col_});
      auto post_lengths=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_vids, {post_length_col_});
      for(int i=0;i<res_post_vids.size();i++){
        auto length=gbp::BufferBlock::RefSingle<int>(post_lengths[0][i]);
        if(length==0){
          res_post_imageFile_vids.push_back(res_post_vids[i]);
          post_imageFile_index++;
        }else{
          res_post_content_vids.push_back(res_post_vids[i]);
          post_content_index++;
        }
      }
      auto comment_content=txn.BatchGetVertexPropsFromVids(comment_label_id_, res_comment_vids, {comment_content_col_});
      auto post_content=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_content_vids, {post_content_col_});
      auto post_imageFile=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_imageFile_vids, {post_imageFile_col_});
      auto post_creationDate=txn.BatchGetVertexPropsFromVids(post_label_id_, res_post_vids, {post_creationDate_col_});
      auto comment_creationDate=txn.BatchGetVertexPropsFromVids(comment_label_id_, res_comment_vids, {comment_creationDate_col_});
      constexpr int64_t mill_per_min = 60 * 1000l;
      for (auto i = tmp.size(); i > 0; --i)
      {
        const auto &v = tmp[i - 1];
        output.put_long(v.person_id);
        auto firstname = person_props[0][i-1];
        output.put_buffer_object(firstname);
        auto lastname = person_props[1][i-1];
        output.put_buffer_object(lastname);
        output.put_long(v.creationDate);
        output.put_long(v.message_id);
        // uint32_t x;
        if (v.is_post)
        {
          auto length=gbp::BufferBlock::RefSingle<int>(post_lengths[0][post_index]);
          auto content = length == 0 ? post_imageFile[0][post_imageFile_index] : post_content[0][post_content_index];
          if(length==0){
            post_imageFile_index--;
          }else{
            post_content_index--;
          }

          output.put_buffer_object(content);
          auto item = post_creationDate[0][post_index];
          auto min = (v.creationDate -
                      gbp::BufferBlock::Ref<Date>(item).milli_second) /
                     mill_per_min;
          output.put_int(min);
          post_index--;
        }
        else
        {
          gbp::BufferBlock content = comment_content[0][comment_index];
          output.put_buffer_object(content);
          auto item = comment_creationDate[0][comment_index];
          auto min = (v.creationDate -
                      gbp::BufferBlock::Ref<Date>(item).milli_second) /
                     mill_per_min;
          output.put_int(min);
          comment_index--;
        }
        output.put_byte(friends_[v.v] ? 0 : 1);
      }
      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t knows_label_id_;
    label_t comment_label_id_;
    label_t likes_label_id_;
    label_t hasCreator_label_id_;

    cgraph::PropertyHandle person_firstName_col_;
    cgraph::PropertyHandle person_lastName_col_;
    cgraph::PropertyHandle post_content_col_;
    cgraph::PropertyHandle post_imageFile_col_;
    cgraph::PropertyHandle post_length_col_;
    cgraph::PropertyHandle comment_content_col_;
    cgraph::PropertyHandle post_creationDate_col_;
    cgraph::PropertyHandle comment_creationDate_col_;

    std::vector<oid_t> persons_;
    std::vector<oid_t> messages_;
    std::vector<bool> friends_;

    GraphDBSession &graph_;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC7 *app = new gs::IC7(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC7 *casted = static_cast<gs::IC7 *>(app);
    delete casted;
  }
}

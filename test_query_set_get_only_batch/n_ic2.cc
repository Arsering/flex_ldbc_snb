#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC2 : public AppBase
  {
  public:
    IC2(GraphDBSession &graph)
        : post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),
          post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
          post_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
              graph.get_vertex_property_column(post_label_id_, "length")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          graph_(graph) {
          }
    ~IC2() {}

    struct message_info
    {
      message_info(vid_t message_vid_, vid_t person_vid_, oid_t message_id_,
                   int64_t creationdate_, bool is_post_)
          : message_vid(message_vid_),
            person_vid(person_vid_),
            message_id(message_id_),
            creationdate(creationdate_),
            is_post(is_post_) {}

      vid_t message_vid;
      vid_t person_vid;
      oid_t message_id;
      int64_t creationdate;
      bool is_post;
    };

    struct message_info_comparer
    {
      bool operator()(const message_info &lhs, const message_info &rhs)
      {
        if (lhs.creationdate > rhs.creationdate)
        {
          return true;
        }
        if (lhs.creationdate < rhs.creationdate)
        {
          return false;
        }
        return lhs.message_id < rhs.message_id;
      }
    };

    bool Query(Decoder &input, Encoder &output) override
    {
      auto txn = graph_.GetReadTransaction();

      oid_t personid = input.get_long();
      int64_t maxdate = input.get_long();
      CHECK(input.empty());

      vid_t root{};
      if (!txn.GetVertexIndex(person_label_id_, personid, root))
      {
        return false;
      }
      message_info_comparer comparer;
      std::priority_queue<message_info, std::vector<message_info>,
                          message_info_comparer>
          pq(comparer);
      {
        int64_t current_date = 0;
        std::vector<vid_t> friend_ids_all;
        std::vector<vid_t> friend_ids;
        {
          auto person_knows_person_in = txn.GetIncomingEdges<Date>(
              person_label_id_, root, person_label_id_, knows_label_id_);
          for (; person_knows_person_in.is_valid(); person_knows_person_in.next())
          {
            friend_ids_all.push_back(person_knows_person_in.get_neighbor());
          }
          auto person_knows_person_out = txn.GetOutgoingEdges<Date>(
              person_label_id_, root, person_label_id_, knows_label_id_);
          for (; person_knows_person_out.is_valid(); person_knows_person_out.next())
          {
            friend_ids_all.push_back(person_knows_person_out.get_neighbor());
          }
        }
        // size_t batch_size = 300;
        // batch_size = batch_size * BATCH_SIZE_RATIO;
        size_t batch_size = BATCH_SIZE;
        friend_ids.reserve(batch_size);
        for (size_t i = 0; i < friend_ids_all.size(); i += batch_size)
        {
          friend_ids.clear();
          if (i + batch_size > friend_ids_all.size())
            friend_ids.assign(friend_ids_all.begin() + i, friend_ids_all.end());
          else
            friend_ids.assign(friend_ids_all.begin() + i, friend_ids_all.begin() + i + batch_size);

          auto post_hasCreator_person_in_items = txn.BatchGetIncomingEdges<grape::EmptyType>(person_label_id_, post_label_id_, hasCreator_label_id_, friend_ids);
          std::vector<vid_t> post_vids;
          std::vector<vid_t> post_creator_ids;
          for (size_t i = 0; i < post_hasCreator_person_in_items.size(); i++)
          {          
            // gbp::get_thread_logfile() << friend_ids[i] << std::endl;

            for (; post_hasCreator_person_in_items[i].is_valid(); post_hasCreator_person_in_items[i].next())
            {
              post_vids.push_back(post_hasCreator_person_in_items[i].get_neighbor());
              post_creator_ids.push_back(friend_ids[i]);
            }
          }

          post_hasCreator_person_in_items.clear();
          auto post_creationDate_col_items = txn.BatchGetVertexPropsFromVids(post_label_id_, post_vids, {"creationDate"});

          for (size_t post_idx = 0; post_idx < post_creationDate_col_items[0].size(); post_idx++)
          {
            auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(post_creationDate_col_items[0][post_idx]).milli_second;
            post_creationDate_col_items[0][post_idx].free();
            if (creationDate < maxdate)
            {
              if (pq.size() < 20)
              {
                pq.emplace(post_vids[post_idx], post_creator_ids[post_idx], txn.GetVertexId(post_label_id_, post_vids[post_idx]),
                           creationDate, true);
                current_date = pq.top().creationdate;
              }
              else if (creationDate > current_date)
              {
                auto message_id = txn.GetVertexId(post_label_id_, post_vids[post_idx]);
                pq.pop();
                pq.emplace(post_vids[post_idx], post_creator_ids[post_idx], message_id, creationDate, true);
                current_date = pq.top().creationdate;
              }
              else if (creationDate == current_date)
              {
                auto message_id = txn.GetVertexId(post_label_id_, post_vids[post_idx]);
                if (message_id < pq.top().message_id)
                {
                  pq.pop();
                  pq.emplace(post_vids[post_idx], post_creator_ids[post_idx], message_id, creationDate, true);
                }
              }
            }
          }
          post_hasCreator_person_in_items.clear();
          post_creationDate_col_items.clear();

          auto comment_hasCreator_person_in_items = txn.BatchGetIncomingEdges<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, friend_ids);
          std::vector<vid_t> comment_vids;

          std::vector<vid_t> comment_creator_vids;
          for (size_t i = 0; i < comment_hasCreator_person_in_items.size(); i++)
          {
            for (; comment_hasCreator_person_in_items[i].is_valid(); comment_hasCreator_person_in_items[i].next())
            {
              comment_vids.push_back(comment_hasCreator_person_in_items[i].get_neighbor());
              comment_creator_vids.push_back(friend_ids[i]);
            }
          }
          comment_hasCreator_person_in_items.clear();
          auto comment_creationDate_col_items = txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {"creationDate"});

          for (size_t comment_idx = 0; comment_idx < comment_creationDate_col_items[0].size(); comment_idx++)
          {
            auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(comment_creationDate_col_items[0][comment_idx]).milli_second;
            comment_creationDate_col_items[0][comment_idx].free();
            if (creationDate < maxdate)
            {
              if (pq.size() < 20)
              {
                pq.emplace(comment_vids[comment_idx], comment_creator_vids[comment_idx], txn.GetVertexId(comment_label_id_, comment_vids[comment_idx]),
                           creationDate, false);
                current_date = pq.top().creationdate;
              }
              else if (creationDate > current_date)
              {
                auto message_id = txn.GetVertexId(comment_label_id_, comment_vids[comment_idx]);
                pq.pop();
                pq.emplace(comment_vids[comment_idx], comment_creator_vids[comment_idx], message_id, creationDate, false);
                current_date = pq.top().creationdate;
              }
              else if (creationDate == current_date)
              {
                auto message_id = txn.GetVertexId(comment_label_id_, comment_vids[comment_idx]);
                if (message_id < pq.top().message_id)
                {
                  pq.pop();
                  pq.emplace(comment_vids[comment_idx], comment_creator_vids[comment_idx], message_id, creationDate, false);
                }
              }
            }
          }
        }
      }

      std::vector<message_info> vec;
      vec.reserve(pq.size());
      while (!pq.empty())
      {
        vec.emplace_back(pq.top());
        pq.pop();
      }
      std::reverse(vec.begin(), vec.end());

      std::vector<vid_t> person_vids;
      std::vector<vid_t> post_vids;
      std::vector<vid_t> comment_vids;
      for (size_t i = 0; i < vec.size(); i++)
      {
        person_vids.push_back(vec[i].person_vid);
        if (vec[i].is_post)
        {
          post_vids.push_back(vec[i].message_vid);
        }
        else
        {
          comment_vids.push_back(vec[i].message_vid);
        }
      }

      auto person_oids = txn.BatchGetVertexIds(person_label_id_, person_vids);
      auto person_names = txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, {"firstName", "lastName"});
      auto comment_content_items = txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {"content"});
      auto post_length_items = txn.BatchGetVertexPropsFromVids(post_label_id_, post_vids, {"length"});
      std::vector<vid_t> post_image_vids;
      std::vector<vid_t> post_content_vids;
      for (size_t post_idx = 0; post_idx < post_length_items[0].size(); post_idx++)
      {
        auto length = gbp::BufferBlock::RefSingle<int>(post_length_items[0][post_idx]);

        if (length == 0)
        {
          post_image_vids.push_back(post_vids[post_idx]);
        }
        else
        {
          post_content_vids.push_back(post_vids[post_idx]);
        }
      }

      auto post_image_items = txn.BatchGetVertexPropsFromVids(post_label_id_, post_image_vids, {"imageFile"});
      auto post_content_items = txn.BatchGetVertexPropsFromVids(post_label_id_, post_content_vids, {"content"});

      uint32_t comment_count = 0;
      uint32_t post_image_count = 0;
      uint32_t post_content_count = 0;
      for (size_t i = 0; i < vec.size(); i++)
      {
        auto &v = vec[i];
        output.put_long(person_oids[i]);
        output.put_buffer_object(person_names[0][i]);
        person_names[0][i].free();
        output.put_buffer_object(person_names[1][i]);
        person_names[1][i].free();

        output.put_long(v.message_id);
        if (!v.is_post)
        {
          output.put_buffer_object(comment_content_items[0][comment_count++]);
          comment_content_items[0][comment_count - 1].free();
        }
        else
        {
          auto &item = gbp::BufferBlock::RefSingle<int>(post_length_items[0][post_image_count + post_content_count]) == 0 ? post_image_items[0][post_image_count++] : post_content_items[0][post_content_count++];
          output.put_buffer_object(item);
          item.free();
          post_length_items[0][post_image_count + post_content_count - 1].free();
        }
        output.put_long(v.creationdate);
      }

      return true;
    }

  private:
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t person_label_id_;

    label_t knows_label_id_;
    label_t hasCreator_label_id_;

    const DateColumn &post_creationDate_col_;
    const DateColumn &comment_creationDate_col_;
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &post_content_col_;
    const StringColumn &post_imageFile_col_;
    const IntColumn &post_length_col_;
    const StringColumn &comment_content_col_;

    GraphDBSession &graph_;
    // float BATCH_SIZE_RATIO = 1.0;
  };

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC2 *app = new gs::IC2(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC2 *casted = static_cast<gs::IC2 *>(app);
    delete casted;
  }
}

#include <queue>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "n_utils.h"

namespace gs
{

  class IC9 : public AppBase
  {
  public:
    IC9(GraphDBSession &graph)
        : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
          post_label_id_(graph.schema().get_vertex_label_id("POST")),
          comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
          hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),
          knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),
          post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
          post_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
              graph.get_vertex_property_column(post_label_id_, "length")))),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
          person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "lastName")))),
          comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(comment_label_id_, "content")))),
          comment_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(comment_label_id_,
                                               "creationDate")))),
          post_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
              graph.get_vertex_property_column(post_label_id_, "creationDate")))),
          graph_(graph) {}
    ~IC9() {}

    struct message_info
    {
      message_info(vid_t message_vid_, vid_t other_person_vid_, oid_t message_id_,
                   int64_t creation_date_, bool is_comment_)
          : message_vid(message_vid_),
            other_person_vid(other_person_vid_),
            message_id(message_id_),
            creation_date(creation_date_),
            is_comment(is_comment_) {}

      vid_t message_vid;
      vid_t other_person_vid;
      oid_t message_id;
      int64_t creation_date;
      bool is_comment;
    };

    struct message_info_comparer
    {
      bool operator()(const message_info &lhs, const message_info &rhs)
      {
        if (lhs.creation_date > rhs.creation_date)
        {
          return true;
        }
        if (lhs.creation_date < rhs.creation_date)
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

      message_info_comparer cmp;
      std::priority_queue<message_info, std::vector<message_info>,
                          message_info_comparer>
          que(cmp);

      auto post_hasCreator_person_in = txn.GetIncomingGraphView<grape::EmptyType>(
          person_label_id_, post_label_id_, hasCreator_label_id_);

      auto comment_hasCreator_person_in =
          txn.GetIncomingGraphView<grape::EmptyType>(
              person_label_id_, comment_label_id_, hasCreator_label_id_);
      {
        std::vector<vid_t> person_vids;
        get_1d_2d_neighbors(txn, person_label_id_, knows_label_id_, root, txn.GetVertexNum(person_label_id_), person_vids);

        size_t batch_size = 300;
        std::vector<vid_t> person_vids_tmp;
        person_vids_tmp.reserve(batch_size);
        for (size_t i = 0; i < person_vids.size(); i += batch_size)
        {
          person_vids_tmp.clear();
          if (i + batch_size > person_vids.size())
            person_vids_tmp.assign(person_vids.begin() + i, person_vids.end());
          else
            person_vids_tmp.assign(person_vids.begin() + i, person_vids.begin() + i + batch_size);

          std::vector<vid_t> post_vids;
          std::vector<vid_t> post_creator_ids;
          auto post_hasCreator_person_in = txn.BatchGetIncomingEdges<grape::EmptyType>(person_label_id_, post_label_id_, hasCreator_label_id_, person_vids_tmp);

          for (auto person_idx = 0; person_idx < person_vids_tmp.size(); person_idx++)
          {
            for (; post_hasCreator_person_in[person_idx].is_valid(); post_hasCreator_person_in[person_idx].next())
            {
              post_vids.push_back(post_hasCreator_person_in[person_idx].get_neighbor());
              post_creator_ids.push_back(person_vids_tmp[person_idx]);
            }
          }

          post_hasCreator_person_in.clear();
          auto post_creationDate_col_items = txn.BatchGetVertexPropsFromVids(post_label_id_, post_vids, {"creationDate"});

          for (size_t post_idx = 0; post_idx < post_creationDate_col_items[0].size(); post_idx++)
          {
            auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(post_creationDate_col_items[0][post_idx]).milli_second;
            post_creationDate_col_items[0][post_idx].free();
            if (creationDate < maxdate)
            {
              if (que.size() < 20)
              {
                que.emplace(post_vids[post_idx], post_creator_ids[post_idx], txn.GetVertexId(post_label_id_, post_vids[post_idx]),
                            creationDate, false);
              }
              else if (creationDate > que.top().creation_date)
              {
                auto message_id = txn.GetVertexId(post_label_id_, post_vids[post_idx]);
                que.pop();
                que.emplace(post_vids[post_idx], post_creator_ids[post_idx], message_id, creationDate, false);
              }
              else if (creationDate == que.top().creation_date)
              {
                auto message_id = txn.GetVertexId(post_label_id_, post_vids[post_idx]);
                if (message_id < que.top().message_id)
                {
                  que.pop();
                  que.emplace(post_vids[post_idx], post_creator_ids[post_idx], message_id, creationDate, false);
                }
              }
            }
          }
        }

        batch_size = 200;
        for (size_t i = 0; i < person_vids.size(); i += batch_size)
        {
          person_vids_tmp.clear();
          if (i + batch_size > person_vids.size())
            person_vids_tmp.assign(person_vids.begin() + i, person_vids.end());
          else
            person_vids_tmp.assign(person_vids.begin() + i, person_vids.begin() + i + batch_size);

          std::vector<vid_t> comment_vids;
          std::vector<vid_t> comment_creator_ids;

          auto comment_hasCreator_person_in = txn.BatchGetIncomingEdges<grape::EmptyType>(person_label_id_, comment_label_id_, hasCreator_label_id_, person_vids_tmp);

          for (auto person_idx = 0; person_idx < person_vids_tmp.size(); person_idx++)
          {
            for (; comment_hasCreator_person_in[person_idx].is_valid(); comment_hasCreator_person_in[person_idx].next())
            {
              comment_vids.push_back(comment_hasCreator_person_in[person_idx].get_neighbor());
              comment_creator_ids.push_back(person_vids_tmp[person_idx]);
            }
          }
          comment_hasCreator_person_in.clear();

          auto comment_creationDate_col_items = txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {"creationDate"});

          for (size_t comment_idx = 0; comment_idx < comment_creationDate_col_items[0].size(); comment_idx++)
          {
            auto creationDate = gbp::BufferBlock::RefSingle<gs::Date>(comment_creationDate_col_items[0][comment_idx]).milli_second;
            comment_creationDate_col_items[0][comment_idx].free();
            if (creationDate < maxdate)
            {
              if (que.size() < 20)
              {
                que.emplace(comment_vids[comment_idx], comment_creator_ids[comment_idx], txn.GetVertexId(comment_label_id_, comment_vids[comment_idx]),
                            creationDate, true);
              }
              else if (creationDate > que.top().creation_date)
              {
                auto message_id = txn.GetVertexId(comment_label_id_, comment_vids[comment_idx]);
                que.pop();
                que.emplace(comment_vids[comment_idx], comment_creator_ids[comment_idx], message_id, creationDate, true);
              }
              else if (creationDate == que.top().creation_date)
              {
                auto message_id = txn.GetVertexId(comment_label_id_, comment_vids[comment_idx]);
                if (message_id < que.top().message_id)
                {
                  que.pop();
                  que.emplace(comment_vids[comment_idx], comment_creator_ids[comment_idx], message_id, creationDate, true);
                }
              }
            }
          }
        }
      }

      std::vector<message_info> vec;
      vec.reserve(que.size());
      while (!que.empty())
      {
        vec.emplace_back(que.top());
        que.pop();
      }

      std::reverse(vec.begin(), vec.end());

      std::vector<vid_t> person_vids;
      std::vector<vid_t> post_vids;
      std::vector<vid_t> comment_vids;

      for (size_t i = 0; i < vec.size(); i++)
      {
        person_vids.push_back(vec[i].other_person_vid);
        if (vec[i].is_comment)
        {
          comment_vids.push_back(vec[i].message_vid);
        }
        else
        {
          post_vids.push_back(vec[i].message_vid);
        }
      }

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

      auto person_oids = txn.BatchGetVertexIds(person_label_id_, person_vids);
      auto person_names = txn.BatchGetVertexPropsFromVids(person_label_id_, person_vids, {"firstName", "lastName"});
      auto comment_content_items = txn.BatchGetVertexPropsFromVids(comment_label_id_, comment_vids, {"content"});

      uint32_t comment_count = 0;
      uint32_t post_image_count = 0;
      uint32_t post_content_count = 0;
      for (size_t idx = 0; idx < vec.size(); idx++)
      {
        auto &v = vec[idx];
        output.put_long(person_oids[idx]);
        output.put_buffer_object(person_names[0][idx]);
        person_names[0][idx].free();
        output.put_buffer_object(person_names[1][idx]);
        person_names[1][idx].free();
        output.put_long(v.message_id);
        if (v.is_comment)
        {
          output.put_buffer_object(comment_content_items[0][comment_count]);
          comment_content_items[0][comment_count].free();
          comment_count++;
        }
        else
        {
          auto &item = gbp::BufferBlock::RefSingle<int>(post_length_items[0][post_image_count + post_content_count]) == 0 ? post_image_items[0][post_image_count++] : post_content_items[0][post_content_count++];
          output.put_buffer_object(item);
          item.free();
        }
        output.put_long(v.creation_date);
      }

      return true;
    }

  private:
    label_t person_label_id_;
    label_t post_label_id_;
    label_t comment_label_id_;
    label_t hasCreator_label_id_;
    label_t knows_label_id_;

    const StringColumn &post_content_col_;
    const StringColumn &post_imageFile_col_;
    const IntColumn &post_length_col_;
    const StringColumn &person_firstName_col_;
    const StringColumn &person_lastName_col_;
    const StringColumn &comment_content_col_;
    const DateColumn &comment_creationDate_col_;
    const DateColumn &post_creationDate_col_;

    GraphDBSession &graph_;
  };
} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::IC9 *app = new gs::IC9(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::IC9 *casted = static_cast<gs::IC9 *>(app);
    delete casted;
  }
}

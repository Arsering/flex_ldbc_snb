#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

class IC1 : public AppBase {
public:
  IC1(GraphDBSession &graph)
      : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
        knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
        isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
        organisation_label_id_(
            graph.schema().get_vertex_label_id("ORGANISATION")),
        workAt_label_id_(graph.schema().get_edge_label_id("WORKAT")),
        studyAt_label_id_(graph.schema().get_edge_label_id("STUDYAT")),
        person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "firstName")))),
        person_lastName_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "lastName")))),
        person_birthday_col_(*(std::dynamic_pointer_cast<DateColumn>(
            graph.get_vertex_property_column(person_label_id_, "birthday")))),
        person_creationDate_col_(*(std::dynamic_pointer_cast<DateColumn>(
            graph.get_vertex_property_column(person_label_id_,
                                             "creationDate")))),
        person_gender_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "gender")))),
        person_browserUsed_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_,
                                             "browserUsed")))),
        person_locationIp_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "locationIP")))),
        place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(place_label_id_, "name")))),
        person_email_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "email")))),
        person_language_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(person_label_id_, "language")))),
        organisation_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(organisation_label_id_, "name")))),
        graph_(graph) {}

  struct person_info {
    person_info(uint8_t distance_, gbp::BufferBlock lastName_, oid_t id_,
                vid_t vid_)
        : distance(distance_), lastName(lastName_), id(id_), vid(vid_) {}

    uint8_t distance;
    gbp::BufferBlock lastName;
    oid_t id;
    vid_t vid;
  };

  // 定义优先队列的比较器
  struct person_info_comparer {
    bool operator()(const person_info &lhs, const person_info &rhs) {
      if (lhs.distance != rhs.distance) {
        return lhs.distance < rhs.distance; // 距离小的优先
      }
      if (!(lhs.lastName == rhs.lastName)) {
        return lhs.lastName < rhs.lastName; // 姓氏小的���先
      }
      return lhs.id < rhs.id; // ID小的优先
    }
  };

//   void get_friends(const ReadTransaction &txn, vid_t root,
//                    const std::string_view &firstname) {
//     person_info_comparer comparer;
//     std::priority_queue<person_info, std::vector<person_info>,
//                         person_info_comparer>
//         pq(comparer);
//     uint8_t dep = 1;
//     distance_[root] = 1;
//     std::queue<vid_t> q;
//     q.push(root);
//     while (!q.empty()) {
//       auto u = q.front();
//       q.pop();
//       if (dep != distance_[u]) {
//         if (pq.size() == 20)
//           break;
//         dep = distance_[u];
//       }
//       auto in_friends=txn.GetOtherVertices(person_label_id_, person_label_id_, knows_label_id_, {u}, "in");
//       auto out_friends=txn.GetOtherVertices(person_label_id_, person_label_id_, knows_label_id_, {u}, "out");
//       std::vector<vid_t> total_friends;
//       total_friends.insert(total_friends.end(), in_friends.begin(), in_friends.end());
//       total_friends.insert(total_friends.end(), out_friends.begin(), out_friends.end());
//       auto first_names=txn.BatchGetVertexPropsFromVid(person_label_id_, total_friends, std::vector<std::string>{"firstName"});
//     //   for (auto &friend_vid:in_friends) {
//     //     if (distance_[friend_vid])
//     //       continue;
//     //     distance_[friend_vid]=distance_[u]+1;
//     //     if (distance_[friend_vid]<4) {
//     //       q.push(friend_vid);
//     //    }
       
//     //   }
//     }
//   }

  bool Query(Decoder &input, Encoder &output) override {
    LOG(ERROR) << "Not support IC1 batch";
    return false;
    
    auto txn = graph_.GetReadTransaction();

    oid_t person_id = input.get_long();
    std::string_view firstname = input.get_string();

    CHECK(input.empty());
    int max_hop = 3;
    vid_t root{};
    if (!txn.GetVertexIndex(person_label_id_, person_id, root)) {
      return false;
    }

    // 使用优先队列存储前20个结果
    person_info_comparer comparer;
    std::priority_queue<person_info, std::vector<person_info>,
                        person_info_comparer>
        pq(comparer);
    uint8_t dep = 1;

    // 使用BFS收集所有在max_hop距离内的人
    std::vector<uint8_t> distance;
    distance.resize(txn.GetVertexNum(person_label_id_));
    std::fill(distance.begin(), distance.end(), max_hop + 1);
    distance[root] = 0;

    std::vector<vid_t> current_level{root};
    std::vector<vid_t> next_level;

    for (int hop = 1; hop <= max_hop && !current_level.empty(); hop++) {
      // 打印当前层的节点
      std::ofstream ofs("batch_level_nodes.txt", std::ios::app);
      ofs << "Depth " << hop << ": ";
      for (const auto &vid : current_level) {
        ofs << vid << " ";
      }
      ofs << "\n";
      ofs.close();

      // 批量获取当前层所有人的朋友
      auto out_friends =
          txn.GetOtherVertices(person_label_id_, person_label_id_,
                               knows_label_id_, current_level, "out");

      auto in_friends =
          txn.GetOtherVertices(person_label_id_, person_label_id_,
                               knows_label_id_, current_level, "in");

      // 获取当前层所有人的firstName
      auto first_names =
          txn.BatchGetVertexPropsFromVid(person_label_id_, current_level,
                                         std::vector<std::string>{"firstName"});

      // 处理所有朋友
      for (size_t i = 0; i < current_level.size(); i++) {
        if (dep != distance[current_level[i]]) {
          if (pq.size() == 20)
            break;
          dep = distance[current_level[i]];
        }

        // 处理出边朋友
        for (const auto &friend_vid : out_friends[i]) {
          if (distance[friend_vid] > hop) {
            distance[friend_vid] = hop;
            next_level.push_back(friend_vid);

            auto firstName = first_names[i];
            if (firstName == firstname) {
              auto lastName = person_lastName_col_.get(friend_vid);
              if (pq.size() < 20) {
                pq.emplace(hop, lastName,
                           txn.GetVertexId(person_label_id_, friend_vid),
                           friend_vid);
              } else {
                const auto &top = pq.top();
                if (hop < top.distance) {
                  pq.pop();
                  pq.emplace(hop, lastName,
                             txn.GetVertexId(person_label_id_, friend_vid),
                             friend_vid);
                } else if (hop == top.distance) {
                  if (lastName < top.lastName) {
                    pq.pop();
                    pq.emplace(hop, lastName,
                               txn.GetVertexId(person_label_id_, friend_vid),
                               friend_vid);
                  } else if (lastName == top.lastName) {
                    oid_t id = txn.GetVertexId(person_label_id_, friend_vid);
                    if (id < top.id) {
                      pq.pop();
                      pq.emplace(hop, lastName, id, friend_vid);
                    }
                  }
                }
              }
            }
          }
        }

        // 处理入边朋友
        for (const auto &friend_vid : in_friends[i]) {
          if (distance[friend_vid] > hop) {
            distance[friend_vid] = hop;
            next_level.push_back(friend_vid);

            auto firstName = first_names[i];
            if (firstName == firstname) {
              auto lastName = person_lastName_col_.get(friend_vid);
              if (pq.size() < 20) {
                pq.emplace(hop, lastName,
                           txn.GetVertexId(person_label_id_, friend_vid),
                           friend_vid);
              } else {
                const auto &top = pq.top();
                if (hop < top.distance) {
                  pq.pop();
                  pq.emplace(hop, lastName,
                             txn.GetVertexId(person_label_id_, friend_vid),
                             friend_vid);
                } else if (hop == top.distance) {
                  if (lastName < top.lastName) {
                    pq.pop();
                    pq.emplace(hop, lastName,
                               txn.GetVertexId(person_label_id_, friend_vid),
                               friend_vid);
                  } else if (lastName == top.lastName) {
                    oid_t id = txn.GetVertexId(person_label_id_, friend_vid);
                    if (id < top.id) {
                      pq.pop();
                      pq.emplace(hop, lastName, id, friend_vid);
                    }
                  }
                }
              }
            }
          }
        }
      }

      if (pq.size() == 20) {
        break;
      }

      std::swap(current_level, next_level);
      next_level.clear();
    }

    std::vector<person_info> all_persons_info;
    std::vector<vid_t> all_persons;
    all_persons_info.reserve(pq.size());
    while (!pq.empty()) {
      all_persons_info.push_back(pq.top());
      all_persons.push_back(pq.top().vid);
      pq.pop();
    }
    std::ofstream ofs("batch_ans.txt", std::ios::app);
    for (auto &info : all_persons_info) {
      ofs << info.vid << " ";
    }
    ofs << "\n";
    ofs.close();

    // 批量获取所有人的属性
    auto person_props = txn.BatchGetVertexPropsFromVid(
        person_label_id_, all_persons,
        std::vector<std::string>{"firstName", "lastName", "birthday",
                                 "creationDate", "gender", "browserUsed",
                                 "locationIP", "email", "language"});

    // 批量获取所有人的位置
    auto locations =
        txn.GetOtherVertices(person_label_id_, place_label_id_,
                             isLocatedIn_label_id_, all_persons, "out");

    // 批量获取所有地点的名称
    std::vector<vid_t> place_vids;
    for (const auto &loc : locations) {
      CHECK(loc.size() == 1) << "Person should have exactly one location";
      place_vids.push_back(loc[0]);
    }

    auto place_props = txn.BatchGetVertexPropsFromVid(
        place_label_id_, place_vids, std::vector<std::string>{"name"});

    // 批量获取所有人的工作和学习经历
    auto work_orgs =
        txn.GetOtherVertices(person_label_id_, organisation_label_id_,
                             workAt_label_id_, all_persons, "out");

    auto study_orgs =
        txn.GetOtherVertices(person_label_id_, organisation_label_id_,
                             studyAt_label_id_, all_persons, "out");

    // 收集所有组织
    std::vector<vid_t> org_vids;
    for (const auto &orgs : work_orgs) {
      org_vids.insert(org_vids.end(), orgs.begin(), orgs.end());
    }
    for (const auto &orgs : study_orgs) {
      org_vids.insert(org_vids.end(), orgs.begin(), orgs.end());
    }

    // 批量获取组织名称
    auto org_props = txn.BatchGetVertexPropsFromVid(
        organisation_label_id_, org_vids, std::vector<std::string>{"name"});

    // 输出结果
    for (size_t i = 0; i < all_persons_info.size(); i++) {
      const auto &person_vid = all_persons[i];
      output.put_long(txn.GetVertexId(person_label_id_, person_vid));
      output.put_int(distance[person_vid]);

      // 输出个人属性
      const size_t props_offset = i * 9;
      output.put_buffer_object(person_props[props_offset]);     // firstName
      output.put_buffer_object(person_props[props_offset + 1]); // lastName
      output.put_long(
          gbp::BufferBlock::Ref<Date>(person_props[props_offset + 2])
              .milli_second); // birthday
      output.put_long(
          gbp::BufferBlock::Ref<Date>(person_props[props_offset + 3])
              .milli_second);                                   // creationDate
      output.put_buffer_object(person_props[props_offset + 4]); // gender
      output.put_buffer_object(person_props[props_offset + 5]); // browserUsed
      output.put_buffer_object(person_props[props_offset + 6]); // locationIP
      output.put_buffer_object(person_props[props_offset + 7]); // email
      output.put_buffer_object(person_props[props_offset + 8]); // language

      // 输出位置
      output.put_buffer_object(place_props[i]); // place name

      // 输出工作经历
      output.put_int(work_orgs[i].size());
      for (const auto &org_vid : work_orgs[i]) {
        output.put_long(txn.GetVertexId(organisation_label_id_, org_vid));
      }

      // 输出学习经历
      output.put_int(study_orgs[i].size());
      for (const auto &org_vid : study_orgs[i]) {
        output.put_long(txn.GetVertexId(organisation_label_id_, org_vid));
      }
    }

    return true;
  }

private:
  label_t person_label_id_;
  label_t place_label_id_;
  label_t knows_label_id_;
  label_t isLocatedIn_label_id_;
  label_t organisation_label_id_;
  label_t workAt_label_id_;
  label_t studyAt_label_id_;
  std::vector<person_info> ans_;
  std::vector<uint8_t> distance_;


  const StringColumn &person_firstName_col_;
  const StringColumn &person_lastName_col_;
  const DateColumn &person_birthday_col_;
  const DateColumn &person_creationDate_col_;
  const StringColumn &person_gender_col_;
  const StringColumn &person_browserUsed_col_;
  const StringColumn &person_locationIp_col_;
  const StringColumn &place_name_col_;
  const StringColumn &person_email_col_;
  const StringColumn &person_language_col_;
  const StringColumn &organisation_name_col_;

  GraphDBSession &graph_;
};

} // namespace gs

extern "C" {
void *CreateApp(gs::GraphDBSession &db) {
  gs::IC1 *app = new gs::IC1(db);
  return static_cast<void *>(app);
}

void DeleteApp(void *app) {
  gs::IC1 *casted = static_cast<gs::IC1 *>(app);
  delete casted;
}
}
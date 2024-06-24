#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include <boost/parameter/aux_/tagged_argument.hpp>
#include <flex/storages/rt_mutable_graph/types.h>
#include <flex/utils/property/column.h>
#include <grape/types.h>
// #include "utils.h"

namespace gs
{

class PERSON_EDGE_IS : public AppBase {
public:
  PERSON_EDGE_IS(GraphDBSession &graph)
      : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        //isLocatedIn
        isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
        organisation_label_id_(graph.schema().get_vertex_label_id("ORGANISATION")),
        place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
        place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(graph.get_vertex_property_column(place_label_id_, "name")))),
        //knows
        knows_label_id_(graph.schema().get_edge_label_id("KNOWS")),
          person_firstName_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(person_label_id_, "firstName")))),
        //hasInterestIn
        hasInterest_label_id_(graph.schema().get_edge_label_id("HASINTEREST")),
        tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
        tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tag_label_id_, "name")))),
        //workAt
        workAt_label_id_(graph.schema().get_edge_label_id("WORKAT")),
        organisation_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(organisation_label_id_, "name")))),
        studyAt_label_id_(graph.schema().get_edge_label_id("STUDYAT")),
        //likes
        likes_label_id_(graph.schema().get_edge_label_id("LIKES")),
        comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
        comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(comment_label_id_, "content")))),
        post_label_id_(graph.schema().get_vertex_label_id("POST")),
        post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
        post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),
        //hasCreator
        hasCreator_label_id_(graph.schema().get_edge_label_id("HASCREATOR")),

        graph_(graph) {}
  ~PERSON_EDGE_IS() {}

  bool Query(Decoder &input, Encoder &output) override {
//     LOG(INFO)<<"begin person edge is";
    auto txn = graph_.GetReadTransaction();

    oid_t req = input.get_long();
    CHECK(input.empty());

    vid_t root{};
    if (!txn.GetVertexIndex(person_label_id_, req, root)) {
      return false;
    }
    // LOG(INFO)<<"end get root, root is "<<root;
    
    //isLocatedIn
{    
//       auto person_isLocatedIn_place_out =
//           txn.GetOutgoingSingleGraphView<grape::EmptyType>(
//               person_label_id_, place_label_id_, isLocatedIn_label_id_);
//     assert(person_isLocatedIn_place_out.exist(root));
//     auto item = person_isLocatedIn_place_out.get_edge(root);
//     auto person_place = gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item).neighbor;
//     item = place_name_col_.get(person_place);
//     output.put_buffer_object(item);

//     // LOG(INFO)<<"end get location";

//     //knows
//     auto person_knows_person_out = txn.GetOutgoingGraphView<Date>(
//           person_label_id_, person_label_id_, knows_label_id_);
//     auto person_knows_person_in = txn.GetIncomingGraphView<Date>(
//           person_label_id_, person_label_id_, knows_label_id_);
//     auto ie = person_knows_person_in.get_edges(root);
//     for (; ie.is_valid(); ie.next()){
//         auto v = ie.get_neighbor();
//         auto person_firstName_item = person_firstName_col_.get(v);
//         output.put_buffer_object(person_firstName_item);
//     }

//     // LOG(INFO)<<"end get in knows";
    
//     auto oe = person_knows_person_out.get_edges(root);
//     for (; oe.is_valid(); oe.next()){
//         auto v = oe.get_neighbor();
//         auto person_firstName_item = person_firstName_col_.get(v);
//         output.put_buffer_object(person_firstName_item);
//     }

//     // LOG(INFO)<<"end get out knows";
    
//     //hasInterestIn
//     auto toe = txn.GetOutgoingEdges<grape::EmptyType>(
//           person_label_id_, root, tag_label_id_, hasInterest_label_id_);
//     for (; toe.is_valid(); toe.next())
//     {
//         auto v=toe.get_neighbor();
//         auto item = tag_name_col_.get(v);
//         output.put_buffer_object(item);
//     }

//     // LOG(INFO)<<"end get has interested in ";
    
//     //workAt
//     auto person_workAt_organisation_out = txn.GetOutgoingGraphView<int>(
//           person_label_id_, organisation_label_id_, workAt_label_id_);
//     auto companies = person_workAt_organisation_out.get_edges(root);
//     for (; companies.is_valid(); companies.next())
//     {
//       auto item = organisation_name_col_.get(companies.get_neighbor());
//       output.put_buffer_object(item);
//     }

//     // LOG(INFO)<<"end get workat";
    
//     //studyAt
//     auto person_studyAt_organisation_out = txn.GetOutgoingGraphView<int>(
//           person_label_id_, organisation_label_id_, studyAt_label_id_);
//     auto universities = person_studyAt_organisation_out.get_edges(root);
//     for (; universities.is_valid(); universities.next())
//     {
//       auto item = organisation_name_col_.get(universities.get_neighbor());
//       output.put_buffer_object(item);
//     }

//     //hasCreator
//     auto comment_ie = txn.GetIncomingEdges<grape::EmptyType>(
//           person_label_id_, root, comment_label_id_, hasCreator_label_id_);
//     for (; comment_ie.is_valid(); comment_ie.next())
//     {
//       auto cid = comment_ie.get_neighbor();
//       auto item=comment_content_col_.get(cid);
//       output.put_buffer_object(item);
//     }
//     auto post_ie = txn.GetIncomingEdges<grape::EmptyType>(
//           person_label_id_, root, post_label_id_, hasCreator_label_id_);
//     for (; post_ie.is_valid(); post_ie.next())
//     {
//       auto pid = post_ie.get_neighbor();
//       auto content = gbp::BufferBlock::Ref<int>(item) == 0 ? post_imageFile_col_.get(pid) : post_content_col_.get(pid);
//       output.put_buffer_object(content);
//     }
}


    // LOG(INFO)<<"end get studyat";
    
    //在comment和post部分实现这个边的访问
    // //likes
    // auto poc = txn.GetOutgoingEdges<grape::EmptyType>(person_label_id_,root,comment_label_id_,likes_label_id_);
    // for(;poc.is_valid();poc.next()){
    //     auto v=poc.get_neighbor();
    //     auto item=comment_content_col_.get(v);
    //     output.put_buffer_object(item);
    // }

    // LOG(INFO)<<"end get comment likes";
    
    // auto pop = txn.GetOutgoingEdges<grape::EmptyType>(person_label_id_,root,post_label_id_,likes_label_id_);
    // for(;pop.is_valid();pop.next()){
    //     auto v=pop.get_neighbor();
    //     auto content = gbp::BufferBlock::Ref<int>(item) == 0 ? post_imageFile_col_.get(v) : post_content_col_.get(v);
    //     output.put_buffer_object(content);
    // }

    // LOG(INFO)<<"end get post likes";
    
    return true;
    }

  private:
    label_t person_label_id_;
    label_t hasInterest_label_id_;
    label_t isLocatedIn_label_id_;
    label_t knows_label_id_;
    label_t studyAt_label_id_;
    label_t workAt_label_id_;
    label_t likes_label_id_;

    //isLocatedIn
    label_t organisation_label_id_;
    label_t place_label_id_;
    const StringColumn &place_name_col_;

    //knows
    const StringColumn &person_firstName_col_;

    //hasInterestIn
    label_t tag_label_id_;
    const StringColumn &tag_name_col_;

    //workAt+studyAt
    const StringColumn &organisation_name_col_;

    //likes
    label_t comment_label_id_;
    const StringColumn &comment_content_col_;
    label_t post_label_id_;
    const StringColumn &post_content_col_;
    const StringColumn &post_imageFile_col_;

    //hasCreator
    label_t hasCreator_label_id_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::PERSON_EDGE_IS *app = new gs::PERSON_EDGE_IS(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::PERSON_EDGE_IS *casted = static_cast<gs::PERSON_EDGE_IS *>(app);
    delete casted;
  }
}

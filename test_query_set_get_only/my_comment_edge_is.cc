#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include <flex/storages/rt_mutable_graph/types.h>
#include <flex/utils/property/column.h>
#include <grape/types.h>
// #include "utils.h"

namespace gs
{

class COMMENT_EDGE_IS : public AppBase {
public:
  COMMENT_EDGE_IS(GraphDBSession &graph)
      : person_label_id_(graph.schema().get_vertex_label_id("PERSON")),
        post_label_id_(graph.schema().get_vertex_label_id("POST")),
        comment_label_id_(graph.schema().get_vertex_label_id("COMMENT")),
        comment_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
            graph.get_vertex_property_column(comment_label_id_, "content")))),
        //likes
        likes_label_id_(graph.schema().get_edge_label_id("LIKES")),
        //hasTag
        hasTag_label_id_(graph.schema().get_edge_label_id("HASTAG")),
        tag_label_id_(graph.schema().get_vertex_label_id("TAG")),
        tag_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(tag_label_id_, "name")))),
        //isLocatedIn
        isLocatedIn_label_id_(graph.schema().get_edge_label_id("ISLOCATEDIN")),
        place_label_id_(graph.schema().get_vertex_label_id("PLACE")),
        place_name_col_(*(std::dynamic_pointer_cast<StringColumn>(graph.get_vertex_property_column(place_label_id_, "name")))),
        //replyOf
        replyOf_label_id_(graph.schema().get_edge_label_id("REPLYOF")),
          post_imageFile_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "imageFile")))),
          post_length_col_(*(std::dynamic_pointer_cast<IntColumn>(
              graph.get_vertex_property_column(post_label_id_, "length")))),
            post_content_col_(*(std::dynamic_pointer_cast<StringColumn>(
              graph.get_vertex_property_column(post_label_id_, "content")))),

        graph_(graph) {}
  ~COMMENT_EDGE_IS() {}

    bool Query(Decoder &input, Encoder &output) override {
        // LOG(INFO)<<"begin comment edge is";
        auto txn = graph_.GetReadTransaction();

        oid_t id = input.get_long();
        CHECK(input.empty());

        vid_t lid{};
        if (!txn.GetVertexIndex(comment_label_id_, id, lid)) {
            return false;
        }

        
        
        // LOG(INFO)<<"end get vertex";
        
        // //person likes
        // auto person_likes_comment_in = txn.GetIncomingGraphView<Date>(
        //     comment_label_id_, person_label_id_, likes_label_id_);
        // auto person_ie = person_likes_comment_in.get_edges(lid); 
        // LOG(INFO)<<"end get likes comments in get edges";
        // for (; person_ie.is_valid(); person_ie.next())
        // {
        //     auto item = person_ie.get_data();
        //     output.put_buffer_object(((Date *)item)->milli_second);
        // }

        // LOG(INFO)<<"end get likes";

        // // //hasTag
        // auto comment_hasTag_tag_out = txn.GetOutgoingGraphView<grape::EmptyType>(
        //   comment_label_id_, tag_label_id_, hasTag_label_id_);
        // LOG(INFO)<<"get comment_hasTag_tag_out";
        // auto oe = comment_hasTag_tag_out.get_edges(lid);
        // LOG(INFO)<<"get comment_hasTag_tag_out get edges";
        // for (; oe.is_valid(); oe.next())
        // {
        //     auto v=oe.get_neighbor();
        //     LOG(INFO)<<"get neighbor, neighbor is "<<v;
        //     auto item = tag_name_col_.get(v);
        //     output.put_buffer_object(item);
        // }

        // LOG(INFO)<<"end get hasTag";
        
        // //isLocatedIn
        // auto comment_isLocatedIn_place_out =
        // txn.GetOutgoingSingleGraphView<grape::EmptyType>(
        // comment_label_id_, place_label_id_, isLocatedIn_label_id_);
        // assert(comment_isLocatedIn_place_out.exist(lid));
        // auto item = comment_isLocatedIn_place_out.get_edge(lid);
        // auto person_place = gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item).neighbor;
        // item = place_name_col_.get(person_place);
        // output.put_buffer_object(item);

        // LOG(INFO)<<"end get isLocatedIn";

        // replyOf comment, post部分在post的测试代码中实现
{       
        // auto comment_replyOf_comment_in = txn.GetIncomingGraphView<grape::EmptyType>(
        //   comment_label_id_, comment_label_id_, replyOf_label_id_);
        // auto ie = comment_replyOf_comment_in.get_edges(lid);
        // for (; ie.is_valid(); ie.next())
        // {
        //   auto u = ie.get_neighbor();
        //   auto item=comment_content_col_.get(u);
        //   output.put_buffer_object(item);
        // }
        // LOG(INFO)<<"end get replyOf comment";
}

        // auto comment_replyOf_post_out_=txn.GetOutgoingSingleGraphView<grape::EmptyType>(
        //           comment_label_id_, post_label_id_, replyOf_label_id_);
        // if(comment_replyOf_post_out_.exist(lid)){
            // auto &e1 = comment_replyOf_post_out_.get_edge(lid);
            // auto v=gbp::BufferBlock::Ref<gs::MutableNbr<grape::EmptyType>>(e1).neighbor;
            // auto item = post_length_col_.get(lid);
            // auto content = gbp::BufferBlock::Ref<int>(item) == 0 ? post_imageFile_col_.get(lid) : post_content_col_.get(lid);
            // output.put_buffer_object(content);
        // }

        // LOG(INFO)<<"end get replyOf";

        return true;
    }

  private:
    label_t person_label_id_;
    label_t comment_label_id_;
    label_t post_label_id_;
    const StringColumn &comment_content_col_;

    //likes
    label_t likes_label_id_;

    //hasTag
    label_t hasTag_label_id_;
    label_t tag_label_id_;
    const StringColumn &tag_name_col_;

    //isLocatedIn
    label_t place_label_id_;
    label_t isLocatedIn_label_id_;
    const StringColumn &place_name_col_;

    //replyOf
    label_t replyOf_label_id_;
    const StringColumn &post_imageFile_col_;
    const StringColumn &post_content_col_;
    const IntColumn &post_length_col_;

    GraphDBSession &graph_;
};

} // namespace gs

extern "C"
{
  void *CreateApp(gs::GraphDBSession &db)
  {
    gs::COMMENT_EDGE_IS *app = new gs::COMMENT_EDGE_IS(db);
    return static_cast<void *>(app);
  }

  void DeleteApp(void *app)
  {
    gs::COMMENT_EDGE_IS *casted = static_cast<gs::COMMENT_EDGE_IS *>(app);
    delete casted;
  }
}

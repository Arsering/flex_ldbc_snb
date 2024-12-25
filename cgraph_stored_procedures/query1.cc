#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include <bitset>

namespace gs
{
    class Query1 : public AppBase
    {
        static constexpr int limit = 2000;

    public:
        Query1(GraphDBSession &graph)
            : center_label_id_(graph.schema().get_vertex_label_id("center")),
              medium_label_id_(graph.schema().get_vertex_label_id("medium")),
              connect_label_id_(graph.schema().get_edge_label_id("connect")),
              type_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
                  graph.get_vertex_property_column(medium_label_id_, "type")))),
              graph_(graph)
        {
            duration = 0;
            t0 = 0;
            t1 = 0;
            t2 = 0;
            d1_num = 0;
            d2_num = 0;
        }
        ~Query1()
        {
            LOG(INFO) << "duration = " << duration << ", t0 = " << t0 << ", t1 = " << t1 << ", t2 = " << t2;
            ;
            LOG(INFO) << "d1_num = " << d1_num << ", d2_num = " << d2_num;
        }

        bool Query(Decoder &input, Encoder &output)
        {
            duration -= grape::GetCurrentTime();
            t0 -= grape::GetCurrentTime();
            auto txn = graph_.GetReadTransaction();
            int32_t medium_types_num = input.get_int();
            std::unordered_set<std::string_view> valid_types;
            for (auto i = 0; i < medium_types_num; ++i)
            {
                valid_types.insert(input.get_string());
            }
            auto center_id = input.get_string();
            CHECK(input.empty());
            gs::vid_t center_vid;
            if (!txn.GetVertexIndex(center_label_id_, center_id, center_vid))
            {
                LOG(INFO) << center_id << " not founded\n";
                txn.Abort();
                duration += grape::GetCurrentTime();
                return false;
            }
            t0 += grape::GetCurrentTime();
            t1 -= grape::GetCurrentTime();
            // get all medium with center id.
            std::vector<gs::vid_t> medium_vids;
#if 0
            const auto &edges = txn.GetOutgoingEdges<double>(
                center_label_id_, center_vid, medium_label_id_, connect_label_id_);
#else
            auto edge_view = txn.GetOutgoingGraphView<double>(
                center_label_id_, medium_label_id_, connect_label_id_);
            static thread_local std::vector<MutableNbr<double>> edges;
            edge_view.read(center_vid, edges);
#endif
            int sum_degree = 0;
            auto reserver_edge_view = txn.GetIncomingGraphView<double>(
                medium_label_id_, center_label_id_, connect_label_id_);
            for (auto &e : edges)
            {
                auto medium_vid = e.neighbor;
                static thread_local std::vector<char> medium_type_vec;
                // auto medium_type = type_name_col_.get_view(medium_vid);
                type_name_col_.read(medium_vid, medium_type_vec);
                std::string_view medium_type(medium_type_vec.data(), medium_type_vec.size());
                if (valid_types.find(medium_type) != valid_types.end())
                {
                    int in_degree = reserver_edge_view.estimated_degree(medium_vid);
                    if (in_degree > 1)
                    {
                        medium_vids.push_back(medium_vid);
                        sum_degree += (in_degree - 1);
                        if (sum_degree >= limit)
                        {
                            break;
                        }
                    }
                }
            }

            int num = 0;
            size_t begin_loc = output.skip_int();
            d1_num += medium_vids.size();
            t1 += grape::GetCurrentTime();
            t2 -= grape::GetCurrentTime();
            // get all medium with center id.
            //  reverse expand, need the results along each path.
            for (auto medium_vid : medium_vids)
            {
                static thread_local std::vector<char> oid_vec;
                // auto oid = txn.GetVertexId(medium_label_id_, medium_vid).AsStringView();
                txn.read_vertex_id(medium_label_id_, medium_vid, oid_vec);
                std::string_view oid(oid_vec.data(), oid_vec.size());
                // const auto &edges = reserver_edge_view.get_edges(medium_vid);
                reserver_edge_view.read(medium_vid, edges);
                for (auto &e : edges)
                {
                    auto nbr_vid = e.neighbor;
                    if (nbr_vid != center_vid)
                    {
                        // auto nbr_oid = txn.GetVertexId(center_label_id_, nbr_vid).AsStringView();
                        static thread_local std::vector<char> nbr_oid_vec;
                        txn.read_vertex_id(center_label_id_, nbr_vid, nbr_oid_vec);
                        std::string_view nbr_oid(nbr_oid_vec.data(), nbr_oid_vec.size());
                        output.put_string_view(oid);
                        output.put_double(e.data);
                        output.put_string_view(nbr_oid);
                        ++num;
                        if (num == limit)
                        {
                            break;
                        }
                    }
                }
                if (num == limit)
                {
                    break;
                }
            }
            d2_num += num;
            t2 += grape::GetCurrentTime();
            duration += grape::GetCurrentTime();
            output.put_int_at(begin_loc, num);
            return true;
        }

    private:
        label_t center_label_id_;
        label_t medium_label_id_;
        label_t connect_label_id_;
        const StringColumn &type_name_col_;
        GraphDBSession &graph_;
        double duration, t0, t1, t2;
        size_t d1_num, d2_num;
    };
} // namespace gs

extern "C"
{
    void *CreateApp(gs::GraphDBSession &db)
    {
        gs::Query1 *app = new gs::Query1(db);
        return static_cast<void *>(app);
    }

    void DeleteApp(void *app)
    {
        gs::Query1 *casted = static_cast<gs::Query1 *>(app);
        delete casted;
    }
}

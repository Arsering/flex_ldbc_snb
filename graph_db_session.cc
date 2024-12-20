/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/utils/app_utils.h"

namespace gs
{

  ReadTransaction GraphDBSession::GetReadTransaction()
  {
    uint32_t ts = db_.version_manager_.acquire_read_timestamp();
    return ReadTransaction(db_.graph_, db_.version_manager_, ts);
  }

  InsertTransaction GraphDBSession::GetInsertTransaction()
  {
    uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
    return InsertTransaction(db_.graph_, alloc_, logger_, db_.version_manager_,
                             ts);
  }

  SingleVertexInsertTransaction
  GraphDBSession::GetSingleVertexInsertTransaction()
  {
    uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
    return SingleVertexInsertTransaction(db_.graph_, alloc_, logger_,
                                         db_.version_manager_, ts);
  }

  SingleEdgeInsertTransaction GraphDBSession::GetSingleEdgeInsertTransaction()
  {
    uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
    return SingleEdgeInsertTransaction(db_.graph_, alloc_, logger_,
                                       db_.version_manager_, ts);
  }

  UpdateTransaction GraphDBSession::GetUpdateTransaction()
  {
    uint32_t ts = db_.version_manager_.acquire_update_timestamp();
    return UpdateTransaction(db_.graph_, alloc_, work_dir_, logger_,
                             db_.version_manager_, ts);
  }

  const MutablePropertyFragment &GraphDBSession::graph() const
  {
    return db_.graph();
  }

  MutablePropertyFragment &GraphDBSession::graph() { return db_.graph(); }

  const Schema &GraphDBSession::schema() const { return db_.schema(); }

  std::shared_ptr<ColumnBase> GraphDBSession::get_vertex_property_column(
      uint8_t label, const std::string &col_name) const
  {
    return db_.get_vertex_property_column(label, col_name);
  }

  std::shared_ptr<RefColumnBase> GraphDBSession::get_vertex_id_column(
      uint8_t label) const
  {
    assert(false);
    return std::make_shared<TypedRefColumn<oid_t>>(
        db_.graph().lf_indexers_[label].get_keys(), StorageStrategy::kMem);
  }

#define likely(x) __builtin_expect(!!(x), 1)

  std::vector<char> GraphDBSession::Eval(const std::string &input)
  {
    uint8_t type = input.back();
    const char *str_data = input.data();
    size_t str_len = input.size() - 1;

    std::vector<char> result_buffer;
    // if (type != 4)
    //   return result_buffer;
    static std::atomic<size_t> query_id{0};
    gbp::get_counter_query().fetch_add(1);

    Decoder decoder(str_data, str_len);
    Encoder encoder(result_buffer);

    AppBase *app = nullptr;
    if (likely(apps_[type] != nullptr))
    {
      app = apps_[type];
    }
    else
    {
      app_wrappers_[type] = db_.CreateApp(type, thread_id_);
      if (app_wrappers_[type].app() == NULL)
      {
        LOG(ERROR) << "[Query-" + std::to_string((int)type)
                   << "] is not registered...";
        return result_buffer;
      }
      else
      {
        apps_[type] = app_wrappers_[type].app();
        app = apps_[type];
      }
    }
#ifdef DEBUG
    gbp::debug::get_counter_malloc().store(0);
    gbp::debug::get_counter_bpm().store(0);
    gbp::debug::get_counter_copy().store(0);
    gbp::debug::get_log_marker().store(1);
    gbp::debug::get_counter_MAP_find().store(0);
    gbp::debug::get_counter_any().store(0);
    gbp::debug::get_counter_bp().store(0);
    size_t st = gbp::GetSystemTime();
#endif
    if (app->Query(decoder, encoder))
    {
#ifdef DEBUG
      st = gbp::GetSystemTime() - st;
      LOG(INFO) << "profiling: [" << (int)type << "]["
                << gbp::debug::get_counter_bp().load() << " | " << st << " | "
                << gbp::debug::get_counter_any().load() << "]";
#endif
      // encoder.put_long(gbp::debug::get_query_id().load());
      std::string_view output{result_buffer.data(), result_buffer.size()};
      size_t cur_query_id = query_id.fetch_add(1);
      gbp::debug::get_query_id().store(cur_query_id);
      if (cur_query_id < 100)
      {
        std::lock_guard lock(gbp::debug::get_file_lock());
        gbp::get_query_file()
            << input << "eor#" << gbp::debug::get_query_id().load() << "eor#";
        gbp::get_result_file()
            << output << "eor#" << gbp::debug::get_query_id().load() << "eor#";
      }
      else if (cur_query_id == 100)
      {
        gbp::get_query_file().flush();
        gbp::get_result_file().flush();
        gbp::get_query_file().close();
        gbp::get_result_file().close();
      }
      return result_buffer;
    }

    LOG(INFO) << "[Query-" << (int)type << "][Thread-" << thread_id_
              << "] retry - 1 / 3";
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    decoder.reset(str_data, str_len);
    result_buffer.clear();

    if (app->Query(decoder, encoder))
    {
      return result_buffer;
    }

    LOG(INFO) << "[Query-" << (int)type << "][Thread-" << thread_id_
              << "] retry - 2 / 3";
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    decoder.reset(str_data, str_len);
    result_buffer.clear();

    if (app->Query(decoder, encoder))
    {
      return result_buffer;
    }

    LOG(INFO) << "[Query-" << (int)type << "][Thread-" << thread_id_
              << "] retry - 3 / 3";
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    decoder.reset(str_data, str_len);
    result_buffer.clear();

    if (app->Query(decoder, encoder))
    {
      return result_buffer;
    }
    LOG(INFO) << "[Query-" << (int)type << "][Thread-" << thread_id_
              << "] failed after 3 retries";

    result_buffer.clear();
    return result_buffer;
  } // namespace gs

#undef likely

  void GraphDBSession::GetAppInfo(Encoder &result) { db_.GetAppInfo(result); }

  int GraphDBSession::SessionId() const { return thread_id_; }

} // namespace gs

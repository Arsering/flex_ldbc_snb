import json
import sys

resuls_dir = sys.argv[1]
queryname = sys.argv[2]

fin_log = open(resuls_dir + "/LDBC-SNB-results.json")
data = json.load(fin_log)

all_metrics = data['all_metrics']

assert data["unit"] == "MICROSECONDS"


duration = float(data["total_duration"])
duration /= 1000000
duration /= 3600

op_count = int(data["total_count"])
throughput = float(data["throughput"])

print("## " + queryname)

summary_header  = "| Benchmark duration(h) | Benchmark operations | Throughput(ops/s) |"
summary_spliter = "| ------------------ | -------------------- | ---------- |"
summary_content = "| " + ("%.2f" % duration) + " | " + str(op_count) + " | " + ("%.2f" % throughput) + " |"

print("")
print("- Summary")
print("")
print(summary_header)
print(summary_spliter)
print(summary_content)

detail_header =  "| Query   | Total count | Min. | Max. | Mean | P50  | P90  | P95  | P99  |"
detail_spliter = "| ------- | ----------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |"

print("")
print("- Detail")
print("")
print(detail_header)
print(detail_spliter)

queries = []
for i in range(14):
	queries.append("LdbcQuery" + str(i + 1))

queries.append("LdbcShortQuery1PersonProfile")
queries.append("LdbcShortQuery2PersonPosts")
queries.append("LdbcShortQuery3PersonFriends")
queries.append("LdbcShortQuery4MessageContent")
queries.append("LdbcShortQuery5MessageCreator")
queries.append("LdbcShortQuery6MessageForum")
queries.append("LdbcShortQuery7MessageReplies")
queries.append("LdbcUpdate1AddPerson")
queries.append("LdbcUpdate2AddPostLike")
queries.append("LdbcUpdate3AddCommentLike")
queries.append("LdbcUpdate4AddForum")
queries.append("LdbcUpdate5AddForumMembership")
queries.append("LdbcUpdate6AddPost")
queries.append("LdbcUpdate7AddComment")
queries.append("LdbcUpdate8AddFriendship")

query_num = len(all_metrics)
assert query_num == len(queries)

dict = {}
count_dict = {}
for i in range(query_num):
	metric = all_metrics[i]
	name = metric["name"]
	assert metric["unit"] == "MICROSECONDS"
	count = int(metric["count"])
	count_dict[name] = count
	rt = metric["run_time"]
	rt_mean = float(rt["mean"])
	rt_min = int(rt["min"])
	rt_max = int(rt["max"])
	rt_p50 = int(rt["50th_percentile"])
	rt_p90 = int(rt["90th_percentile"])
	rt_p95 = int(rt["95th_percentile"])
	rt_p99 = int(rt["99th_percentile"])
	detail_line = "| " + name + " | " + str(count) + " | " + str(rt_min) + " | " + str(rt_max) + " | " + ("%.2f" % rt_mean) + " | " + str(rt_p50) + " | " + str(rt_p90) + " | " + str(rt_p95) + " | " + str(rt_p99) + " |"
	dict[name] = detail_line

for i in range(query_num):
	print(dict[queries[i]])


fin_delay = open(resuls_dir + "/LDBC-SNB-validation.json")
delay = json.load(fin_delay)

delay_count_dict = {}
expected_delay_count = int(delay["excessive_delay_count"])
execcessive_delay = delay["excessive_delay_count_per_type"]
get_delay_count = 0
for i in range(query_num):
	query_name = queries[i]
	dc = int(execcessive_delay[query_name])
	delay_count_dict[query_name] = dc
	get_delay_count += dc
	
assert expected_delay_count == get_delay_count

delay_header  = "| Query | Total count | Total late count | Late count Percentage |"
delay_spliter = "| ----- | ----------- | ---------------- | --------------------- |"

print("")
print("- Delay")
print("")
print(delay_header)
print(delay_spliter)

for i in range(query_num):
	query_name = queries[i]
	count = count_dict[query_name]
	delay_count = delay_count_dict[query_name]
	pc = float(delay_count) / float(count) * 100.0
	delay_line = "| " + query_name + " | " + str(count) + " | " + str(delay_count) + " | " + ("%.2f" % pc) + " |"
	print(delay_line)

delay_total_pc = float(expected_delay_count) / float(op_count) * 100.0
delay_total_line = "| Total | " + str(op_count) + " | " + str(expected_delay_count) + " | " + ("%.2f" % delay_total_pc) + " |"
print(delay_total_line)

print("")

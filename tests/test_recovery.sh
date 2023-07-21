#!/bin/bash

RESULT_LOG=$1
GRAPH_DIR=$2

export GRAPHSCOPE_IP="172.31.28.163"

# ux_ts is original_start_time of the last ins-x request
u1_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate1 | tail -n 1 | cut -d '|' -f 6`
u2_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate2 | tail -n 1 | cut -d '|' -f 6`
u3_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate3 | tail -n 1 | cut -d '|' -f 6`
u4_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate4 | tail -n 1 | cut -d '|' -f 6`
u5_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate5 | tail -n 1 | cut -d '|' -f 6`
u6_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate6 | tail -n 1 | cut -d '|' -f 6`
u7_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate7 | tail -n 1 | cut -d '|' -f 6`
u8_ts=`sed '$d' $RESULT_LOG | grep LdbcUpdate8 | tail -n 1 | cut -d '|' -f 6`

PERSON_UPDATE_STREAM=$GRAPH_DIR/updateStream_0_*_person.csv
FORUM_UPDATE_STREAM=$GRAPH_DIR/updateStream_0_*_forum.csv

u1_req=`grep "$u1_ts|.*|1|.*" $PERSON_UPDATE_STREAM`
u1_person=`echo $u1_req | cut -d '|' -f 4`

u2_req=`grep "$u2_ts|.*|2|.*" $FORUM_UPDATE_STREAM`
u2_person=`echo $u2_req | cut -d '|' -f 4`
u2_post=`echo $u2_req | cut -d '|' -f 5`

u3_req=`grep "$u3_ts|.*|3|.*" $FORUM_UPDATE_STREAM`
u3_person=`echo $u3_req | cut -d '|' -f 4`
u3_comment=`echo $u3_req | cut -d '|' -f 5`

u4_req=`grep "$u4_ts|.*|4|.*" $FORUM_UPDATE_STREAM`
u4_forum=`echo $u4_req | cut -d '|' -f 4`

u5_req=`grep "$u5_ts|.*|5|.*" $FORUM_UPDATE_STREAM`
u5_forum=`echo $u5_req | cut -d '|' -f 4`
u5_person=`echo $u5_req | cut -d '|' -f 5`

u6_req=`grep "$u6_ts|.*|6|.*" $FORUM_UPDATE_STREAM`
u6_post=`echo $u6_req | cut -d '|' -f 4`

u7_req=`grep "$u7_ts|.*|7|.*" $FORUM_UPDATE_STREAM`
u7_comment=`echo $u7_req | cut -d '|' -f 4`

u8_req=`grep "$u8_ts|.*|8|.*" $FORUM_UPDATE_STREAM`
u8_person1=`echo $u8_req | cut -d '|' -f 4`
u8_person2=`echo $u8_req | cut -d '|' -f 5`

echo "The last INS-1 request before shutting down is:"
echo $u1_req
echo "Checking the existence of person($u1_person)"
rt_admin query_vertex PERSON $u1_person

echo "The last INS-2 request before shutting down is:"
echo $u2_req
echo "Checking the existence of edge person($u2_person) likes post($u2_post):"
rt_admin query_edge PERSON $u2_person POST $u2_post LIKES

echo "The last INS-3 request before shutting down is:"
echo $u3_req
echo "Checking the existence of edge person($u3_person) likes comment($u3_comment):"
rt_admin query_edge PERSON $u3_person COMMENT $u3_comment LIKES

echo "The last INS-4 request before shutting down is:"
echo $u4_req
echo "Checking the existence of forum($u4_forum)"
rt_admin query_vertex FORUM $u4_forum

echo "The last INS-5 request before shutting down is:"
echo $u5_req
echo "Checking the existence of edge forum($u5_forum) hasMember person($u5_person):"
rt_admin query_edge FORUM $u5_forum PERSON $u5_person HASMEMBER

echo "The last INS-6 request before shutting down is:"
echo $u6_req
echo "Checking the existence of POST($u6_post)"
rt_admin query_vertex POST $u6_post

echo "The last INS-7 request before shutting down is:"
echo $u7_req
echo "Checking the existence of COMMENT($u7_comment)"
rt_admin query_vertex COMMENT $u7_comment

echo "The last INS-8 request before shutting down is:"
echo $u8_req
echo "Checking the existence of edge person($u8_person1) knows person($u8_person2):"
rt_admin query_edge PERSON $u8_person1 PERSON $u8_person2 KNOWS

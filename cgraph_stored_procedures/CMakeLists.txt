cmake_minimum_required(VERSION 3.1)
project(flex_ldbc_snb_queries)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3 -ggdb3 -fno-omit-frame-pointer -O0 -fno-gnu-unique -fPIC -rdynamic")

# 设置FLEX_HOME路径
set(FLEX_HOME "/usr/local" CACHE PATH "Path to FLEX installation")

# 查找MPI
find_package(MPI REQUIRED)

# 设置包含目录
include_directories(
    ${MPI_CXX_INCLUDE_PATH}
    ${FLEX_HOME}/include
)

# 设置链接目录
link_directories(${FLEX_HOME}/lib)

# 定义要编译的目标列表
set(TARGETS
    n_ic1 n_ic2 n_ic3 n_ic4 n_ic5 n_ic6 n_ic7 n_ic8 n_ic9 n_ic10 
    n_ic11 n_ic12 n_ic13 n_ic14 n_is1 n_is2 n_is3 n_is4 n_is5 
    n_is6 n_is7 
)

# 为每个目标创建共享库
foreach(target ${TARGETS})
    add_library(${target} SHARED ${target}.cc)
    target_link_libraries(${target}
        flex_utils
        flex_rt_mutable_graph
        flex_graph_db
    )
endforeach() 
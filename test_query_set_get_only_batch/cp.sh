#!/bin/bash

# 遍历当前目录下所有的 .cc 文件
for file in *.cc; do
    # 跳过已经存在的 *_nocol.cc 文件
    if [[ $file != *_nocol.cc ]]; then
        # 构造新文件名
        new_file="${file%.cc}_nocol.cc"
        # 如果新文件不存在，则复制
        if [ ! -f "$new_file" ]; then
            cp "$file" "$new_file"
            echo "已复制 $file 到 $new_file"
        fi
    fi
done
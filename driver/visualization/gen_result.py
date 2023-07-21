from matplotlib import pyplot as plt
import numpy as np
from pandas import *
import json
import sys

def plot_table(row, col, vals,graph):
    """
    函数功能: 绘制二维表格，草图如下:
        -----------------
            |col1 |col2 |
        -----------------
        row1|value|value|
        -----------------
        row2|value|value|
        -----------------
    输入：
        row:string,(N)           #['row1', 'row2']
        col:string,(M)           #['col1', 'col2']
        vals:np, (N,M)          
    """
    
    R, C = len(row), len(col)
    idx = Index(row)
    df = DataFrame(np.random.randn(R, C), index=idx, columns=col)
    
    # 根据行数列数设置表格大小
    figC, figR = C, R
    fig = plt.figure(figsize=(30, 30))
    
    # 设置fig并去掉边框
    ax = fig.add_subplot(111, frameon=True, xticks=[], yticks=[])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    the_table=plt.table(cellText=vals, rowLabels=df.index, colLabels=df.columns,  rowLoc='center', loc='center',cellLoc='center')
    the_table.set_fontsize(30)
    
    # 伸缩表格大小常数
    #the_table.scale(figR/R*2 ,figC/C*0.8)
    the_table.scale(1.0,4.0)
    #plt.title('time with http',fontdict= {'fontsize': 40})
    plt.axis('off')
    plt.savefig(graph+".png")
graph = str(sys.argv[1])
row = []
for i in range(1,15):
    row.append("Complex" + str(i))
for i in range(1,8):
    row.append("Short" + str(i))
for i in range(1,9):
    row.append("Update" + str(i))


col = ['count','min','max','mean','50th_percentile','90th_percentile','95th_percentile','99th_percentile']
n = len(col)
S = []
data = {}
with open('../graphscope/results/LDBC-SNB-results.json', 'r') as f:
    data = json.load(f)
    
    dat = data['all_metrics']
    dat = dat[:1] + dat[6:14] + dat[1:6] + dat[14:]
    for i in dat:
        r = []
        for j in col:
            r.append(str(round(i['run_time'][j],2)))
        S.append(r)

#print(len(S))
#print(len(S[0]))
col = ['Total count','Min.','Max','Mean','P50','P90','P95','P99']
vals = np.array(S)

str_30 = "Complex1 523933 465 346384 8681.31 5508 6830 8034 125660 \
Complex2 368170 53 174056 1930.08 1484 2409 2822 6496 \
Complex3 128512 11474 281888 20244.07 18475 24585 26700 62124 \
Complex4 378397 58 229528 2853.48 2316 3824 4389 8970 \
Complex5 189198 28225 961792 337757.53 320480 521600 576768 677856 \
Complex6 43109 234 145424 3047.59 3212 5481 6125 11987 \
Complex7 283797 39 220432 464.26 112 262 337 4095 \
Complex8 1513588 1570 241512 4273.26 3136 6924 8412 13817 \
Complex9 35475 9432 527840 165935.19 154592 239440 268720 335520 \
Complex10 368170 1626 280064 38679.43 37138 45850 49478 97048 \
Complex11 681114 62 231824 924.78 561 813 938 4747 \
Complex12 309598 127 291392 23991.27 22355 31612 35428 69392 \
Complex13 716963 48 221800 743.21 328 1031 1194 4459 \
Complex14 278006 85 291328 3791.86 1061 1957 29770 51572 \
Short1 7302529 31 137800 78.90 59 79 87 188 \
Short2 7302529 29 162496 269.85 208 402 606 1119 \
Short3 7302529 33 157000 562.04 306 964 2517 4019 \
Short4 7302328 33 200720 95.75 73 95 105 218 \
Short5 7302328 31 148488 83.90 69 90 98 164 \
Short6 7302328 32 200944 87.70 73 95 103 163 \
Short7 7302328 31 138872 114.99 94 154 177 243 \
Update1 4197 137 121984 904.11 285 498 2214 8328 \
Update2 2562611 39 184808 207.85 79 104 117 1059 \
Update3 3724097 39 185376 206.87 79 104 117 1076 \
Update4 71805 82 141104 262.83 137 180 243 1195 \
Update5 7724095 43 218200 230.37 100 148 188 1080 \
Update6 939752 70 177536 239.61 155 220 260 753 \
Update7 2755361 81 412048 295.88 156 203 230 1513 \
Update8 273349 58 151272 1244.82 542 2598 5052 9410"


str_300 =  "\
        Complex1 326326 44 907136 14075.74 8056 10017 10990 396976 \
        Complex2 229311 44 312144 1277.65 888 1455 1759 6927 \
        Complex3 59750 93576 546624 156157.10 152040 194320 209920 259440 \
        Complex4 235679 45344 8803 354.60 2701 4820 5492 13930 \
Complex5 101006 2433 789728 280544.12 270032 409616 446784 529568 \
Complex6 14629 60 158816 18781.95 4879 42130 46532 77072 \
Complex7 265140 37 325968 436.86 100 243 312 4513 \
Complex8 2828157 41 367152 446.34 128 191 220 4739 \
Complex9 12035 53 465024 148705.99 131728 222448 265152 319952 \
Complex10 192829 45 400528 60427.09 58990 78028 86088 127720 \
Complex11 353520 76 346656 1187.11 851 1048 1166 6582 \
Complex12 192829 42 359680 32632.02 30925 46922 53576 88640 \
Complex13 446551 49 344592 1507.92 1283 1956 2252 7017 \
Complex14 173152 48 384960 10563.06 2058 24738 30599 56864 \
Short1 6817920 31 116340 75.85 58 78 86 154 \
Short2 6817920 30 314576 186.69 162 237 284 437 \
Short3 6817920 31 310048 767.01 432 1407 3250 5338 \
Short4 6816037 34 312896 94.71 74 98 108 181 \
Short5 6816037 30 325200 83.18 70 93 101 153 \
Short6 6816037 32 144512 86.05 73 97 105 153 \
Short7 6816037 30 120028 118.22 98 165 200 274 \
Update1 1924 142 275728 898.52 291 480 640 9501 \
Update2 1450882 41 311536 252.13 82 107 120 1242 \
Update3 2235838 42 333632 252.16 82 107 119 1191 \
Update4 32817 92 122848 312.20 141 183 243 1038 \
Update5 4396694 44 354352 272.18 100 150 190 1131 \
Update6 590149 88 345424 347.37 171 263 413 1681 \
Update7 1960498 84 333792 349.77 158 209 241 2226 \
Update8 172045 62 118504 1294.21 447 2805 4936 12065 "

str_100 = "Complex1 369993 44 611200 23103.42 7037 9801 229632 274704 \
Complex2 259996 42 303696 1640.68 1188 1750 2096 6405 \
Complex3 78210 31114 459920 54658.66 50944 66132 73916 140016 \
Complex4 267217 57 312752 2906.34 2262 3982 4702 9809 \
Complex5 123331 2440 725664 230111.66 225288 321104 361584 435104 \
Complex6 22165 60 179504 7541.73 7990 15299 17312 31876 \
Complex7 253153 38 246344 521.15 103 255 362 4181 \
Complex8 1923965 41 313504 538.28 132 213 286 4220 \
Complex9 18254 50 461472 151057.30 144912 204512 249152 312832 \
Complex10 240496 47 462048 50097.52 48152 59880 66512 126256 \
Complex11 437265 54 317216 1119.87 705 922 1094 5342 \
Complex12 218632 50 368784 30224.56 28266 40128 46346 90180 \
Complex13 506306 44 309936 1091.08 617 1259 1452 5601 \
Complex14 196323 43 350768 8055.02 1332 23917 28993 54058 \
Sort1 6168974 30 178408 93.69 58 81 95 401 \
Sort2 6168974 29 148504 235.34 179 317 439 837 \
Sort3 6168974 31 168744 675.01 358 1256 2985 4743 \
Sort4 6169406 33 195504 110.18 71 97 113 443 \
Sort5 6169406 31 134752 95.65 67 91 103 302 \
Sort6 6169406 32 155128 98.49 71 95 108 295 \
Sort7 6169406 31 160032 130.90 95 163 194 366 \
Update1 2471 150 176888 1180.09 307 1435 2912 12181 \
Update2 2172995 39 241384 232.43 79 107 141 2063 \
Update3 4223704 40 286592 231.89 79 107 143 2203 \
Update4 41796 85 236696 320.73 143 204 305 2285 \
Update5 4927042 44 250704 254.14 99 154 211 2119 \
Update6 738684 79 210312 305.89 166 230 280 1645 \
Update7 3590745 75 247552 318.91 157 208 255 2087 \
Update8 190818 60 327680 1336.57 531 2896 5380 10755"
if graph == "sf30":
    ls = str_30.split()
elif graph == "sf100":
    ls = str_100.split()
elif graph == "sf300":
    ls = str_300.split()
else:
    raise Exception(graph)
k = 0
for i in ls:
    if (k%9) != 0: 
        S[k//9][((k%9)-1)] += " ("+i+")"
    k+=1
with open(graph+".csv",'w') as f:
    f.write(',')
    for i in range(len(col)):
        if i+ 1 == len(col):
            f.write(col[i]+'\n')
        else:
            f.write(col[i]+',')
    for i in range(len(S)):
        f.write(row[i] + ',')
        for j in range(len(S[0])):
            if j+1 == len(S[0]):
                f.write(S[i][j] + '\n')
            else:
                f.write(S[i][j] + ',')
    for i in data.keys():
        if i != "all_metrics":
            f.write(str(i) + ":" + str(data[i]) + ",")

vals = np.array(S)
plot_table(row, col, vals,graph)
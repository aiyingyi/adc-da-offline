#!/bin/bash


db=warningplatform
# 电压单位问题
# 指定算法计算的窗口大小(充电次数)


window_size=2
# 预警斜率和压差的边界值
th1=0.5
th2=40


echo `date -d @$1   +'%Y-%m-%d %H:%M:%S'`

# 将脚本参数放入到数组里面，包含20个时间戳以及vin码
_index=0
args[0]=0
for i in "$@"
do
  if [ $_index -lt $[$paraNum-1] ]
    then
    args[$_index]=`date -d @$i   +'%Y-%m-%d %H:%M:%S'`
    let _index++
  else
    vin=$i
  fi
done

echo "数组的元素为: ${args[*]}"

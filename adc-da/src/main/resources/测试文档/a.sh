#!/bin/bash

db=warningplatform

# 电压单位问题
# 指定算法计算的窗口大小(充电次数)
window_size=10

# 预警斜率和压差的边界值
th1=0.5
th2=40

# 将脚本参数放入到数组里面，包含20个时间戳以及vin码
_index=0
for i in "$@"
do
  if [ $_index -lt $[$#-1] ]
  then
    args[_index]=`date -d @$(($i/1000)) +'%Y-%m-%d %H:%M:%S'`
    let _index++
  else
    vin=$i
  fi
done


echo $vin
echo ${args[0]}


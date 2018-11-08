#!/usr/bin/env bash
#check the param

usage='用法: startAll.sh '

jobs=('trend' 'terminal' 'share' 'scene' 'qr' 'phone' 'link1' 'link2' 'entrance')

for job in ${jobs[@]};do
 sh ./stop.sh ${job}
done

echo 'start all end'

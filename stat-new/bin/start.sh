#!/usr/bin/env bash
#check the param
a=${@}
count=0
for var in "$@"
do
    (( count++ ))
done

usage='
     用法: start.sh job [repartitionNum]\n
     job:[trend：趋势分析|terminal：终端分析|share：分享|scene：场景|qr：二维码|phone：手机型号|link1：外链1|link2：外链2|entrance：入口页|region:地区]
       \n
     rePartitionNum:spark再分区数量，默认20\n
     '

job='jobName'
repartitionNum=20

#check the args
if [ ${count} -lt 1 ]
  then
   echo ${usage}
elif [ ${count} -gt 2 ]
  then
    echo ${usage}
elif [ ${count} -eq 1 ]
  then
    job=$1
elif [ ${count} -eq 2 ]
  then
    job=$1
    repartitionNum=$2
fi
#job:[trend|terminal|share|scene|qr|phone|link1|link2|entrance]
#match the job to submit job
mainClass='com.ald.stat.job.'
if [ ${job} = 'trend' ]
 then
   mainClass=${mainClass}'AnalysisTrend'
elif [ ${job} = 'terminal' ]
 then
   mainClass=${mainClass}'AnalysisTerminal'
elif [ ${job} = 'share' ]
 then
  mainClass=${mainClass}'AnalysisShare'
elif [ ${job} = 'scene' ]
 then
  mainClass=${mainClass}'AnalysisScene'
elif [ ${job} = 'qr' ]
 then
  mainClass=${mainClass}'AnalysisQrCode'
elif [ ${job} = 'phone' ]
 then
  mainClass=${mainClass}'AnalysisResionalAndPhoneModel'
elif [ ${job} = 'link1' ]
 then
  mainClass=${mainClass}'AnalysisLinkFirstBatch'
elif [ ${job} = 'link2' ]
 then
  mainClass=${mainClass}'AnalysisLinkSecondBatch'
elif [ ${job} = 'entrance' ]
 then
  mainClass=${mainClass}'AnalysisEntrancePage'
elif [ ${job} = 'region' ]
 then
  mainClass=${mainClass}'AnalysisRegion'
else
 exit 1
fi

#mainClass='com.ald.stat.job.AnalysisTrend'

appId=`yarn application -list |grep "${mainClass}" |awk '{print $1}'`
if [ -n "$appId" ]; then
    echo "ERROR: The job ${mainClass} is started!"
    exit 1
fi

#获取配置
master=`sed '/spark.master.host/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`
numExecutors=`sed '/num.executors/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`
executorCores=`sed '/executor.cores/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`
executorMemory=`sed '/executor.memory/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`
maxCores=`sed '/max.cores/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`
jarPath=`sed '/jar.full.path/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`
submitUser=`sed '/submit.user/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`


su ${submitUser}

echo "Starting job "${mainClass}

spark-submit --master ${master} --deploy-mode cluster --num-executors ${numExecutors} --executor-cores ${executorCores} --executor-memory ${executorMemory} --conf spark.cores.max=${maxCores} --class ${mainClass} ${jarPath} &


appId=''
while [ -z ${appId} ]; do
    echo -e ".\c"
    sleep 1
    appId=`yarn application -list |grep "${mainClass}" |awk '{print $1}'`
    if [ -n "$appId" ]; then
        break
    fi
done

echo 'job is started,appId='${appId}
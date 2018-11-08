#!/usr/bin/env bash
#check the param
a=${@}
count=0
for var in "$@"
do
    (( count++ ))
done

usage='
     用法: stop.sh job \n
     job:[trend：趋势分析|terminal：终端分析|share：分享|scene：场景|qr：二维码|phone：手机型号|link1：外链1|link2：外链2|entrance：入口页]'

job='jobName'

#check the args
if [ ${count} -eq 1 ]
  then
    job=$1
else
    echo ${usage}
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
else
 exit 1
fi

#mainClass='com.ald.stat.job.AnalysisTrend'
appId=`yarn application -list |grep "${mainClass}" |awk '{print $1}'`
if [ -z "$appId" ]; then
    echo "ERROR: The job ${mainClass} is not started!"
    exit 1
else
 yarn application -kill ${appId}
fi

#check again
appId=`yarn application -list |grep "${mainClass}" |awk '{print $1}'`
while [ -n ${appId} ]; do
        sleep 1
        appId=`yarn application -list |grep "${mainClass}" |awk '{print $1}'`
    if [ -z "${appId}" ]; then
        break
    fi
done

echo 'job is stopped,appId='${appId}
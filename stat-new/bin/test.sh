#!/usr/bin/env bash


arr=(`sed '/jobs.list/!d;s/.*=//' ../src/main/resources/app.properties | tr -d '\r'`)

for var in ${arr[@]};do
echo ${var}
done
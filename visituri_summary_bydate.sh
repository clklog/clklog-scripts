#!/bin/bash

source ./clklog-scripts.env
ck_login="clickhouse-client -u $CK_USER_NAME --password $CK_USER_PWD"
ck_log_db="$CLKLOG_LOG_DB."
ck_stat_db="$CLKLOG_STAT_DB."

#-----------------------------------------------------------
# 参数1：0-全量计算（truncate全表，重新生成数据），1-增量计算
# 参数2：对应报表日期，默认为当前日期
#----------------------------------------------------------

if [ $# -lt 1 ] ; then
   echo "至少需要输入1个参数！"
exit 1
fi;

#当前日期
current_date=$(date +%Y-%m-%d)

#计算的日期
cal_date=${current_date}
end_date=${current_date}
#如果传入计算日期，按指定日期计算，否则计算当日数据
if [ $# -ge 2 ] ; then
  cal_date=$2
fi;

# 文件存放目录
dir_path=$CLKLOG_SCRIPT_LOG/visituri_summary_bydate
mkdir -p ${dir_path}

# 脚本日志名
shell_log_name=${current_date}.log

# 计算方式
cal_mode=$1


cal_type="增量计算"
if [ $1 -eq 0 ]; then
  #查询埋点日志表当前数据最小的日期（用于全量计算）
  min_date=`${ck_login} --query="select min(partition) from (select max(stat_date) as partition from ${ck_log_db}log_analysis where stat_date>='2023-01-01' group by stat_date)"`

  #----------- 4.全量计算 ----------- 
  cal_date=$min_date
  end_date=`date -d "+1 day $current_date" +%Y-%m-%d`
  cal_type="全量计算"

  #清空报表
  echo "truncate table start..." >> ${dir_path}/${shell_log_name}
  ${ck_login} --query="truncate table ${ck_stat_db}visituri_summary_bydate"
  echo "truncate table success..." >> ${dir_path}/${shell_log_name}

elif [ $1 -eq 1 ]; then
  #----------- 5 增量计算 -----------
  end_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

fi

st=$(date "+%Y-%m-%d %H:%M:%S")
echo "-------------------- ${st} start--------------------" >> ${dir_path}/${shell_log_name}
echo "计算方式 : $cal_type...起始日期：${cal_date}, 结束日期：${current_date}" >> ${dir_path}/${shell_log_name}
echo "当前计算日期 : ${current_date}" >> ${dir_path}/${shell_log_name}

while [[ "$cal_date" != "$end_date" ]]
do
  # 按日计算sql
  
  previous_date=`date -d "-7 day $cal_date" +%Y-%m-%d`

  cur_date_sql="INSERT INTO ${ck_stat_db}visituri_summary_bydate
SELECT '${cal_date}' AS stat_date
	, If(t2.lib = '', 'all', t2.lib) AS lib
	, multiIf(t2.project_name = '', 'all', t2.project_name = 'N/A', '', t2.project_name) AS project_name
	,multiIf(t2.url = '', 'all', t2.url = 'N/A', '', t2.url) AS url,
	multiIf(t2.title = '', 'all', t2.title = 'N/A', '', t2.title) AS title,
	 t2.pv,  NOW() AS update_time
FROM (
	SELECT lib, project_name, url,count(t1.pv) AS pv,title 
	FROM (
		SELECT lib, project_name,if(url = '', 'N/A', url) AS url
			, multiIf(lib = 'js'
				AND event = '\$pageview', event, lib IN ('iOS', 'Android')
				AND event = '\$AppViewScreen', event, lib = 'MiniProgram'
				AND event = '\$MPViewScreen', event, NULL) AS pv
				,if(title = '', 'N/A', title) AS title 
		FROM ${ck_log_db}log_analysis
		WHERE stat_date = '${cal_date}'
	) t1
	GROUP BY lib, project_name,url,title WITH CUBE
) t2"
  # echo ${cur_date_sql}

  ${ck_login} --query="${cur_date_sql}"
  # 强制进行分区合并，实现重复数据的删除
  ${ck_login} --query="optimize table ${ck_stat_db}visituri_summary_bydate FINAL SETTINGS optimize_skip_merged_partitions=1"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

#!/bin/bash
#ckhost="localhost"
#ckport=9000
ck_login="clickhouse-client -u default --password 123456"
ck_log_db="clklog."
ck_stat_db="clklog."

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
dir_path=/usr/local/services/scripts/cklog/user_visit_bydate
mkdir ${dir_path}

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
  ${ck_login} --query="truncate table ${ck_stat_db}user_visit_bydate"
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
  cur_date_sql="INSERT INTO ${ck_stat_db}user_visit_bydate
SELECT '${cal_date}' AS stat_date
	, If(t3.lib = '', 'all', t3.lib) AS lib
	, multiIf(t3.project_name = '', 'all', t3.project_name = 'N/A', '', t3.project_name) AS project_name
	, If(t3.is_first_day = '', 'all', t3.is_first_day) AS is_first_day
	, multiIf(t3.country = '', 'all', t3.country = 'N/A', '', t3.country) AS country
	, multiIf(t3.province = '', 'all', t3.province = 'N/A', '', t3.province) AS province
	, t3.v1_uv, t3.v2_uv, t3.v3_uv, t3.v4_uv, t3.v5_uv,t3.v6_uv,t3.v7_uv,t3.v8_uv,t3.v9_uv,t3.v10_uv
	, t3.v11_15_uv, t3.v16_50_uv, t3.v51_100_uv, t3.v101_200_uv, t3.v201_300_uv,t3.vt301_uv
	, NOW() AS update_time
FROM (
	SELECT lib, project_name, is_first_day
		, if(country = '', 'N/A', country) AS country
		, if(province = '', 'N/A', province) AS province
		, countDistinct(if(visit = 1, distinct_id, NULL)) AS v1_uv
		, countDistinct(if(visit = 2, distinct_id, NULL)) AS v2_uv
		, countDistinct(if(visit = 3, distinct_id, NULL)) AS v3_uv
		, countDistinct(if(visit = 4, distinct_id, NULL)) AS v4_uv
		, countDistinct(if(visit = 5, distinct_id, NULL)) AS v5_uv
		, countDistinct(if(visit = 6, distinct_id, NULL)) AS v6_uv
		, countDistinct(if(visit = 7, distinct_id, NULL)) AS v7_uv
		, countDistinct(if(visit = 8, distinct_id, NULL)) AS v8_uv
		, countDistinct(if(visit = 9, distinct_id, NULL)) AS v9_uv
		, countDistinct(if(visit = 10, distinct_id, NULL)) AS v10_uv
		, countDistinct(if(visit >= 11
			AND visit <= 15, distinct_id, NULL)) AS v11_15_uv
		, countDistinct(if(visit >= 16
			AND visit <= 50, distinct_id, NULL)) AS v16_50_uv
		, countDistinct(if(visit >= 51
			AND visit <= 100, distinct_id, NULL)) AS v51_100_uv
		, countDistinct(if(visit >= 101
			AND visit <= 200, distinct_id, NULL)) AS v101_200_uv
		, countDistinct(if(visit >= 201
			AND visit <= 300, distinct_id, NULL)) AS v201_300_uv
		, countDistinct(if(visit >= 301, distinct_id, NULL)) AS vt301_uv
	FROM (
		SELECT distinct_id, lib, project_name, is_first_day, country
			, province, countDistinct(event_session_id) AS visit
		FROM (
			SELECT distinct_id, lib, project_name, is_first_day, country
				, province, arraySort(groupUniqArray(stat_date)) AS stat_dates
				, event_session_id
			FROM ${ck_log_db}log_analysis
			WHERE stat_date <= '${cal_date}'
				AND stat_date >= '${previous_date}'
				AND event_session_id <> ''
			GROUP BY event_session_id, distinct_id, lib, project_name, is_first_day, country, province
		) t1
		WHERE indexOf(stat_dates, toDate('${cal_date}')) = 1
		GROUP BY lib, project_name, is_first_day, country, province, distinct_id
	) t2
	GROUP BY lib, project_name, is_first_day, country, province WITH CUBE
) t3"
   echo ${cur_date_sql}

  ${ck_login} --query="${cur_date_sql}"
  # 强制进行分区合并，实现重复数据的删除
  ${ck_login} --query="optimize table ${ck_stat_db}user_visit_bydate FINAL SETTINGS optimize_skip_merged_partitions=1"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

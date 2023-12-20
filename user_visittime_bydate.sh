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
dir_path=/usr/local/services/scripts/cklog/user_visittime_bydate
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
  ${ck_login} --query="truncate table ${ck_stat_db}user_visittime_bydate"
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

  cur_date_sql="INSERT INTO ${ck_stat_db}user_visittime_bydate
SELECT '${cal_date}' AS stat_date
	, If(t3.lib = '', 'all', t3.lib) AS lib
	, multiIf(t3.project_name = '', 'all', t3.project_name = 'N/A', '', t3.project_name) AS project_name
	, If(t3.is_first_day = '', 'all', t3.is_first_day) AS is_first_day
	, multiIf(t3.country = '', 'all', t3.country = 'N/A', '', t3.country) AS country
	, multiIf(t3.province = '', 'all', t3.province = 'N/A', '', t3.province) AS province
	, t3.vt0_10_uv, t3.vt10_30_uv, t3.vt30_60_uv, t3.vt60_120_uv, t3.vt120_180_uv
	, t3.vt180_240_uv, t3.vt240_300_uv, t3.vt300_600_uv, t3.vt600_1800_uv, t3.vt1800_3600_uv,t3.vt3600_uv
	, NOW() AS update_time
FROM (
	SELECT lib, project_name, is_first_day
		, if(country = '', 'N/A', country) AS country
		, if(province = '', 'N/A', province) AS province
		, countDistinct(if(visitTime < 10, distinct_id, NULL)) AS vt0_10_uv
		, countDistinct(if(visitTime >= 10
			AND visitTime < 30, distinct_id, NULL)) AS vt10_30_uv
		, countDistinct(if(visitTime >= 30
			AND visitTime < 60, distinct_id, NULL)) AS vt30_60_uv
		, countDistinct(if(visitTime >= 60
			AND visitTime < 120, distinct_id, NULL)) AS vt60_120_uv
		, countDistinct(if(visitTime >= 120
			AND visitTime < 180, distinct_id, NULL)) AS vt120_180_uv
		, countDistinct(if(visitTime >= 180
			AND visitTime < 240, distinct_id, NULL)) AS vt180_240_uv
		, countDistinct(if(visitTime >= 240
			AND visitTime < 300, distinct_id, NULL)) AS vt240_300_uv
		, countDistinct(if(visitTime >= 300
			AND visitTime < 600, distinct_id, NULL)) AS vt300_600_uv
		, countDistinct(if(visitTime >= 600
			AND visitTime < 1800, distinct_id, NULL)) AS vt600_1800_uv
		, countDistinct(if(visitTime >= 1800
			AND visitTime < 3600, distinct_id, NULL)) AS vt1800_3600_uv
		, countDistinct(if(visitTime >= 3600, distinct_id, NULL)) AS vt3600_uv
	FROM (
		SELECT distinct_id, lib, project_name, is_first_day, country
			, province, sum(diff) AS visitTime
		FROM (
			SELECT distinct_id, lib, project_name, is_first_day, country
				, province, arraySort(groupUniqArray(stat_date)) AS stat_dates
				, max(log_time) - min(log_time) AS diff
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
  ${ck_login} --query="optimize table ${ck_stat_db}user_visittime_bydate FINAL SETTINGS optimize_skip_merged_partitions=1"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

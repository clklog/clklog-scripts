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
dir_path=/usr/local/services/scripts/cklog/user_pv_bydate
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
  ${ck_login} --query="truncate table ${ck_stat_db}user_pv_bydate"
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
  cur_date_sql="INSERT INTO ${ck_stat_db}user_pv_bydate
SELECT '${cal_date}' AS stat_date
	, If(t3.lib = '', 'all', t3.lib) AS lib
	, multiIf(t3.project_name = '', 'all', t3.project_name = 'N/A', '', t3.project_name) AS project_name
	, If(t3.is_first_day = '', 'all', t3.is_first_day) AS is_first_day
	, multiIf(t3.country = '', 'all', t3.country = 'N/A', '', t3.country) AS country
	, multiIf(t3.province = '', 'all', t3.province = 'N/A', '', t3.province) AS province
	, t3.pv0_uv, t3.pv1_uv, t3.pv2_5_uv, t3.pv6_10_uv, t3.pv11_20_uv, t3.pv21_30_uv
	, t3.pv31_40_uv, t3.pv41_50_uv, t3.pv51_100_uv, t3.pv101_uv, NOW() AS update_time
FROM (
	SELECT lib, project_name, is_first_day
		, if(country = '', 'N/A', country) AS country
		, if(province = '', 'N/A', province) AS province
		, countDistinct(if(pv = 0, distinct_id, NULL)) AS pv0_uv
		, countDistinct(if(pv = 1, distinct_id, NULL)) AS pv1_uv
		, countDistinct(if(pv >= 2
			AND pv <= 5, distinct_id, NULL)) AS pv2_5_uv
		, countDistinct(if(pv >= 6
			AND pv <= 10, distinct_id, NULL)) AS pv6_10_uv
		, countDistinct(if(pv >= 11
			AND pv <= 20, distinct_id, NULL)) AS pv11_20_uv
		, countDistinct(if(pv >= 21
			AND pv <= 30, distinct_id, NULL)) AS pv21_30_uv
		, countDistinct(if(pv >= 31
			AND pv <= 40, distinct_id, NULL)) AS pv31_40_uv
		, countDistinct(if(pv >= 41
			AND pv <= 50, distinct_id, NULL)) AS pv41_50_uv
		, countDistinct(if(pv >= 51
			AND pv <= 100, distinct_id, NULL)) AS pv51_100_uv
		, countDistinct(if(pv >= 101, distinct_id, NULL)) AS pv101_uv
	FROM (
		SELECT lib, project_name, is_first_day, country, province
			, distinct_id, count(t1.pv) AS pv
		FROM (
			SELECT lib, project_name, is_first_day, country, province
				, distinct_id
				, multiIf(lib = 'js'
					AND event = '\$pageview', event, lib IN ('iOS', 'Android')
					AND event = '\$AppViewScreen', event, lib = 'MiniProgram'
					AND event = '\$MPViewScreen', event, NULL) AS pv
			FROM ${ck_log_db}log_analysis
			WHERE stat_date = '${cal_date}'
		) t1
		GROUP BY lib, project_name, is_first_day, country, province, distinct_id
	) t2
	GROUP BY lib, project_name, is_first_day, country, province WITH CUBE
) t3"
   echo ${cur_date_sql}

  ${ck_login} --query="${cur_date_sql}"
  # 强制进行分区合并，实现重复数据的删除
  ${ck_login} --query="optimize table ${ck_stat_db}user_pv_bydate FINAL SETTINGS optimize_skip_merged_partitions=1"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

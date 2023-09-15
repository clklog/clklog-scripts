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
dir_path=/usr/local/services/scripts/cklog/visitor_life_bydate
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
  ${ck_login} --query="truncate table ${ck_stat_db}visitor_life_bydate"
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

   before_date_1=`date -d "-1 day $cal_date" +%Y-%m-%d`
   before_date_2=`date -d "-2 day $cal_date" +%Y-%m-%d`
   before_date_3=`date -d "-3 day $cal_date" +%Y-%m-%d` 
 
  # 按日计算sql 
  cur_date_sql="INSERT INTO ${ck_stat_db}visitor_life_bydate
SELECT '${cal_date}' AS stat_date
	, if(lib = '', 'all', lib) AS lib
	, multiIf(project_name = '', 'all', project_name = 'N/A', '', project_name) AS project_name
	, multiIf(country = '', 'all', country = 'N/A', '', country) AS country
	, multiIf(province = '', 'all', province = 'N/A', '', province) AS province
	, new_users, continuous_active_users, revisit_users, silent_users, churn_users
	, now() AS update_time
FROM (
	SELECT lib, project_name, country, province
		, count(DISTINCT new_user) AS new_users, count(DISTINCT active_user) AS active_users
		, count(DISTINCT continuous_active_user) AS continuous_active_users, count(DISTINCT revisit_user) AS revisit_users
		, count(DISTINCT silent_user) AS silent_users, count(DISTINCT churn_user) AS churn_users
		, now() AS update_time
	FROM (
		SELECT If(lib = '', 'N/A', lib) AS lib
			, If(project_name = '', 'N/A', project_name) AS project_name
			, If(country = '', 'N/A', country) AS country
			, If(province = '', 'N/A', province) AS province
			, if(is_first_day
				AND indexOf(date_arr, '${cal_date}') > 0, distinct_id, NULL) AS new_user
			, if(indexOf(date_arr, '${cal_date}') > 0, distinct_id, NULL) AS active_user
			, if(indexOf(date_arr, '${cal_date}') > 0
				AND indexOf(date_arr, '${before_date_1}') > 0, distinct_id, NULL) AS continuous_active_user
			, if(indexOf(date_arr, '${cal_date}') > 0
				AND indexOf(date_arr, '${before_date_1}') = 0, distinct_id, NULL) AS revisit_user
			, if(indexOf(date_arr, '${cal_date}') = 0
				AND indexOf(date_arr, '${before_date_1}') > 0, distinct_id, NULL) AS silent_user
			, if(indexOf(date_arr, '${cal_date}') = 0
			AND indexOf(date_arr, '${before_date_1}') = 0
			AND (indexOf(date_arr, '${before_date_2}') > 0
				OR indexOf(date_arr, '${before_date_3}') > 0), distinct_id, NULL) AS churn_user
		FROM (
			SELECT lib, project_name, country, province, distinct_id
				, max(if(stat_date = '${cal_date}'
					AND is_first_day = 'true', true, false)) AS is_first_day
				, groupUniqArray(formatDateTime(stat_date, '%Y-%m-%d')) AS date_arr
				, count(1) AS users
			FROM ${ck_log_db} log_analysis
			WHERE stat_date >= '${before_date_3}'
				AND stat_date <= '${cal_date}'
				AND ((lib = 'js'
						AND event = '\$pageview')
					OR (lib IN ('MiniProgram','iOS', 'Android')
						AND event IN ('\$AppViewScreen', '\$MPViewScreen')))
			GROUP BY lib, project_name, country, province, distinct_id
		) t1
	) t2
	GROUP BY lib, project_name, country, province WITH CUBE
) t3"
   echo ${cur_date_sql}

  ${ck_login} --query="${cur_date_sql}"
  # 强制进行分区合并，实现重复数据的删除
  ${ck_login} --query="optimize table ${ck_stat_db}visitor_life_bydate FINAL"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

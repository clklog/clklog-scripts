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
dir_path=/usr/local/services/scripts/cklog/visitor_detail_byinfo

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
  ${ck_login} --query="truncate table ${ck_stat_db}visitor_detail_byinfo"
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
  cur_date_sql="INSERT INTO ${ck_stat_db}visitor_detail_byinfo
SELECT '${cal_date}' AS stat_date
	, If(t2.lib = '', 'all', t2.lib) AS lib
	, multiIf(t2.project_name = '', 'all', t2.project_name = 'N/A', '未知', t2.project_name) AS project_name
	, If(t2.is_first_day = '', 'all', t2.is_first_day) AS is_first_day
	, multiIf(t2.country = '', 'all', t2.country = 'N/A', '未知国家', t2.country) AS country
	, multiIf(t2.province = '', 'all', t2.province = 'N/A', '未知省份', t2.province) AS province
	, multiIf(t2.city = '', 'all', t2.city = 'N/A', '未知城市', t2.city) AS city
	, t2.distinct_id AS distinct_id
	, multiIf(t2.client_ip = '', 'all', t2.client_ip = 'N/A', '未知', t2.client_ip) AS client_ip
	, multiIf(t2.manufacturer = '', 'all', t2.manufacturer = 'N/A', '未知', t2.manufacturer) AS manufacturer
	, t2.latest_time AS latest_time
	, t2.first_time AS first_time
	, t6.visitTime AS visit_time
	, t6.visit_count AS visit_count
	, t4.pv AS pv
	,NOW() AS update_time
FROM (
	SELECT lib, project_name, is_first_day, country, province, city
		,distinct_id,manufacturer,client_ip,min(log_time) as first_time
		,max(log_time) as latest_time
	FROM (
		SELECT lib, project_name, is_first_day,
		  if(country = '', 'N/A', country) AS country
			, if(province = '', 'N/A', province) AS province
			, if(city = '', 'N/A', city) AS city
			, distinct_id
			, if(manufacturer = '', 'N/A', manufacturer) AS manufacturer
			, if(client_ip = '', 'N/A', client_ip) AS client_ip
			, log_time as log_time  
		FROM ${ck_log_db}log_analysis
		WHERE stat_date = '${cal_date}' 
		AND distinct_id <> ''
	) t1
	GROUP BY lib, project_name, is_first_day, country, province,city, distinct_id,client_ip,manufacturer 
) t2
	LEFT JOIN (
		SELECT lib, project_name, is_first_day, country, province, city
		,distinct_id,manufacturer
		,client_ip
		,count(t3.pv) AS pv
		FROM (
			SELECT lib, project_name, is_first_day,
			  	if(country = '', 'N/A', country) AS country
				, if(province = '', 'N/A', province) AS province
				, if(city = '', 'N/A', city) AS city
				, distinct_id
				, if(manufacturer = '', 'N/A', manufacturer) AS manufacturer
				, if(client_ip = '', 'N/A', client_ip) AS client_ip 
				, log_time
				, multiIf(lib = 'js'
				AND event = '\$pageview', event, lib IN ('iOS', 'Android')
				AND event = '\$AppViewScreen', event, lib = 'MiniProgram'
				AND event = '\$MPViewScreen', event, NULL) AS pv
			FROM ${ck_log_db}log_analysis
			WHERE stat_date = '${cal_date}'
		) t3
		GROUP BY lib, project_name, is_first_day, country, province,city, distinct_id,client_ip,manufacturer 
	) t4
	ON t2.lib = t4.lib
		AND t2.project_name = t4.project_name
		AND t2.country = t4.country
		AND t2.province = t4.province
		AND t2.is_first_day = t4.is_first_day
		AND t2.city = t4.city
		AND t2.distinct_id = t4.distinct_id
		AND t2.manufacturer = t4.manufacturer
		AND t2.client_ip = t4.client_ip
	LEFT JOIN (
		SELECT lib, project_name, country, province, city
		,distinct_id,manufacturer,client_ip
		,count(1) AS visit_count
		,sum(diff) AS visitTime
		,sum(multiIf(pv = 1, 1, 0)) AS bounce
		FROM (
			SELECT lib, project_name,
			  	if(country = '', 'N/A', country) AS country
				, if(province = '', 'N/A', province) AS province
				, if(city = '', 'N/A', city) AS city
				, distinct_id
				, if(manufacturer = '', 'N/A', manufacturer) AS manufacturer
				, if(client_ip = '', 'N/A', client_ip) AS client_ip 
				, arraySort(groupUniqArray(stat_date)) AS stat_dates
				,max(log_time)-min(log_time) AS diff
				, count(1) AS pv
			FROM ${ck_log_db}log_analysis
			WHERE stat_date <= '${cal_date}'
				AND stat_date >= '${previous_date}'
				AND event_session_id<> ''
			GROUP BY event_session_id,lib, project_name, country, province,city, distinct_id,client_ip,manufacturer
		) t5
		WHERE indexOf(stat_dates, toDate('${cal_date}')) = 1
		GROUP BY lib, project_name, country, province,city, distinct_id,client_ip,manufacturer 
	) t6
	ON t2.lib = t6.lib
		AND t2.project_name = t6.project_name
		AND t2.country = t6.country
		AND t2.province = t6.province
		AND t2.city = t6.city
		AND t2.distinct_id = t6.distinct_id
		AND t2.manufacturer = t6.manufacturer
		AND t2.client_ip = t6.client_ip"
   echo ${cur_date_sql}

  ${ck_login} --query="${cur_date_sql}"
  # 强制进行分区合并，实现重复数据的删除
  ${ck_login} --query="optimize table ${ck_stat_db}visitor_detail_byinfo FINAL"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

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
dir_path=/usr/local/services/scripts/cklog/visitor_detail_bysession

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
  ${ck_login} --query="truncate table ${ck_stat_db}visitor_detail_bysession"
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
  cur_date_sql="INSERT INTO ${ck_stat_db}visitor_detail_bysession
SELECT '${cal_date}' AS stat_date
	, multiIf(t2.project_name = '', 'all', t2.project_name = 'N/A', '未知', t2.project_name) AS project_name
	, multiIf(t2.country = '', 'all', t2.country = 'N/A', '未知国家', t2.country) AS country
	, multiIf(t2.province = '', 'all', t2.province = 'N/A', '未知省份', t2.province) AS province
	, if(t2.client_ip = '', 'N/A', t2.client_ip) AS client_ip
	, multiIf(t2.latest_referrer_host = '', 'all', t2.latest_referrer_host = 'N/A', '直接访问', t2.latest_referrer_host) AS latest_referrer_host
	, multiIf(t2.latest_search_keyword = '', 'all', t2.latest_search_keyword = 'N/A', '', t2.latest_search_keyword) AS latest_search_keyword
	, t2.distinct_id AS distinct_id
	, t2.event_session_id AS event_session_id
	, t2.first_time AS first_time
	, t2.latest_time AS latest_time
	, t2.visit_time AS visit_time
	, t4.pv as pv
	, NOW() AS update_time
	FROM ( 
		SELECT 
		 project_name AS project_name
		, country AS country
		, province AS province
		, client_ip AS client_ip
		, latest_referrer_host AS latest_referrer_host
		, latest_search_keyword AS latest_search_keyword
		, distinct_id AS distinct_id
		, event_session_id AS event_session_id
		, first_time AS first_time
		, latest_time AS latest_time
		, visit_time AS visit_time
		FROM (	
		  SELECT 
				 project_name  AS project_name
				, if(country = '', 'N/A', country) AS country
				, if(province = '', 'N/A', province) AS province
				, distinct_id
				, event_session_id AS event_session_id
				, min(log_time) AS first_time
				, max(log_time) AS latest_time
				, max(log_time)-min(log_time) AS visit_time
				, client_ip AS client_ip
				, arraySort(groupUniqArray(stat_date)) AS stat_dates 
				, if(latest_referrer_host = '' or latest_referrer_host='url的domain解析失败', 'N/A', latest_referrer_host) AS latest_referrer_host 
				, if(latest_search_keyword = '', 'N/A', latest_search_keyword) AS latest_search_keyword 
				FROM ${ck_log_db}log_analysis
				WHERE stat_date <= '${cal_date}'
				AND stat_date >= '${previous_date}'
				AND event_session_id<> ''
				GROUP BY project_name, country, province,distinct_id,event_session_id,client_ip,latest_referrer_host,latest_search_keyword
		) t1
		WHERE indexOf(stat_dates, toDate('${cal_date}')) = 1
	) t2 
	LEFT JOIN (
		SELECT project_name
		, multiIf(t3.country = '', 'all', t3.country = 'N/A', '未知国家', t3.country) AS country
			, multiIf(t3.province = '', 'all', t3.province = 'N/A', '未知省份', t3.province) AS province
				, count(t3.pv) AS pv,
				distinct_id as distinct_id,
				event_session_id as event_session_id,
				client_ip as client_ip 
			FROM (SELECT  project_name,
				  if(country = '', 'N/A', country) AS country
						, if(province = '', 'N/A', province) AS province
					, multiIf(lib = 'js'
						AND event = '\$pageview', event, lib IN ('iOS', 'Android')
						AND event = '\$AppViewScreen', event, lib = 'MiniProgram'
						AND event = '\$MPViewScreen', event, NULL) AS pv
					,  client_ip 
					, distinct_id AS distinct_id
					, event_session_id AS event_session_id
				FROM ${ck_log_db}log_analysis
				WHERE stat_date = '${cal_date}'
			) t3
			where event_session_id <> ''
			GROUP BY project_name, country, province,distinct_id,event_session_id,client_ip
	) t4
	ON t2.project_name = t4.project_name 
	AND t2.country = t4.country 
	AND t2.province = t4.province 
	AND t2.distinct_id = t4.distinct_id 
	AND t2.event_session_id = t4.event_session_id
	AND t2.client_ip = t4.client_ip"
   echo ${cur_date_sql}

  ${ck_login} --query="${cur_date_sql}"
  # 强制进行分区合并，实现重复数据的删除
  ${ck_login} --query="optimize table ${ck_stat_db}visitor_detail_bysession FINAL"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

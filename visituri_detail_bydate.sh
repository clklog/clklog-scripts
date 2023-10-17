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
dir_path=/usr/local/services/scripts/cklog/visituri_detail_bydate

# 脚本日志名
shell_log_name=${current_date}.log

# 计算方式
cal_mode=$1


cal_type="增量计算"
if [ $1 -eq 0 ]; then
  #查询埋点日志表当前数据最小的日期（用于全量计算）
  min_date=`${ck_login} --query="select min(partition) from (select max(stat_date) as partition from ${ck_log_db}log_analysis where stat_date>='2023-10-01' group by stat_date)"`

  #----------- 4.全量计算 ----------- 
  cal_date=$min_date
  end_date=`date -d "+1 day $current_date" +%Y-%m-%d`
  cal_type="全量计算"

  #清空报表
  echo "truncate table start..." >> ${dir_path}/${shell_log_name}
  ${ck_login} --query="truncate table ${ck_stat_db}visituri_detail_bydate"
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
  cur_date_sql="INSERT INTO ${ck_stat_db}visituri_detail_bydate
SELECT '${cal_date}' AS stat_date
	, If(t2.lib = '', 'all', t2.lib) AS lib
	, multiIf(t2.project_name = '', 'all', t2.project_name = 'N/A', '', t2.project_name) AS project_name
	, If(t2.is_first_day = '', 'all', t2.is_first_day) AS is_first_day
	, multiIf(t2.country = '', 'all', t2.country = 'N/A', '', t2.country) AS country
	, multiIf(t2.province = '', 'all', t2.province = 'N/A', '', t2.province) AS province
	, If(t2.urlAndPathAndTitle[1] = '','all',t2.urlAndPathAndTitle[1]) as url
	, If(t2.urlAndPathAndTitle[2] = '','all',t2.urlAndPathAndTitle[2]) as url_path
	, If(t2.urlAndPathAndTitle[3] = '','all',t2.urlAndPathAndTitle[3]) as title 
	, t2.pv, t4.visitCount, t2.uv, t2.new_uv, t2.ipCount
	, t4.visitTime, t4.bounce,t6.entry_count AS entry_count,t5.exit_count AS exit_count,t7.down_pv_count AS down_pv_count,NOW() AS update_time 
FROM (
	SELECT lib, project_name, is_first_day, country, province
		, count(t1.pv) AS pv, COUNTDistinct(t1.user) AS uv
		, COUNTDistinct(t1.new_user) AS new_uv, COUNTDistinct(t1.client_ip) AS ipCount
		,array(t1.url,t1.url_path,t1.title) AS urlAndPathAndTitle 
	FROM (
		SELECT lib, project_name, is_first_day,
		  if(country = '', 'N/A', country) AS country
			, if(province = '', 'N/A', province) AS province
			, if(url = '' or url='url的domain解析失败', 'N/A', url) AS url 
			, multiIf(lib = 'js'
				AND event = '\$pageview', event, lib IN ('iOS', 'Android')
				AND event = '\$AppViewScreen', event, lib = 'MiniProgram'
				AND event = '\$MPViewScreen', event, NULL) AS pv
			, multiIf(lib = 'js'
				AND event = '\$pageview', distinct_id, lib IN ('iOS', 'Android')
				AND event = '\$AppViewScreen', distinct_id, lib = 'MiniProgram'
				AND event = '\$MPViewScreen', distinct_id, NULL) AS user
			, multiIf(lib = 'js'
			AND event = '\$pageview'
			AND is_first_day = 'true', distinct_id, lib IN ('iOS', 'Android')
			AND event = '\$AppViewScreen'
			AND is_first_day = 'true', distinct_id, lib = 'MiniProgram'
			AND event = '\$MPViewScreen'
			AND is_first_day = 'true', distinct_id, NULL) AS new_user
			, client_ip 
			, if(title = '', 'N/A', title) AS title
			, if(url_path = '', 'N/A', url_path) AS url_path
		FROM ${ck_log_db}log_analysis
		WHERE stat_date = '${cal_date}'
	) t1
	GROUP BY lib, project_name, is_first_day, country, province,urlAndPathAndTitle WITH CUBE
) t2
	LEFT JOIN (
		SELECT lib, project_name, is_first_day, country, province
			, count(1) AS visitCount, sum(diff) AS visitTime
			, sum(multiIf(pv = 1, 1, 0)) AS bounce
			,array(t3.url,t3.url_path,t3.title) AS urlAndPathAndTitle 
		FROM (
			SELECT lib, project_name, is_first_day
				, if(country = '', 'N/A', country) AS country
				, if(province = '', 'N/A', province) AS province 
				, if(url = '' or url='url的domain解析失败', 'N/A', url) AS url 
				, arraySort(groupUniqArray(stat_date)) AS stat_dates
				, max(log_time) - min(log_time) AS diff
				, count(1) AS pv
				, if(title = '', 'N/A', title) AS title 
				, if(url_path = '', 'N/A', url_path) AS url_path
			FROM ${ck_log_db}log_analysis
			WHERE stat_date <= '${cal_date}'
				AND stat_date >= '${previous_date}'
				AND event_session_id <> ''
			GROUP BY event_session_id, lib, project_name, is_first_day, country, province,url,title,url_path
		) t3
		WHERE indexOf(stat_dates, toDate('${cal_date}')) = 1
		GROUP BY lib, project_name, is_first_day, country, province,urlAndPathAndTitle WITH CUBE
	) t4
	ON t2.lib = t4.lib
		AND t2.project_name = t4.project_name
		AND t2.country = t4.country
		AND t2.province = t4.province
		AND t2.is_first_day = t4.is_first_day and t2.urlAndPathAndTitle=t4.urlAndPathAndTitle
   LEFT JOIN (
		SELECT 
			t_e_c3.lib AS lib,
			t_e_c3.project_name AS project_name,
			t_e_c3.is_first_day AS is_first_day,
			t_e_c3.country AS country,
			t_e_c3.province AS province,
			count(1) AS exit_count,
			array(t_e_c3.url,t_e_c3.url_path,t_e_c3.title) AS urlAndPathAndTitle 
			FROM
			(
				SELECT 
				t_e_c2.lib AS lib,
				t_e_c2.project_name AS project_name,
				t_e_c2.is_first_day AS is_first_day,
				t_e_c2.country AS country,
				t_e_c2.province AS province,
				t_e_c1.event_session_id AS event_session_id,
				if(t_e_c1.url = '' or url='url的domain解析失败', 'N/A', url) AS url,
				if(t_e_c1.title = '','N/A', title) AS title,
				if(t_e_c1.url_path = '', 'N/A', url_path) AS url_path
				FROM ${ck_log_db}log_analysis t_e_c1,
			    (
					SELECT lib, project_name, is_first_day
						, if(country = '', 'N/A', country) AS country
						, if(province = '', 'N/A', province) AS province 
						, event_session_id AS teventSessionId
						, arraySort(groupUniqArray(stat_date)) AS stat_dates
						, max(log_time) AS maxTime 
					FROM ${ck_log_db}log_analysis
					WHERE stat_date <= '${cal_date}'
						AND stat_date >= '${previous_date}'
						AND event_session_id <> ''
						AND url <> ''
						AND event in('\$pageview','\$AppViewScreen','\$MPViewScreen')
					GROUP BY event_session_id, lib, project_name, is_first_day, country, province
				) t_e_c2 
				WHERE t_e_c1.event_session_id=t_e_c2.teventSessionId 
				AND t_e_c1.log_time=t_e_c2.maxTime 
				AND t_e_c1.url<>''
				AND t_e_c1.event in('\$pageview','\$AppViewScreen','\$MPViewScreen')
				AND indexOf(stat_dates, toDate('${cal_date}')) = 1
				GROUP BY t_e_c2.lib, t_e_c2.project_name, t_e_c2.is_first_day, t_e_c2.country, t_e_c2.province,t_e_c1.event_session_id,t_e_c1.url,t_e_c1.title,t_e_c1.url_path
			) t_e_c3
		    GROUP BY t_e_c3.lib, t_e_c3.project_name, t_e_c3.is_first_day, t_e_c3.country, t_e_c3.province,urlAndPathAndTitle WITH CUBE
	) t5
	ON t2.lib = t5.lib
		AND t2.project_name = t5.project_name
		AND t2.country = t5.country
		AND t2.province = t5.province
		AND t2.is_first_day = t5.is_first_day 
		AND t2.urlAndPathAndTitle = t5.urlAndPathAndTitle
	LEFT JOIN (
		SELECT 
			t_e_c3.lib AS lib,
			t_e_c3.project_name AS project_name,
			t_e_c3.is_first_day AS is_first_day,
			t_e_c3.country AS country,
			t_e_c3.province AS province,
			count(1) AS entry_count,
			array(t_e_c3.url,t_e_c3.url_path,t_e_c3.title) AS urlAndPathAndTitle 
			FROM
			(
				SELECT 
				t_e_c2.lib AS lib,
				t_e_c2.project_name AS project_name,
				t_e_c2.is_first_day AS is_first_day,
				t_e_c2.country AS country,
				t_e_c2.province AS province,
				t_e_c1.event_session_id AS event_session_id,
				if(t_e_c1.url = '' or url='url的domain解析失败', 'N/A', url) AS url,
				if(t_e_c1.title = '','N/A', title) AS title,
				if(t_e_c1.url_path = '', 'N/A', url_path) AS url_path
				FROM ${ck_log_db}log_analysis t_e_c1,
			    (
					SELECT lib, project_name, is_first_day
						, if(country = '', 'N/A', country) AS country
						, if(province = '', 'N/A', province) AS province 
						, event_session_id AS teventSessionId
						, arraySort(groupUniqArray(stat_date)) AS stat_dates
						, min(log_time) AS minTime 
					FROM ${ck_log_db}log_analysis
					WHERE stat_date <= '${cal_date}'
						AND stat_date >= '${previous_date}'
						AND event_session_id <> ''
						AND url <> ''
						AND event in('\$pageview','\$AppViewScreen','\$MPViewScreen')
					GROUP BY event_session_id, lib, project_name, is_first_day, country, province
				) t_e_c2 
				WHERE t_e_c1.event_session_id=t_e_c2.teventSessionId 
				AND t_e_c1.log_time=t_e_c2.minTime 
				AND t_e_c1.url<>'' 
				AND t_e_c1.event in('$pageview','$AppViewScreen','$MPViewScreen')
				AND indexOf(stat_dates, toDate('${cal_date}')) = 1
				GROUP BY t_e_c2.lib, t_e_c2.project_name, t_e_c2.is_first_day, t_e_c2.country, t_e_c2.province,t_e_c1.event_session_id,t_e_c1.url,t_e_c1.title,t_e_c1.url_path
			) t_e_c3
		    GROUP BY t_e_c3.lib, t_e_c3.project_name, t_e_c3.is_first_day, t_e_c3.country, t_e_c3.province,urlAndPathAndTitle WITH CUBE
	) t6
	ON t2.lib = t6.lib
		AND t2.project_name = t6.project_name
		AND t2.country = t6.country
		AND t2.province = t6.province
		AND t2.is_first_day = t6.is_first_day 
		AND t2.urlAndPathAndTitle = t6.urlAndPathAndTitle
	LEFT JOIN (
		SELECT 
			t_e_c3.lib AS lib,
			t_e_c3.project_name AS project_name,
			t_e_c3.is_first_day AS is_first_day,
			t_e_c3.country AS country,
			t_e_c3.province AS province,
			sum(t_e_c3.down_pv_count) AS down_pv_count,
			array(t_e_c3.url,t_e_c3.url_path,t_e_c3.title) AS urlAndPathAndTitle 
			FROM
			(
				SELECT 
				t_e_c2.lib AS lib,
				t_e_c2.project_name AS project_name,
				t_e_c2.is_first_day AS is_first_day,
				t_e_c2.country AS country,
				t_e_c2.province AS province,
				t_e_c1.event_session_id AS event_session_id,
				count(1) AS down_pv_count,
				if(t_e_c1.url = '' or url='url的domain解析失败', 'N/A', url) AS url,
				if(t_e_c1.title = '','N/A', title) AS title,
				if(t_e_c1.url_path = '', 'N/A', url_path) AS url_path
				FROM ${ck_log_db}log_analysis t_e_c1,
			    (
					SELECT lib, project_name, is_first_day
						, if(country = '', 'N/A', country) AS country
						, if(province = '', 'N/A', province) AS province 
						, event_session_id AS teventSessionId
						, arraySort(groupUniqArray(stat_date)) AS stat_dates
						, min(log_time) AS minTime 
					FROM ${ck_log_db}log_analysis
					WHERE stat_date <= '${cal_date}'
						AND stat_date >= '${previous_date}'
						AND event_session_id <> ''
						AND url <> ''
						AND event in('\$pageview','\$AppViewScreen','\$MPViewScreen')
					GROUP BY event_session_id, lib, project_name, is_first_day, country, province
				) t_e_c2 
				WHERE t_e_c1.event_session_id=t_e_c2.teventSessionId 
				AND t_e_c1.log_time=t_e_c2.minTime 
				AND t_e_c1.url<>'' 
				AND t_e_c1.event in('$pageview','$AppViewScreen','$MPViewScreen')
				AND indexOf(stat_dates, toDate('${cal_date}')) = 1
				GROUP BY t_e_c2.lib, t_e_c2.project_name, t_e_c2.is_first_day, t_e_c2.country, t_e_c2.province,t_e_c1.event_session_id,t_e_c1.url,t_e_c1.title,t_e_c1.url_path
			) t_e_c3
		    GROUP BY t_e_c3.lib, t_e_c3.project_name, t_e_c3.is_first_day, t_e_c3.country, t_e_c3.province,urlAndPathAndTitle WITH CUBE
	) t7
	ON t2.lib = t7.lib
		AND t2.project_name = t7.project_name
		AND t2.country = t7.country
		AND t2.province = t7.province
		AND t2.is_first_day = t7.is_first_day 
		AND t2.urlAndPathAndTitle = t7.urlAndPathAndTitle"
   echo ${cur_date_sql}

  ${ck_login} --query="${cur_date_sql}"
  # 强制进行分区合并，实现重复数据的删除
  ${ck_login} --query="optimize table ${ck_stat_db}visituri_detail_bydate FINAL"
  echo "生成数据：${cal_date}" >> ${dir_path}/${shell_log_name}
  
  #开始日期+1天
  cal_date=`date -d "+1 day $cal_date" +%Y-%m-%d`

  sleep 2s
done
echo "${cal_type}完成..." >> ${dir_path}/${shell_log_name}

et=$(date "+%Y-%m-%d %H:%M:%S")
echo -e "-------------------- ${et} end -------------------- \n" >> ${dir_path}/${shell_log_name}

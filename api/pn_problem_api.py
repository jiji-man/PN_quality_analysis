#!/usr/bin/env python
#coding=utf-8

import time,json,urllib,datetime
from django.http import HttpResponse
from src.lib import db_mysql
from conf  import pn_problem_collect_conf

pn_event_delay_data_table='pn_event_delay_data'
pn_event_data_table='pn_event_data'
pn_event_history_table='pn_event_history'
pn_master_list = tuple(pn_problem_collect_conf.pn_master_list)
pn_backup_list = tuple(pn_problem_collect_conf.pn_backup_list)
pn_aws_list = tuple(pn_problem_collect_conf.pn_aws_list)
pn_aliyun_list = tuple(pn_problem_collect_conf.pn_aliyun_list)
pn_node_list = tuple(pn_problem_collect_conf.pn_node_list)


# 将返回值打包成字典转为json格式
def to_json_result(code,message,status,data):
    dict_response = {}
    dict_response['code'] = code
    dict_response['success'] = status
    dict_response['message'] = message
    dict_response['body'] = data
    result = json.dumps(dict_response)
    return result

class UrlChuLi:
    """Url处理类，需要传入两个实参：UrlChuLi('实参','编码类型')，默认utf-8
    url编码方法：url_bm() url解码方法：url_jm()"""

    def __init__(self, can, encoding='utf-8'):
        self.can = can
        self.encoding = encoding

    def url_bm(self):
        """url_bm() 将传入的中文实参转为UrlEncode编码"""
        quma = str(self.can).encode(self.encoding)
        return urllib.parse.quote(quma)

    def url_jm(self):
        """url_jm() 将传入的url进行解码成中文"""
        quma = str(self.can)
        return urllib.parse.unquote(quma, self.encoding)

# 判断字符串是否是一个有效的时间字符串,并转为时间戳格式
def is_vaild_data(str_date):
    try:
        date = time.strptime(str_date, "%Y-%m-%d")
        time_stamp = int(time.mktime(date))
        return time_stamp
    except:
        return False

# 将时间戳转化为标准时间格式
def timestamp_to_time(timestamp):
    time_array = time.localtime(timestamp)
    standard_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return standard_time

# 将总秒数转为天时分秒格式
def sec_to_time(sec):
    day = sec // 86400
    hour = (sec - day * 86400) // 3600
    minute = (sec - hour * 3600 - day * 86400) // 60
    second = sec - hour * 3600 - minute * 60 - day * 86400
    if day == 0 and hour == 0 and minute == 0:
        return (str(second) + "s")
    elif day == 0 and hour == 0 and minute != 0 and second != 0:
        return(str(minute) + "m " + str(second) + "s")
    elif day == 0 and hour == 0 and minute != 0 and second == 0:
        return(str(minute) + "m")
    elif day == 0 and hour != 0 and minute == 0 and second == 0:
        return(str(hour) + "h")
    elif day == 0 and hour != 0 and minute != 0 and second == 0:
        return(str(hour) + "h " + str(minute) + "m")
    elif day == 0 and hour != 0 and second != 0:
        return(str(hour) + "h " + str(minute) + "m " + str(second) + "s")
    elif day != 0 and hour == 0 and minute == 0 and second == 0:
        return(str(day) + "d")
    elif day != 0 and minute != 0 and second == 0:
        return(str(day) + "d " + str(hour) + "h " + str(minute) + "m" )
    elif day != 0 and hour != 0 and minute == 0 and second == 0:
        return(str(day) + "d " + str(hour) + "h")
    else:
        return(str(day) + "d " + str(hour) + "h " + str(minute) + "m " + str(second) + "s")

#判断时间是否正确
def judge_time(start_time,end_time):
    print(start_time,end_time)
    try:
        start_time = time.strptime(start_time, "%Y-%m-%d")
        end_time = time.strptime(end_time, "%Y-%m-%d")
    except Exception as e :
        status_message = ' error : One of start_time and end_time value is invaild.'
        print(e,status_message)
        return False
    else:
        if start_time > end_time:
            status_message = ' error : start_time cannot be greater than end_time.'
            return False
    return True

def judge_source(source_node):
    if source_node not in ('aws', 'aliyun'):
        status_message = " error : source_node' value is incorrect."
        return status_message,False
    else:
        if source_node == 'aliyun':
            source_node = 'opscloud-1'
        elif source_node == 'aws':
            source_node = 'aws-template-2'

    return source_node

#获取两个日期之间的天数,并返回起始日期的时间戳
def get_dat_diff(start_time,end_time):
    start_time = time.strptime(start_time, "%Y-%m-%d")
    end_time = time.strptime(end_time, "%Y-%m-%d")
    start_time_stamp = int(time.mktime(start_time))
    start_time = datetime.datetime(start_time[0], start_time[1], start_time[2])
    end_time = datetime.datetime(end_time[0], end_time[1], end_time[2])
    day_diff = (abs(start_time - end_time).days + 1)
    return start_time_stamp,day_diff

def select_pn_block_data(start_time,end_time,pn_type,source_node,dest_node):
    result = []
    #sql = "SELECT source AS source_node,pn_node AS dest_node,type , MAX(r_clock - clock + 1) AS max_duration , SUM(r_clock - clock + 1) AS total_duration , COUNT(*) AS number_of_times " \
    #      "FROM %s " \
    #      "WHERE clock BETWEEN %s AND %s  AND type = %s AND source = '%s'  AND pn_node IN %s " \
    #      "GROUP BY pn_node;" % (
    #          pn_event_data_table, start_time, end_time, pn_type, source_node, dest_node)
    sql = ("SELECT source AS source_node,pn_node AS dest_node,type , MAX(r_clock - clock + 1) AS max_duration , "
           "SUM(r_clock - clock + 1) AS total_duration , COUNT(*) AS number_of_times "
           "FROM %s "
           "WHERE clock BETWEEN %s AND %s  AND type = %s AND source = '%s'  AND pn_node IN %s "
           "GROUP BY pn_node;" % (
            pn_event_data_table, start_time, end_time, pn_type, source_node, dest_node))
    print('sql',sql)
    try:
        mysql_conn_dict = db_mysql.MyPymysqlPoolDict()
        result = mysql_conn_dict.select(sql)
        print('sql_result', result)
    except Exception as e:
        status_message = 'error: 数据库连接失败. '
        print(e, status_message)
    else:
        if result:
            for r in result:
                if r['source_node'] == 'opscloud-1':
                    r['source_node'] = 'aliyun'
                elif r['source_node'] == 'aws-template-2':
                    r['source_node'] = 'aws'

                if r['dest_node'] == 'opscloud-1':
                    r['dest_node'] = 'aliyun'
                elif r['dest_node'] == 'aws-template-3':
                    r['dest_node'] = 'aws'

                if r['type'] == 0:
                    r['type'] = 'icmp'
                elif r['type'] == 2:
                    r['type'] = 'telnet'
                r['total_duration'] = int(r['total_duration'])
        else:
            result = []
    finally:
        mysql_conn_dict.dispose()
        print('result',result)
    return result


def  pn_status(request):
    from src.lib import django_api
    django_api.DjangoApi().os_environ_update()
    data_list = []
    body_dict = {'data' : data_list}

    # 定义默认的code和status值
    code = 500
    success = False
    if request.method == 'POST':  # 当提交表单时
        try:
            start_time_str = json.loads(request.body.decode()).get('start_time')
            end_time_str = json.loads(request.body.decode()).get('end_time')
            source_node = json.loads(request.body.decode()).get('source_node')
            dest_node = json.loads(request.body.decode()).get('dest_node')
            #pn_attribute = json.loads(request.body.decode()).get('pn_attribute')
            type = json.loads(request.body.decode()).get('type')
        except Exception as e:
            print(e,'error: Failed to get transfer parameters.')
            status_message = " error : Failed to get transfer parameters."
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)
        #判断传入的源节点参数，并进行转换
        if source_node:
            if source_node not in ('aws','aliyun') and source_node != '':
                status_message = " error : source_node' value is incorrect."
                result_json = to_json_result(code, status_message, success, body_dict)
                return HttpResponse(result_json)
            else:
                if source_node == 'aliyun':
                    source_node = 'opscloud-1'
                elif source_node == 'aws':
                    source_node = 'aws-template-2'
                elif source_node == '':
                    source_node = 'opscloud-1'
        else:
            source_node = 'opscloud-1'

        if not start_time_str or not end_time_str :
            status_message = ' error : start_time,end_time value cannot be empty.'
            result_json = to_json_result(code, status_message, success,body_dict)
            return HttpResponse(result_json)

        start_time = is_vaild_data(start_time_str)
        end_time = is_vaild_data(end_time_str)

        if not start_time or  not end_time:
            status_message = ' error : One of start_time and end_time value is invaild.'
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)


        if start_time > end_time:
            status_message = ' error : start_time cannot be greater than end_time.'
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)
        if not type:
            pn_type = 0
        else:
            if type == 'telnet':
                pn_type = 2
            #elif type == 'dealy':
            #    pn_type = 1
            elif  type == 'icmp':
                pn_type = 0
            else:
                pn_type = 0


        if not dest_node:
            status_message = " error : dest_node's value is empty. "
            body_dict['data'] = data_list
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)
        else:
            pn_list = pn_master_list + pn_backup_list
            if dest_node == 'aliyun':
                if pn_type == 2:
                    dest_node = 'zabbix-server'
                elif pn_type == 0:
                    dest_node = 'opscloud-1'
            elif dest_node == 'aws':
                if pn_type == 2:
                    dest_node = 'aws-zabbix-proxy'
                elif pn_type == 0:
                    dest_node = 'aws-template-3'
            elif dest_node not in pn_list:
                status_message = " error : dest_node' value is incorrect. "
                body_dict['data'] = data_list
                result_json = to_json_result(code, status_message, success, body_dict)
                return HttpResponse(result_json)

            dest_node = "('" + dest_node + "')"   #将节点名称以字符串 转成类似元组的形式，sql语句只接受元组形式

        table_data_result = select_pn_block_data(start_time,end_time,pn_type,source_node,dest_node)
        if not table_data_result:
            code = 0
            success = True
            status_message = ' 提示:未查询到相关数据.'
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)
        graphic_data = {}
        graphic_data_date = []
        value_max_duration = []
        value_total_duration = []
        value_number_of_times = []
        day_start_stamp, day_diff = get_dat_diff(start_time_str, end_time_str)  #获取开始时间的时间戳，开始时间至结束时间的天数差
        #得到时间范围内每一天专线不可用的数据值
        for i in range(0, day_diff):
            day_end_stamp = day_start_stamp + 86400
            day_date = time.strftime("%Y-%m-%d", time.localtime(day_start_stamp))
            #print(day_start_stamp, day_end_stamp,day_date)
            graphic_data_result = select_pn_block_data(day_start_stamp, day_end_stamp, pn_type, source_node, dest_node)
            day_start_stamp = day_end_stamp
            graphic_data_date.append(day_date)

            if graphic_data_result:
                value_max_duration.append(graphic_data_result[0]['max_duration'])
                value_total_duration.append(graphic_data_result[0]['total_duration'])
                value_number_of_times.append(graphic_data_result[0]['number_of_times'])
            else:
                value_max_duration.append(0)
                value_total_duration.append(0)
                value_number_of_times.append(0)
        graphic_data['graphic_data_date'] = graphic_data_date
        graphic_data['value_max_duration'] = value_max_duration
        graphic_data['value_total_duration'] = value_total_duration
        graphic_data['value_number_of_times'] = value_number_of_times

        status_message = 'succes'
        code = 0
        success = True
        body_dict['TableData'] = table_data_result
        body_dict['GraphicData'] = graphic_data
        result_json = to_json_result(code, status_message, success, body_dict)
        print(result_json)

        return HttpResponse(result_json)
    else:
        status_message = ' error : Please use post request.'
        result_json = to_json_result(code, status_message, success, data_list)
        return HttpResponse(result_json)

def  pn_delay_status(request):
    from src.lib import django_api
    django_api.DjangoApi().os_environ_update()
    data_list = []
    body_dict = {}
    body_dict['data'] = data_list
    # 定义默认的code和status值
    code = 500
    success = False
    if request.method == 'POST':  # 当提交表单时
        try:
            start_time = json.loads(request.body.decode()).get('start_time')
            end_time = json.loads(request.body.decode()).get('end_time')
            source_node = json.loads(request.body.decode()).get('source_node')
            node = json.loads(request.body.decode()).get('node')
        except Exception as e:
            print(e,'error: Failed to get transfer parameters.')
            status_message = " error : Failed to get transfer parameters."
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)
        #判断传入的源节点参数
        if source_node:
            if source_node not in ('aws','aliyun') and source_node != '':
                status_message = " error : source_node' value is incorrect."
                result_json = to_json_result(code, status_message, success, body_dict)
                return HttpResponse(result_json)
            elif source_node == '':
                source_node = 'aliyun'
        else:
            source_node = 'aliyun'

        if start_time and end_time :
            result_time = judge_time(start_time, end_time)
            if not result_time:
                status_message = " error :One of start_time and end_time value is incorrect."
                result_json = to_json_result(code, status_message, success, body_dict)
                return HttpResponse(result_json)
        else:
            status_message = ' error : start_time,end_time value cannot be empty.'
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)

        print('pn_node_list',pn_node_list)
        if node:
            if node == 'aws':
                node = 'aws-template-3'
            elif node == 'aliyun':
                node = 'opscloud-1'
            elif node not in pn_node_list:
                status_message = " error : node' value is incorrect. "
                result_json = to_json_result(code, status_message, success, body_dict)
                return HttpResponse(result_json)

        else:
            status_message = " error : node' value is empty. "
            result_json = to_json_result(code, status_message, success, body_dict)
            return HttpResponse(result_json)


        sql = "SELECT CAST(date AS CHAR ) AS date,source,PNnode,valueAvg,valueMax,value9999,value9995,value999,value99,value98 " \
              "FROM %s " \
              "where date BETWEEN '%s' AND '%s' AND source = '%s' AND PNnode = '%s';" % (
                pn_event_delay_data_table, start_time, end_time, source_node, node)
        print('sql',sql)

        try:
            mysql_conn_dict = db_mysql.MyPymysqlPoolDict()
            result = mysql_conn_dict.select(sql)
            print('sql_result',result)
        except Exception as e:
            status_message = 'error: Database query failed. '
            print(e,status_message)
        else:
            if not result:
                status_message = ' error : No eligible data.'
            else:
                date_list = []
                valueAvg_list = []
                valueMax_list = []
                value9999_list = []
                value9995_list = []
                value999_list = []
                value99_list = []
                value98_list = []

                value_all = {}
                for r in result:
                    date_list.append(r['date'])
                    valueAvg_list.append(r['valueAvg'])
                    valueMax_list.append(r['valueMax'])
                    value9999_list.append(r['value9999'])
                    value9995_list.append(r['value9995'])
                    value999_list.append(r['value999'])
                    value99_list.append(r['value99'])
                    value98_list.append(r['value98'])
                value_all['date'] = date_list
                value_all['valueAvg'] = valueAvg_list
                value_all['valueMax'] = valueMax_list
                value_all['value9999'] = value9999_list
                value_all['value9995'] = value9995_list
                value_all['value999'] = value999_list
                value_all['value99'] = value99_list
                value_all['value98'] = value98_list

                status_message = 'succes'
                code = 0
                success = True
                body_dict['StandardData'] = result
                body_dict['LineChartData'] = value_all
        finally:
            mysql_conn_dict.dispose()
            result_json = to_json_result(code, status_message, success, body_dict)
            print(result_json)
            return HttpResponse(result_json)
    else:
        status_message = ' error : Please use post request.'
        result_json = to_json_result(code, status_message, success, body_dict)
        return HttpResponse(result_json)

if __name__ == "__main__":
    pn_status('xxx')
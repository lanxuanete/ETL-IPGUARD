
import datetime
import pymssql
import pymysql
import os
import sys


def make_print_to_file(path):
    '''
    path， it is a path for save your log about fuction print
    example:
    use  make_print_to_file()   and the   all the information of funtion print , will be write in to a log file
    :return:
    '''
    class Logger(object):
        def __init__(self, filename="Default.log",path=path):
            self.terminal = sys.stdout
            self.log = open(os.path.join(path, filename), "a", encoding='utf8', )

        def write(self, message):
            self.terminal.write(message)
            self.log.write(message)

        def flush(self):
            pass

    fileName = datetime.datetime.now().strftime('day' + '%Y_%m_%d_%H_%M_%S')
    sys.stdout = Logger(fileName + '.log', path=path)

    print(fileName.center(60, '*'))



# Returns the set of all days in the period based on the start date, end date
def getDatesByTimes(start_day,end_day):
    result = []
    date_start = datetime.datetime.strptime(start_day, '%Y-%m-%d')+datetime.timedelta(days=1)
    date_end = datetime.datetime.strptime(end_day, '%Y-%m-%d')
    while date_start < date_end:
        result.append(date_start.strftime('%Y%m%d'))
        date_start += datetime.timedelta(days=1)
    print("函数getDatesByTimes()-介于开始日期与结束日期之间的日期列表：",result)
    return result



#Get the daily log table from sqlserver and aggregate it to a target table
def get_ipguard_udisk_log(conn_ms,conn_my):
    print('ipguard_d_udisk_log'.center(60, '-'))
    cursor_ms = conn_ms.cursor()
    cursor_my = conn_my.cursor()

    #获取目标表最大日期
    sql_date_max = "select max(UD_TIME) from ipguard_d_udisk_log "
    cursor_my.execute(sql_date_max)
    res_date_max = cursor_my.fetchall()
    if not res_date_max:
        return False

    # 获得需要插入的日期列表
    start_day= "".join('%s' %a for a in res_date_max)[0:10]
    end_day = datetime.date.today().strftime("%Y-%m-%d")
    print("开始日期：",start_day)
    print("结束日期：",end_day)
    datelist = getDatesByTimes(start_day, end_day)

    #循环插入
    for i in datelist:
        sql_ms = 'select ID,ROWGUID,UD_AGT_ID,UD_SES_ID,UD_USR_ID,UD_TIME,UD_DATE,UD_WEEK,UD_HOUR,UD_TYPE,UD_SUBTYPE,UD_UDISK_ID,UD_VOLUME_SN,UD_UDISK_ENCRYPT_TYPE,UD_DESC,UD_LABEL,UD_UDISK_SIZE,UD_ENCRYPT_TYPE,UD_FILE_TYPE,UD_SRC_NAME,UD_SRC_PATH,UD_SRC_DEVICE,UD_DEST_PATH,UD_DEST_DEVICE,UD_APP_HASH,UD_APP_NAME,UD_APP_TITLE,UD_FILE_TIME,UD_FILE_SIZE,UD_FILE_SIZE_HIGH,UD_IP_MAC,UD_PC_NAME,UD_USER_TYPE,UD_USER_NAME,UD_PRODUCT_ID,CURRENT_TIMESTAMP as ETL_TIME from [OCULAR3_DATA.%s].[dbo].[UDISK_LOG]' %(i)
        # print("sql_ms",sql_ms)
        cursor_ms.execute(sql_ms)
        res = cursor_ms.fetchall()
        if res == []:
            print(i, "无查询结果，继续执行下一日")
            continue
        elif not res:
            return False
        print(i,"查询成功")

        sql_my = 'insert into ipguard_udisk_log values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        # print('sql_my:',sql_my)
        try:
            cursor_my.executemany(sql_my, res)
            print(i, "已插入成功！")
        except pymysql.Error as e:
            print("错误是",e)
            conn_my.rollback()
            conn_my.close()
    conn_my.commit()


#Get the opration table from sqlserver and aggregate it to a target table
def get_it_r_udisk_opration(conn_ms,conn_my):
    print('it_r_udisk_oprations'.center(60, '-'))
    cursor_ms = conn_ms.cursor()
    cursor_my = conn_my.cursor()

    # 获取目标表最大日期
    sql_date_max = "select max(Opr_DATE) from it_r_udisk_oprations "
    cursor_my.execute(sql_date_max)
    res_date_max = cursor_my.fetchall()
    if not res_date_max:
        return False


    # 获得需要插入的日期列表
    start_day = "".join('%s' % a for a in res_date_max)
    end_day = datetime.date.today().strftime("%Y-%m-%d")
    print("开始日期：",start_day)
    print("结束日期：",end_day)
    datelist = getDatesByTimes(start_day, end_day)

    # 循环插入
    for i in datelist:
        sql_ms = 'select FORMAT(UD_DATE,\'####-##-##\') as Opr_Date,UD_AGT_ID as AgentID,UD_USR_ID as UserID,CONVERT(NVARCHAR,(case UD_SUBTYPE when 2 then \'复制入U盘\' when 3 then \'移动入U盘\' when 17 then \'从U盘复制出\' when 18 then \'从U盘移动出\' else \'其它\' end)) as Opr_Type,CONVERT(NVARCHAR,((case UD_SRC_DEVICE when 0 then \'无\' when 1 then \'硬盘\' when 4 then \'可移动盘\' when 5 then \'网络盘\' else \'其它\' end)+\'->\'+(case UD_DEST_DEVICE when 0 then \'无\' when 1 then \'硬盘\' when 4 then \'可移动盘\' when 5 then \'网络盘\' else \'其它\' end))) as DeviceType,count(*) as OprationNum,CURRENT_TIMESTAMP as ETL_TIME FROM [OCULAR3_DATA.%s].[dbo].[UDISK_LOG]where UD_SUBTYPE in (2,3,17,18) group by UD_DATE,UD_AGT_ID,UD_USR_ID,UD_SUBTYPE, UD_SRC_DEVICE,UD_DEST_DEVICE' %(i)
        # print("函数get_it_r_udisk_opration()", sql_ms)
        cursor_ms.execute(sql_ms)
        res = cursor_ms.fetchall()
        if res == []:
            print(i, "无查询结果，继续执行下一日")
            continue
        elif not res:
            return False
        # print(res)
        print(i,"查询成功")

        sql_my = 'insert into it_r_udisk_oprations values(%s,%s,%s,%s,%s,%s,%s)'
        # print('sql_my:',sql_my)
        try:
            cursor_my.executemany(sql_my, res)
            print(i, "已插入成功！")
        except pymysql.Error as e:
            print("错误是",e)
            conn_my.rollback()
            conn_my.close()
    conn_my.commit()



if __name__ == '__main__':
    conn_ms = pymssql.connect(host=,
                           user=,
                           password=,
                           database=,
                           charset='utf8'
                            )


    conn_my = pymysql.connect(host=,
                           user=,
                           password=,
                           database=,
                           charset='utf8')

    path = 'D:\log'
    make_print_to_file(path)#日志
    get_ipguard_udisk_log(conn_ms,conn_my)
    get_it_r_udisk_opration(conn_ms,conn_my)











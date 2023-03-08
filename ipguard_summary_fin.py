import os
import pymssql
import pymysql
import datetime

#Get yesterday's date and return it as a string in the format of yyyymmdd
def getYesterday():
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    yest = str(yesterday.year) + '{:02d}'.format(yesterday.month) + '{:02d}'.format(yesterday.day)
    return int(yest)

#Operation sqlserver wrapper class
class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            print("sqlserver连接成功")
            return cur

    # 执行sql语句
    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        res = cur.fetchall()
        self.conn.commit()
        self.conn.close()
        return res

    # 连接sqlserver，读取库名，返回已存在的日期列表
    def get_all_date(self):
        sql = 'SELECT right(name,8) FROM Master..SysDatabases WHERE name like \'OCULAR3_DATA.%\''  # 显示所有库名
        res = self.ExecQuery(sql)
        if not res:
            return False
        date_list = []
        for i in res:
            date = i[0]
            date_list.append(date)
        if not date_list:
            return False
        return date_list

    # 读取文件列表.mdf数据导入sqlserver中
    def Restore(self, path):
        # path = "D:/IPGuard_DB_BAK"
        filename = os.listdir(path)
        for i in filename:
            if os.path.splitext(i)[1] == '.MDF':
                try:
                    file_str = os.path.splitext(i)[0]  # mdf取文件名
                    #print(file_str)
                    Separate = file_str.split('.')  # 指定‘.’对字符串进行切片
                    #print(Separate)
                    # if Separate[1] in self.get_all_date():
                    #     raise(NameError,"该日期已存在")
                    # else:
                    str = '\'' + file_str + '\''
                    strmdf = '\'' + path + '\\' + file_str + '.MDF\''
                    strldf = '\'' + path + '\\' + file_str + '_Log.LDF\''
                    sql = 'exec sp_attach_db @dbname = %s, @filename1 = %s,@filename2 = %s' % (str, strmdf, strldf)
                    #print(sql)
                    cur = self.__GetConnect()
                    self.conn.autocommit(True)  # 指令立即执行，无需等待conn.commit()
                    cur.execute(sql)
                    print("sql执行成功")
                    self.conn.autocommit(False)  # 指令关闭立即执行，以后还是等待conn.commit()时再统一执行
                    self.conn.close()
                    print("插入成功", path, file_str)
                except:
                    print("该日期已存在：", i)
                    continue
        print("Restore完成")


#Operate mysql wrapper class
class Mysql(object):
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymysql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8",cursorclass=pymysql.cursors.DictCursor
)
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            print("mysql连接成功")
            return cur

    #执行sql语句
    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        res = cur.fetchall()
        #print(res)
        self.conn.commit()
        self.conn.close()
        return res

    #输入表名，返回已存在的日期列表
    def get_all_date(self):

        sql = "select distinct UD_DATE from ipguard_udisk_log "  # 显示所有日期
        res = self.ExecQuery(sql)
        if not res:
            return False
        date_list = []
        for i in res:
            date = i['UD_DATE']
            date_list.append(date)
        if not date_list:
            return False
        return date_list

    # 批量插入
    def insert(self,sql,data):
        cur = self.__GetConnect()
        try:
            cur.executemany(sql, data)
        except pymysql.Error as e:
            print("错误是",e)
            self.conn.rollback()
            self.conn.close()
        self.conn.commit()
        self.conn.close()


if __name__ == '__main__':
    """
        1. restore the .mdf.ldf file in the folder to SqlServer. if the date database already exists in Sqlserver, then skip it.
        2. Extract all data from the [UDISK_LOG] table and insert it into the staging.ipguard_udisk_log
    """
    #还原
    ms = MSSQL(host='127.0.0.1', user="data_reader", pwd="root123456!", db='OCULAR3_DATA')
    path = "D:\IPGuard_DB_BAK" #.mdf文件所在路径
    ms.Restore(path)

    #抽到staging
    my = Mysql(host='127.0.0.1', user='tmp_write', pwd='Ntruy9pe5cvkSu@', db='staging')
    datalist_my=my.get_all_date()#类型是list int
    datalist_ms=ms.get_all_date()#类型是list str
    for i in datalist_ms:
        try:
            if int(i) in datalist_my:
                raise (NameError, "该日期日志信息已存在")
            else:
                sql = 'select * from [OCULAR3_DATA.%s].[dbo].[UDISK_LOG]'%(i)
                data = ms.ExecQuery(sql)
                #print("data",data)
                sqlforinsert = 'insert into ipguard_udisk_log values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                my.insert(sqlforinsert,data)
                print("数据插入成功！")
        except:
            print("该日期已存在：",i)
            continue










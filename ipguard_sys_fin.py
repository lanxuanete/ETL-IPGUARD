import pymssql
import pymysql


#update two tables:ipguard_agent and ipguard_user
def update(tablename_src,tablename_desc):
    conn_ms = pymssql.connect(host='172.16.50.31:61161',
                           user='data_reader',
                           password='zhenge$123456',
                           database='OCULAR3',
                           charset='utf8')
    cursor_ms = conn_ms.cursor()
    sql_select = 'select *,CURRENT_TIMESTAMP as "ETL_TIME" from [OCULAR3].[dbo].[%s]' %(tablename_src)
    print("sql_select:",sql_select)
    cursor_ms.execute(sql_select)
    res_ms = cursor_ms.fetchall()
    if not res_ms:
        return False
    print(res_ms)

    conn_my = pymysql.connect(host='rm-uf665o948h129h2vn7o.mysql.rds.aliyuncs.com',
                           user='tmp_write',
                           password='Ntruy9pe5cvkSu@',
                           database='staging',
                           charset='utf8')
    cursor_my = conn_my.cursor()
    sql_trun='truncate table %s'%(tablename_desc)
    print("sql_trun:",sql_trun)
    cursor_my.execute(sql_trun)
    print("清空成功")
    if tablename_src=='AGENT':
        sql_insert = 'insert into ipguard_agent values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    elif tablename_src=="USER":
        sql_insert = 'insert into ipguard_user values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    print("sql_insert:",sql_insert)
    try:
        cursor_my.executemany(sql_insert, res_ms)
        print("插入成功")
    except pymysql.Error as e:
        print("错误是",e)
        conn_my.rollback()
        conn_my.close()
    conn_my.commit()
    conn_my.close()

if __name__ == '__main__':
    # Mettre à jour la table l'ordinateur, AGENT est la table source, ipguard_agent est la table cible
    update('AGENT','ipguard_agent')
    # Mettre à jour la table d'utilisateurs, USER est la table source, ipguard_user est la table cible
    update('USER','ipguard_user')
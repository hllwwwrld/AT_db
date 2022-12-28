import psycopg2
from db_conn import conn_wfm, conn_intgr


#  запуск sql скрипта/запроса
def run_sql_wfm(sql_request):
    cursor = conn_wfm().cursor()
    cursor.execute(sql_request)
    return cursor.fetchall()


def run_sql_pbd(sql_request):
    cursor = conn_intgr().cursor()
    cursor.execute(sql_request)
    return cursor.fetchall()


# получение последней записи импорта в БД
def get_max_of_integrationcallresult():
    sql_request = """
    select i.id, i.success
    from integrationcallresult i
    order by i.id desc
    limit 1
    """  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте

    max_of_integrationcallresult = run_sql_wfm(sql_request)
    return max_of_integrationcallresult

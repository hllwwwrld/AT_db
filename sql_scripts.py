import datetime

from db_conn import conn_wfm, conn_intgr


#  запуск sql скрипта/запроса в ВФМ
def run_sql_wfm(sql_request):
    cursor = conn_wfm().cursor()
    cursor.execute(sql_request)
    return cursor.fetchall()


#  запуск sql скрипта/запроса в ПБД
def run_sql_pbd(sql_request):
    cursor = conn_intgr().cursor()
    cursor.execute(sql_request)
    return cursor.fetchall()


# получение последней записи импорта в БД ВФМ
def get_max_of_integrationcallresult():
    sql_request = """
    select i.id, i.success
    from integrationcallresult i
    order by i.id desc
    limit 1
    """  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = run_sql_wfm(sql_request)
    return max_of_integrationcallresult


# поиск оргЮнита по параметрам в ВФМ
def find_org_unit_in_wfm(unic_id, with_main_org_unit):
    if with_main_org_unit:
        main_org_unit_filter = "and o3.outerid = 'MainOrgUnit'"
    else:
        main_org_unit_filter = ''

    sql_request = """
                select * from organizationunit o
                join organizationunittype o2
                on o2.id = o.organizationunittype_id
                join organizationunit o3
                on o.parent_id = o3.id
                where o.outerid = '{0}'
                and o.name = '{0}'
                and o.datefrom = '1970-01-01'
                and o2.outer_id = '5'
                {1}
                """.format(unic_id, main_org_unit_filter)
    res = run_sql_wfm(sql_request)  # получаем оргЮнит по параметрам из запроса
    return res


def is_import_res_succes(import_id):
    sql_request = """
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(import_id)
    res = run_sql_wfm(sql_request)
    return res


def find_ep_in_wfm(unic_id, closed, is_valid_org_unit):
    if closed:
        close_date = datetime.date.today()
    else:
        close_date = 'is null'
    if is_valid_org_unit:
        org_unit_id = "and o.outerid = 'b471e2cd-9af3-11e8-80da-42f2e9dc7849'"
    else:
        org_unit_id = ''

    sql_request = """
                select * from employeeposition ep
                join position p
                on p.id = ep.position_id
                join employee e
                on e.id = ep.employee_id
                join organizationunit o
                on o.id = p.organizationunit_id
                where ep.card_number = '{0}'
                and ep.startdate = '2021-01-01'
                and ep.enddate {1}
                and e.outerid = '{0}'
                and e.lastname = '{0}'
                and p.outerid = '{0}'
                {2}
                """.format(unic_id, close_date, org_unit_id)
    res = run_sql_wfm(sql_request)
    return res


def find_sr_in_wfm(unic_id, is_ep_valid):
    if is_ep_valid:
        employee_outer_id = "and e.outerid = '{0}'".format(unic_id)
        position_outer_id = "and p.outerid = '{0}'".format(unic_id)
    else:
        employee_outer_id = ''
        position_outer_id = ''
    sql_request = f"""
                select * from schedule_request sr
                join employee e
                on e.id = sr.employee_id
                join position p
                on p.id = sr.position_id
                join schedule_request_alias sra
                on sra.id = sr.alias_id
                where sr.startdatetime = '{str(datetime.date.today().replace(day=1))} 00:00:00.000'
                and sr.enddatetime = '{str(datetime.date.today().replace(day=10))} 23:59:00.000'
                {employee_outer_id}
                {position_outer_id}
                and sra.outer_id = '750873459435863200'
                """
    return run_sql_wfm(sql_request)


def find_removed_entity_in_wfm(unic_id, data_type):
    if data_type == 'EMPLOYEE_POSITION':
        start_date = "'2021-01-01'"
        end_date = "is null"
    else:
        start_date = datetime.date.today().replace(day=1)
        end_date = f'= {datetime.date.today().replace(day=10)}'

    sql_request = """
                select * from log_jremoved
                where employee_outer_id = '{0}'
                and position_outer_id = '{0}'
                and start_date = {1}
                and end_date {2}
                and type = '{3}'
                """.format(unic_id, start_date, end_date, data_type)
    return run_sql_wfm(sql_request)
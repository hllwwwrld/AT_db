import psycopg2
import requests
from datetime import timedelta
import datetime


# ПОДКЛЮЧЕНИЕ К БД ВФМ
def connIntgr():
    return psycopg2.connect(database="outside_integration_t",
                            user="a_kosolapov",
                            password="TcmYez0uzXgK9",
                            host="stage-t-db.goodt.me",
                            port="5432")


# ПОДКЛЮЧЕНИЕ К ПБД
def connWfm():
    return psycopg2.connect(database="magnit_birzha_test",
                            user="a_kosolapov",
                            password="TcmYez0uzXgK9",
                            host="stage-t-db.goodt.me",
                            port="5432")


def clearDBbeforeImport(formatTT):
    cursor = connIntgr().cursor()
    cursor.execute('''delete from shift_extended 
    where period = '{0}'
    and org_unit_format_tt in ('{1}')'''.format(findPeriod(), "', '".join(formatTT)))


# Прогружает в БД ВФМ запрос по поску смен
def findShiftsInWfm(formatTT, updatedFrom):  # получает как параметры - ПП и дату, с которой отбираются изменения
    first = "'" + "', '".join(formatTT) + "'"  # полученный список ПП перевожу в строку
    cursor = connWfm().cursor()
    cursor.execute(
        """select * from shift s
 join employeeposition e on e.id = s.employee_position_id
 join positioncategoryroster p on s.pos_cat_roster_id = p.id
 join roster r on p.roster_id = r.id
 join organizationunit o on r.org_unit_id = o.id
 join entity_property_org_unit epou on o.id = epou.org_unit_id
 join entity_property_value epv on epv.id = epou.value_id
 join entity_property ep on ep.id = epou.property_id
where epv.str_value in ({0})
 and ep.property_key ='org_unit_format'
 and s.startdatetime >= '2022-09-30 00:00:00.000'
 and s.startdatetime < '2022-11-01 00:00:00.000'
 --and s.worked_status = 'APPROVED'
 and s.worked_status in ('APPROVED', 'NOT_APPROVED')
 --and s.worked_status in ('NOT_APPROVED')
 and e.temporary = false
 and s.extended_status is null
 and s.updated >= '{1}'
 --and s.updated <= '2022-09-29 15:50:00.000'
 and o.active = true
 and o.availableforcalculation = true
order by s.id desc;""".format(first, updatedFrom))  # на место ПП вставляю свою ПП и дату, от которой нужно обнолвять
    res = cursor.fetchall()
    return res


def findUpdatedFrom(formatTT):
    shiftAmount = int(input('Введите макс кол-во смен в выборке: '))
    updatedFrom = datetime.date.today().replace(day=1)  # дата отбора изменний первый день текущего месяца
    res = findShiftsInWfm(formatTT, updatedFrom)
    print(updatedFrom, len(res))
    while len(res) > shiftAmount:
        updatedFrom += timedelta(days=1)
        res = findShiftsInWfm(formatTT, updatedFrom)
        print(updatedFrom, len(res))
    return updatedFrom


def findPeriod():
    period = datetime.date.today().replace(day=1)
    return period


def importFromWfm():
    formatTT = [i for i in input('Введите ПП в формате ММ МК: ').split()]
    url = 'https://magnit-integration.t.goodt.me/export/gendalf/timesheet'
    res = findUpdatedFrom(formatTT)
    clearDBbeforeImport(formatTT)
    queryParams = {'format-tt': ', '.join(formatTT), 'period': findPeriod(), 'updated-from': f'{res}T00:00'}
    imprtRes = requests.get(url, params=queryParams, auth=('superuser', 'qwe'))
    print(', '.join(formatTT), f'{res}T00:00 - Параметры выгрузки')
    return imprtRes.status_code


importFromWfm()

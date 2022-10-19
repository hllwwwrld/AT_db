import psycopg2
import pytest
import allure
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

def test_0():
    cursor = connIntgr().cursor()
    cursor.execute(
        "select tab_id, ep_fullname from shift_extended limit 2"
    )
    res = cursor.fetchall()
    for i in res:
        print(f'tab_id = {i[0]}')
        print(f'ep_fullname = {i[1]}')
    return res


#  1 - Дубли смен
def test_1():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select ep_guid, startdatetime, enddatetime, count(*)
from shift_extended se
where "type"  != 'SCHEDULE_REQUEST'
group by ep_guid, startdatetime, enddatetime
having count(*)>1"""
    )
    res = cursor.fetchall()
    assert len(res) == 0


#  2- дубли отсутствий
def test_2():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select ep_fullname, ep_guid, "date", count(*)
from shift_extended se
where "type" = 'SCHEDULE_REQUEST'
group by ep_fullname, ep_guid, "date"
having count(*)>1"""
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 3 - Проверка смены и отсутствия на один день
def test_3():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select se.ep_guid, se."date", se."type"
from shift_extended se
join shift_extended se2
on se.tab_id = se2.tab_id
where se.ep_guid = se2.ep_guid
and se."date" = se2."date"
and se."type" != se2."type"""
    )
    res = cursor.fetchall()
    assert len(res) == 0


#  4 - Нет факт смен за будущий период
def test_4():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select * from shift_extended se
where "type" != 'SCHEDULE_REQUEST'
and planned != true
and "date" >= '{0}'""".format(datetime.date.today())
    )
    res = cursor.fetchall()
    assert len(res) == 0


#  5 - Нет план смен за прошедший период
def test_5():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select * from shift_extended se
where "type" != 'SCHEDULE_REQUEST'
and planned = true
and "date" <= '{0}'
order by ep_guid""".format(datetime.date.today())
    )
    res = cursor.fetchall()
    assert len(res) == 0


#  6 - Дубли смен
def test_6():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select distinct se.tab_id, se2.tab_id, se."date", se.ep_guid
from shift_extended se
join shift_extended se2
on se.wfm_id = se2.wfm_id
where se.tab_id != se2.tab_id
and se.ep_guid = se2.ep_guid
and se."date" = se2."date"
and se.startdatetime = se2.startdatetime
and se.enddatetime = se2.enddatetime
and se."period" = '{0}';""".format(datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


#  7 - Целевой. Проверка отсутсвтие дублей (отсутствие + смена)
def test_7():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select se.ep_guid, se."date", se.tab_id, se2. tab_id, se.updated from shift_extended se
join shift_extended se2 on se.ep_guid = se2.ep_guid
where se.tab_id != se2.tab_id
and se."date" = se2."date"
and se."type" != se2."type"
and se."date" >= '{0}'
and se.removed is null
and se2.removed is null
and se."period" = '{1}'
and se2."period" = '{2}'
and se."type" = 'SCHEDULE_REQUEST'
order by ep_guid""".format(datetime.date.today(), datetime.date.today().replace(day=1), datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 8 - Целевой табель. Проверка что нет дублей смен с разными wfm_id
def test_8():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select se1.tab_id, se2.tab_id, se1.ep_guid, se1."date"
--delete
from shift_extended se1, shift_extended se2
where  se1.tab_id != se2.tab_id
and se1."period" = '{0}'
--and se1.wfm_id =se2.wfm_id
and se1.ep_guid = se2.ep_guid
and se1."date" = se2."date"
and se1.startdatetime = se2.startdatetime
and se1.enddatetime = se2.enddatetime;""".format(datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 9 - Целевой табель. Проверка что нет дублей смен с разными датами окончания
def test_9():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select se.tab_id, se2.tab_id, se."date", se.ep_guid
from shift_extended se
join shift_extended se2 on se."date" = se2."date"
where se.tab_id != se2.tab_id
and se.ep_guid = se2.ep_guid
and se."date" = se2."date"
and se.startdatetime = se2.startdatetime
and se.enddatetime != se2.enddatetime
and se."period" ='{0}';""".format(datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 10 - Целевой табель. Проверка что нет дублей смен с разными датами начала
def test_10():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select se.tab_id, se2.tab_id, se."date", se.ep_guid
from shift_extended se
join shift_extended se2 on se."date" =se2."date"
where se.tab_id != se2.tab_id
and se.ep_guid = se2.ep_guid
and se."date" = se2."date"
and se.startdatetime != se2.startdatetime
and se.enddatetime = se2.enddatetime
and se."period" ='{0}';""".format(datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 11 - Целевой. Проверка дублей с одинаковыми wfm_id
def test_11():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select se.tab_id, se2.tab_id, se."date", se.ep_guid
from shift_extended se
join shift_extended se2  on se.wfm_id =se2.wfm_id
where se.tab_id != se2.tab_id
and se.ep_guid = se2.ep_guid
and se."date" = se2."date"
and se.startdatetime = se2.startdatetime
and se."period" = '{0}';""".format(datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 12 - Целевой. Проверка дублей cмен
def test_12():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select ep_guid, "date", startdatetime, enddatetime, org_unit_name,  count(1) as "count"
from shift_extended se
where "period" ='{0}'
and  "type" ='SHIFT'
group by  "date", ep_guid, startdatetime, enddatetime, org_unit_name
having count(*)>1;""".format(datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 13 - Целевой. Проверка отсутствия дублей на дату по отсутствиям (schedule_request)
def test_13():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select ep_guid, ep_fullname, "date", org_unit_name, count(1) as "count"
from shift_extended se
where "period" = '{0}'
--and removed is null
and "type" ='SCHEDULE_REQUEST'
group by  ep_guid, "date", ep_fullname, org_unit_name
having count(*)>1
order by org_unit_name;""".format(datetime.date.today().replace(day=1))
    )
    res = cursor.fetchall()
    assert len(res) == 0


# 14 - Проверка смен на корректность подсчета часов
def test_14():
    cursor = connIntgr().cursor()
    cursor.execute(
        """select startdatetime, enddatetime, hours from shift_extended se 
where "type" = 'SHIFT'
and DATE_PART('hour', enddatetime - startdatetime) + date_part('minute', enddatetime - startdatetime)/60 != hours 
and enddatetime::text not like '%23:59%'
group by startdatetime, enddatetime, hours"""
    )
    res = cursor.fetchall()
    assert len(res) == 0
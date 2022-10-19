import pytest
import psycopg2
import allure
import requests


def importFromWfm():
    session = requests.session()
    session.auth = ('superuser', 'qwe')
    url = 'https://magnit-integration.t.goodt.me/export/gendalf/timesheet'
    queryParams = {'format-tt': 'МБ', 'period': '2022-09-01', 'updated-from': '2022-09-20T00:00'}
    imprtRes = requests.get(url, params=queryParams, auth=session.auth)
    print(imprtRes.status_code)

@pytest.fixture()
def conn():
    return psycopg2.connect(database="outside_integration_t",
                            user="a_kosolapov",
                            password="TcmYez0uzXgK9",
                            host="stage-t-db.goodt.me",
                            port="5432")


def test_1(conn):
    cursor = conn.cursor()
    cursor.execute("""
    select startdatetime, enddatetime, hours from shift_extended se 
where "type" = 'SHIFT'
and DATE_PART('hour', enddatetime - startdatetime) + date_part('minute', enddatetime - startdatetime)/60 != hours 
and enddatetime::text not like '%23:59%'
group by startdatetime, enddatetime, hours """)
    rows = cursor.fetchall()
    assert len(rows) == 0

import datetime
import json
import requests
import random
import psycopg2


# ПОДКЛЮЧЕНИЕ К ПБД
def connIntgr():
    return psycopg2.connect(database="outside_integration_t",
                            user="a_kosolapov",
                            password="TcmYez0uzXgK9",
                            host="stage-t-db.goodt.me",
                            port="5432")


# ПОДКЛЮЧЕНИЕ К БД ВФМ
def connWfm():
    return psycopg2.connect(database="magnit_wfm_qa",
                            user="a_kosolapov",
                            password="TcmYez0uzXgK9",
                            host="stage-t-db.goodt.me",
                            port="5432")


def createUnicId():
    someSymbols = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    someSymbols = ' '.join(someSymbols)
    someSymbols = someSymbols.split()
    res = random.sample(someSymbols, 5)
    res = ''.join(res)
    return res


wfm_url = 'https://magnitqa-wfm.goodt.me'


def createOrgUnit(isMainOrgUnitValid):
    if isMainOrgUnitValid:
        mainOrgUnitId = 'MainOrgUnit'
    elif isMainOrgUnitValid is False:
        mainOrgUnitId = 'amogus'

    unicId = createUnicId()
    url = '{0}/api/v1/integration-json/org-units'.format(wfm_url)
    jsonString = [
        {
            "outerId": unicId,
            "active": True,
            "availableForCalculation": None,
            "dateFrom": "1970-01-01",
            "name": unicId,
            "organizationUnitTypeOuterId": 5,
            "parentOuterId": mainOrgUnitId,
            "zoneId": None,
            "properties": None
        }
    ]
    body = json.dumps(jsonString)

    imprtRes = requests.post(url, data=body, auth=('superuser', 'Wsxzaq1!'))

    if imprtRes.status_code == 200:
        return unicId, imprtRes.text
    else:
        print('Ошибка создания оргЮнита - {0}'.format(imprtRes.text))
        return 'amogus', imprtRes.text


def createEmployeePosition(closed, isOrgUnitValid):
    if closed:
        endDate = str(datetime.date.today())
    else:
        endDate = None

    if isOrgUnitValid:
        orgUnitId = "b471e2cd-9af3-11e8-80da-42f2e9dc7849"
    else:
        orgUnitId = 'amogus'

    unicId = createUnicId()
    url = '{0}/api/v1/integration-json/employee-positions-full'.format(wfm_url)
    jsonString = [
        {
            "startWorkDate": "2021-01-01",
            "endWorkDate": endDate,
            "number": unicId,
            "employee": {
                "outerId": unicId,
                "firstName": "Владимир",
                "lastName": unicId,
                "patronymicName": "Ильич",
                "birthDay": "2000-10-20",
                "gender": "MALE",
                "startDate": "2022-03-16",
                "endWorkDate": None,
                "properties": {
                    "КисКод": "CN-0104291"
                }
            },
            "position": {
                "name": "Продавец",
                "outerId": unicId,
                "chief": None,
                "organizationUnit": {
                    "outerId": orgUnitId
                },
                "positionType": {
                    "outerId": "Продавец"
                },
                "positionGroup": {
                    "name": "РП ТЗ"
                },
                "positionCategory": {
                    "outerId": "_12b5312e-d3c3-48b2-88bc-81758457baf3"
                },
                "startWorkDate": "2021-10-05",
                "endWorkDate": None,
                "properties": None
            },
            "rate": None
        }
    ]
    body = json.dumps(jsonString)
    queryParams = {'concrete-dates': 'true', 'process-shifts': 'delete', 'start-date-shift-filter': 'true'}
    imprtRes = requests.post(url, data=body, params=queryParams, auth=('superuser', 'Wsxzaq1!'))

    if imprtRes.status_code == 200:
        return unicId, imprtRes.text
    else:
        print('Ошибка создания сотрудника - {0}'.format(imprtRes.text))
        return unicId, imprtRes.text


def createScheduleRequest(isEpValid):
    closed = False
    isOrgUnitValid = True
    if isEpValid:
        unicId, temp = createEmployeePosition(closed, isOrgUnitValid)
    else:
        unicId = 'amogus2'
    startDate = str(datetime.date.today().replace(day=1))
    endDate = str(datetime.date.today().replace(day=10))
    url = '{0}/api/v1/integration-json/schedule-requests'.format(wfm_url)
    queryParams = {'stop-on-error': 'false', 'delete-intersections': "true", 'split-requests': 'true', 'process-shifts': 'delete', 'start-date-shift-filter': 'true'}
    jsonString = [
        {
            "employeeOuterId": unicId,
            "positionOuterId": unicId,
            "type": "750873459435863200",
            "startDate": startDate,
            "startTime": "00:00",
            "endDate": endDate,
            "endTime": "23:59"
        }
    ]
    body = json.dumps(jsonString)
    imprtRes = requests.post(url, data=body, params=queryParams, auth=('superuser', 'Wsxzaq1!'))
    if imprtRes.status_code == 200:
        return unicId, imprtRes.text
    else:
        print('Ошибка создания отсутствия - {0}'.format(unicId))
        return unicId, imprtRes.text


def create_removed_entity(unicId, dataType):
    if dataType == 'EMPLOYEE_POSITION':
        startDate = '2021-01-01'
        endDate = None
    elif dataType == 'SCHEDULE_REQUEST':
        startDate = str(datetime.date.today().replace(day=1))
        endDate = str(datetime.date.today().replace(day=10))
    url = '{0}/api/v1/integration-json/removed'.format(wfm_url)
    queryParams = {'stop-on-error': 'false', 'open-prev-employee-position': 'false'}
    jsonString = [
        {
            "employeeOuterId": unicId,
            "positionOuterId": unicId,
            "type": dataType,
            "startDate": startDate,
            "endDate": endDate
        }
    ]
    body = json.dumps(jsonString)
    imprtRes = requests.post(url, data=body, params=queryParams, auth=('superuser', 'Wsxzaq1!'))
    if imprtRes.status_code == 200:
        return imprtRes.text
    else:
        print('Ошибка создания записи удаления - статус запроса - {0}'.format(imprtRes.status_code))
        return imprtRes.text


# IntegrRegress - 1 - Создания оргЮнита напрямую в ВФМ
def test_orgUnit():
    validTestFlag = True
    isMainOrgUnitValid = True
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createOrgUnit(isMainOrgUnitValid)

    if '"success":true' in imprtRes and '"callType":"ORGANIZATION_UNIT_IMPORT"' in imprtRes:  # шаг 1 снова получаем самую новую запись импорта в таблице
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 новая запись импорта - успешно
                cursor.execute("""
                select * from organizationunit o
                join organizationunittype o2
                on o2.id = o.organizationunittype_id
                join organizationunit o3
                on o.parent_id = o3.id
                where o.outerid = '{0}'
                and o.name = '{0}'
                and o.datefrom = '1970-01-01'
                and o2.outer_id = '5'
                and o3.outerid = 'MainOrgUnit'
                """.format(unicId))
                res = cursor.fetchall()  # получаем оргЮнит по параметрам из запроса

                if len(res) == 1:  # в БД создан оргЮнит по заданным параметрам
                    pass

                else:
                    validTestFlag = False
                    print('1')
            else:
                validTestFlag = False
                print('2')
        else:
            validTestFlag = False
            print('3')
            print(max_of_integrationcallresult[0][0] + 1, max_of_integrationcallresult_2[0][0])
    else:
        validTestFlag = False
        print('4')
        print(imprtRes)

    assert (validTestFlag is True)


# IntegrRegress - 2 - Создание ошибочной записи оргЮнита при импорте напрямую в ВФМ (с привязкой к несуществующему вышестоящему подразделению)
def test_orgUnit_error():
    validTestFlag = True
    isMainOrgUnitValid = False
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createOrgUnit(isMainOrgUnitValid)

    if '"logref":"error"' in imprtRes:  # шаг 1 - верный ответ на запрос
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 - если сформирована новая запись импорта

            if max_of_integrationcallresult_2[0][1] is False:  # шаг 3 - если сформированная запись неуспешна
                cursor.execute("""
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(max_of_integrationcallresult_2[0][0]))
                res = cursor.fetchall()  # получаем запись с месседжем об ошибке по нашей записи импорта

                if res[0][0] == 'Parent Org Unit of outer id {0} was not found Stop-on-error is false'.format(unicId):  # шаг 4 если верный месседж в ошибке
                    cursor.execute("""
                    select * from organizationunit o
                    join organizationunittype o2
                    on o2.id = o.organizationunittype_id
                    join organizationunit o3
                    on o.parent_id = o3.id
                    where o.outerid = '{0}'
                    and o.name = '{0}'
                    and o.datefrom = '1970-01-01'
                    and o2.outer_id = '5'
                    """.format(unicId))
                    res = cursor.fetchall()  # ищем оргЮнит в Бд по указанным параметрам

                    if len(res) == 0:  # шаг 5 ошибочный оргЮнит не создан в БД
                        pass

                    else:
                        validTestFlag = False
                        print(1)
                else:
                    validTestFlag = False
                    print(2)
            else:
                validTestFlag = False
                print(3)
        else:
            validTestFlag = False
            print(4)
    else:
        validTestFlag = False
        print(5)

    assert validTestFlag is True


# IntegrRegress - 3 - Создание нового сотрудника и назначения напрямую в ВФМ
def test_create_ep():
    validTestFlag = True
    closed = False  # назначение открыто
    isValidOrgUnit = True  # указан валидный оргЮнит
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createEmployeePosition(closed, isValidOrgUnit)

    if '"success":true' in imprtRes and '"callType":"EMPLOYEE_POSITION_FULL_IMPORT"' in imprtRes:  # шаг 1 - если получен верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 - если сформирована запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # если запись импорта - успешно
                cursor.execute("""
                select * from employeeposition ep
                join position p
                on p.id = ep.position_id
                join employee e
                on e.id = ep.employee_id
                join organizationunit o
                on o.id = p.organizationunit_id
                where ep.card_number = '{0}'
                and ep.startdate = '2021-01-01'
                and ep.enddate is null
                and e.outerid = '{0}'
                and e.lastname = '{0}'
                and p.outerid = '{0}'
                and o.outerid = 'b471e2cd-9af3-11e8-80da-42f2e9dc7849'
                """.format(unicId))
                res = cursor.fetchall()  # ищем нашего сотрудника в БД по параметрам

                if len(res) == 1:  # если сотрудник сформирован в ПБД
                    pass

                else:
                    validTestFlag = False
            else:
                validTestFlag = False
        else:
            validTestFlag = False
    else:
        validTestFlag = False

    assert validTestFlag is True


# IntegrRegress - 4 - Закрытие существующего назначения напрямую в ВФМ
def test_close_ep():
    validTestFlag = True
    closed = True  # создание закрытого назначения
    isValidOrgUnit = True  # указан валидный ОЮ
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createEmployeePosition(closed, isValidOrgUnit)

    if '"success":true' in imprtRes and '"callType":"EMPLOYEE_POSITION_FULL_IMPORT"' in imprtRes:  # шаг 1-получен верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2-создана запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 - запись импорта - успешно
                cursor.execute("""
                select * from employeeposition ep
                join position p
                on p.id = ep.position_id
                join employee e
                on e.id = ep.employee_id
                join organizationunit o
                on o.id = p.organizationunit_id
                where ep.card_number = '{0}'
                and ep.startdate = '2021-01-01'
                and ep.enddate = '{1}'
                and e.outerid = '{0}'
                and e.lastname = '{0}'
                and p.outerid = '{0}'
                and o.outerid = 'b471e2cd-9af3-11e8-80da-42f2e9dc7849'
                """.format(unicId, str(datetime.date.today())))
                res = cursor.fetchall()  # ищем наше закрытое назначение в БД по параметрам

                if len(res) == 1:  # шаг 4 - создано закрытое назначение в БД
                    pass

                else:
                    validTestFlag = False
            else:
                validTestFlag = False
        else:
            validTestFlag = False
    else:
        validTestFlag = False

    assert validTestFlag is True


# IntegrRegress - 5 - Создание нового сотрудника и назначения, когда в ВФМ нет оргЮнита, для которого импортируется сотрудник напрямую в ВФМ
def test_create_EP_without_orgUnit():
    validTestFlag = True
    closed = False  # создание открытого назначения
    isValidOrgUnit = False  # указан не валидный ОЮ
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createEmployeePosition(closed, isValidOrgUnit)

    if '"success":false' in imprtRes and '"callType":"EMPLOYEE_POSITION_FULL_IMPORT"' in imprtRes:  # шаг 1 - верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана запись импорта

            if max_of_integrationcallresult_2[0][1] is False:
                cursor.execute("""
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(max_of_integrationcallresult_2[0][0]))
                res = cursor.fetchall()  # для нашей записи импорта запоминаем месседж об ошибке
                print(res[0][0])

                if res[0][0] == 'Organization Unit of outer id amogus was not found'.format(unicId):  # шаг 3 верный месседж об ошибке
                    cursor.execute("""
                    select * from employeeposition ep
                    join position p
                    on p.id = ep.position_id
                    join employee e
                    on e.id = ep.employee_id
                    join organizationunit o
                    on o.id = p.organizationunit_id
                    where ep.card_number = '{0}'
                    and ep.startdate = '2021-01-01'
                    and ep.enddate is null
                    and e.outerid = '{0}'
                    and e.lastname = '{0}'
                    and p.outerid = '{0}'
                    """.format(unicId, datetime.date.today()))
                    res = cursor.fetchall()  # ищем ошибочное назначение по параметрам в БД

                if len(res) == 0:  # в бд нет назначением по отправленным параметрам
                    pass

                else:
                    validTestFlag = False
                    print(1)
            else:
                validTestFlag = False
                print(2)
        else:
            validTestFlag = False
            print(3)
    else:
        validTestFlag = False
        print(4)

    assert validTestFlag is True


# IntegrRegress - 6 - Создание нового отсутствия напрямую в ВФМ
def test_create_schedule_request():
    validTestFlag = True
    isEpValid = True
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createScheduleRequest(isEpValid)

    if '"success":true' in imprtRes and '"callType":"SCHEDULE_REQUESTS_IMPORT"' in imprtRes:  # шаг 1 - верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 2 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 если запись импорта - успешно
                cursor.execute("""
                select * from schedule_request sr
                join employee e
                on e.id = sr.employee_id
                join position p
                on p.id = sr.position_id
                join schedule_request_alias sra
                on sra.id = sr.alias_id
                where sr.startdatetime = '{1} 00:00:00.000'
                and sr.enddatetime = '{2} 23:59:00.000'
                and e.outerid = '{0}'
                and p.outerid = '{0}'
                and sra.outer_id = '750873459435863200'
                    """.format(unicId, str(datetime.date.today().replace(day=1)), str(datetime.date.today().replace(day=10))))
                res = cursor.fetchall()  # ищем наше отсутствие в БД по параметрам

                if len(res) == 1:  # шаг 4 если отсутствие создано в БД
                    pass

                else:
                    validTestFlag = False
                    print(1)
            else:
                validTestFlag = False
                print(2)
        else:
            validTestFlag = False
            print(3)
    else:
        validTestFlag = False
        print(4)

    assert validTestFlag is True


# IntegrRegress - 7 - Создание нового отсутствия с привязкой к назначению, которого нет в ВФМ, прямой импорт в ВФМ
def test_create_sr_with_invalid_EP():
    isValidTest = True
    isEpValid = False
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createScheduleRequest(isEpValid)

    if '"success":false' in imprtRes and '"callType":"SCHEDULE_REQUESTS_IMPORT"' in imprtRes:  # шаг 1 - верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is False:  # шаг 3 новая запись импорта - не успешно
                cursor.execute("""
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(max_of_integrationcallresult_2[0][0]))
                res = cursor.fetchall()  # для нашей записи импорта запоминаем месседж об ошибке

                if res[0][0] == 'Position of the outer id {0} was not found'.format(unicId):  # шаг 4 верный месседж об ошибке
                    cursor.execute("""select * from schedule_request sr
                    join employee e
                    on e.id = sr.employee_id
                    join position p
                    on p.id = sr.position_id
                    join schedule_request_alias sra
                    on sra.id = sr.alias_id
                    where sr.startdatetime = '{1} 00:00:00.000'
                    and sr.enddatetime = '{2} 23:59:00.000'
                    and e.outerid = '{0}'
                    and p.outerid = '{0}'
                    and sra.outer_id = '750873459435863200'
                    """.format(unicId, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10)))  # ищем в БД создаваемое отсутсвие
                    res = cursor.fetchall()
                    print(len(res))

                    if len(res) == 0:  # шаг 5 отсутствие не создано
                        pass

                    else:
                        isValidTest = False
                        print(1)
                else:
                    isValidTest = False
                    print(2)
            else:
                isValidTest = False
                print(3)
        else:
            isValidTest = False
            print(4)
    else:
        isValidTest = False
        print(imprtRes)
        print(5)

    assert isValidTest is True


# IntegrRegress - 8 - Создание записи удаления по назначению, которое есть в ВФМ, прямой импорт в ВФМ
def test_removed_ep():
    isValidTest = True
    closed = False
    isValidOrgUnit = True
    dataType = 'EMPLOYEE_POSITION'
    unicId, tempImprtRes = createEmployeePosition(closed, isValidOrgUnit)
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    imprtRes = create_removed_entity(unicId, dataType)

    if '"success":true' in imprtRes and '"callType":"REMOVED_OBJECTS"' in imprtRes:  # шаг 1 верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 запись импорта - успешно
                cursor.execute("""
                select * from log_jremoved
                where employee_outer_id = '{0}'
                and position_outer_id = '{0}'
                and start_date = '2021-01-01'
                and end_date is null
                and type = '{1}'
                """.format(unicId, dataType))
                res = cursor.fetchall()

                if len(res) == 1:  # шаг 4 создана запись удаления в БД
                    cursor.execute("""
                    select * from employeeposition ep
                    where ep.card_number = '{0}'
                    """.format(unicId))
                    res = cursor.fetchall()

                    if len(res) == 0:  # шаг 5 назначение удалено фактически в БД
                        pass

                    else:
                        isValidTest = False
                        print(1)
                else:
                    isValidTest = False
                    print(2)
            else:
                isValidTest = False
                print(3)
        else:
            isValidTest = False
            print(4)
    else:
        isValidTest = False
        print(5)

    assert isValidTest is True


# IntegrRegress - 9 - Создание записи удаления по отсутствию, которое есть в ВФМ, прямой импорт в ВФМ
def test_removed_sr():
    validTestFlag = True
    isEpValid = True
    dataType = 'SCHEDULE_REQUEST'
    unicId, tempImprtRes = createScheduleRequest(isEpValid)
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    imprtRes = create_removed_entity(unicId, dataType)

    if '"success":true' in imprtRes and '"callType":"REMOVED_OBJECTS"' in imprtRes:  # шаг 1 верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult_2[0][0] == max_of_integrationcallresult[0][0] + 1:  # шаг 2 создана новая запись импорта
            print(max_of_integrationcallresult_2[0][1])

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 запись импорта - успешно
                cursor.execute("""
                select * from log_jremoved
                where employee_outer_id = '{0}'
                and position_outer_id = '{0}'
                and start_date = '{1}'
                and end_date = '{2}'
                and type = '{3}'
                """.format(unicId, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10), dataType))
                res = cursor.fetchall()

                if len(res) == 1:  # шаг 4 создана запись удаления в БД
                    cursor.execute("""
                    select * from schedule_request sr
                    join employee e
                    on e.id = sr.employee_id
                    join position p
                    on p.id = sr.position_id
                    join schedule_request_alias sra
                    on sra.id = sr.alias_id
                    where sr.startdatetime = '{1}'
                    and sr.enddatetime = '{2}'
                    and e.outerid = '{0}'
                    and p.outerid = '{0}'
                    and sra.outer_id = '750873459435863200'
                    """.format(unicId, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10)))
                    res = cursor.fetchall()

                    if len(res) == 0:  # шаг 5 - отсутствие удалено в БД
                        pass

                    else:
                        validTestFlag = False
                        print(1)
                else:
                    validTestFlag = False
                    print(2)
            else:
                validTestFlag = False
                print(3)
        else:
            validTestFlag = False
            print(4)
    else:
        validTestFlag = False
        print(5)

    assert validTestFlag is True


#  IntegrRegress - 10 - Создание записи удаления по назначению, которого нет в ВФМ, прямой импорт в ВФМ
def test_removed_InvalidEp():
    validTestFlag = True  # флаг того, что тест успешен
    unicId = 'amogus3'  # уникальный айдишник, по которому будем удалять назначение
    dataType = 'EMPLOYEE_POSITION'

    cursor = connWfm().cursor()
    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    imprtRes = create_removed_entity(unicId, dataType)  # отправляем запрос на удаление по указанным параметрам

    if '"success":false' in imprtRes and '"callType":"REMOVED_OBJECTS"' in imprtRes:  # шаг 1 - проверка ответа на запрос
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult_2[0][0] == max_of_integrationcallresult[0][0] + 1:  # Шаг 2 - проверка, что сформирована новая запись импорта в БД

            if max_of_integrationcallresult_2[0][1] is False:  # шаг 3 - сформированная запись импорта - неуспешно
                cursor.execute("""
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(max_of_integrationcallresult_2[0][0]))
                res = cursor.fetchall()

                if res[0][0] == 'There is no employee with outer id: {0}'.format(unicId):  # шаг 4 проверка верного сообщения об ошибке в БД  # Шаг 4 - в бд не создалось записи удаленной сущености по несуществ. назначению
                    cursor.execute("""
                    select * from log_jremoved lj
                    where lj.employee_outer_id = '{0}'
                    """.format(unicId))
                    res = cursor.fetchall()

                    if len(res) == 0:  # Шаг 5 - в бд не создалось записи удаленной сущености по несуществ. назначению
                        pass

                    else:
                        validTestFlag = False
                        print(1)

                else:
                    validTestFlag = False
                    print(2)
            else:
                validTestFlag = False
                print(3)

        else:
            validTestFlag = False
            print(4)

    else:
        validTestFlag = False  # если условие не успешно - флаг успешности теста меняется
        print(5)

    assert validTestFlag is True


# IntegrRegress - ??? - Повторная отправка, ранее загруженного отсутствия в ВФМ (не происходит дублирование записей)
def test_create_double_schedule_request():
    validTestFlag = True
    isEpValid = True
    startDate = str(datetime.date.today().replace(day=1))
    endDate = str(datetime.date.today().replace(day=10))
    cursor = connWfm().cursor()

    cursor.execute("""
    select i.id
    from integrationcallresult i
    order by i.id desc
    limit 1
    """)  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = cursor.fetchall()

    unicId, imprtRes = createScheduleRequest(isEpValid)

    url = '{0}/api/v1/integration-json/schedule-requests'.format(wfm_url)
    queryParams = {'stop-on-error': 'false', 'delete-intersections': "true", 'split-requests': 'true', 'process-shifts': 'delete', 'start-date-shift-filter': 'true'}
    jsonString = [
        {
            "employeeOuterId": unicId,
            "positionOuterId": unicId,
            "type": "750873459435863200",
            "startDate": startDate,
            "startTime": "00:00",
            "endDate": endDate,
            "endTime": "23:59"
        }
    ]
    body = json.dumps(jsonString)
    imprtRes = requests.post(url, data=body, params=queryParams, auth=('superuser', 'Wsxzaq1!'))

    if '"success":true' in imprtRes.text and '"callType":"SCHEDULE_REQUESTS_IMPORT"' in imprtRes.text:  # шаг 1 - верный ответ
        cursor.execute("""
        select i.id, i.success
        from integrationcallresult i
        order by i.id desc
        limit 1
        """)
        max_of_integrationcallresult_2 = cursor.fetchall()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 3 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 если запись импорта - успешно
                cursor.execute("""
                select * from schedule_request sr
                join employee e
                on e.id = sr.employee_id
                join position p
                on p.id = sr.position_id
                join schedule_request_alias sra
                on sra.id = sr.alias_id
                where sr.startdatetime = '{1} 00:00:00.000'
                and sr.enddatetime = '{2} 23:59:00.000'
                and e.outerid = '{0}'
                and p.outerid = '{0}'
                and sra.outer_id = '750873459435863200'
                """.format(unicId, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10)))
                res = cursor.fetchall()  # ищем наши отсутствия в БД по параметрам

                if len(res) == 1:  # шаг 4 если отсутствие не дублировалось
                    pass
                else:
                    validTestFlag = False
                    print(1)
            else:
                validTestFlag = False
                print(2)
        else:
            validTestFlag = False
            print(3)
    else:
        validTestFlag = False
        print(4)

    assert validTestFlag is True
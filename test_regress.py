from sql_scripts import *
from requests import *
from create_unic_id import create_unic_id


# IntegrRegress - 1 - Создания оргЮнита напрямую в ВФМ
def test_org_unit():
    valid_test_flag = True
    is_main_org_unit_valid = True

    unic_id = create_unic_id()

    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_org_unit(is_main_org_unit_valid, unic_id)

    if '"success":true' in imprt_res.text and '"callType":"ORGANIZATION_UNIT_IMPORT"' in imprt_res.text:  # шаг 1 снова получаем самую новую запись импорта в таблице
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 новая запись импорта - успешно
                res = find_org_unit_in_wfm(unic_id, is_main_org_unit_valid)

                if len(res) == 1:  # в БД создан оргЮнит по заданным параметрам
                    pass

                else:
                    valid_test_flag = False
                    print('1')
            else:
                valid_test_flag = False
                print('2')
        else:
            valid_test_flag = False
            print('3')
    else:
        valid_test_flag = False
        print('4')

    assert (valid_test_flag is True)


# IntegrRegress - 2 - Создание ошибочной записи оргЮнита при импорте напрямую в ВФМ (с привязкой к несуществующему вышестоящему подразделению)
def test_org_unit_error():
    valid_test_flag = True
    is_main_org_unit_valid = False

    unic_id = create_unic_id()

    max_of_integrationcallresult = get_max_of_integrationcallresult()  # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте

    imprt_res = create_org_unit(is_main_org_unit_valid, unic_id)

    if '"logref":"error"' in imprt_res.text:  # шаг 1 - верный ответ на запрос
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 - если сформирована новая запись импорта

            if max_of_integrationcallresult_2[0][1] is False:  # шаг 3 - если сформированная запись неуспешна
                res = is_import_res_succes(max_of_integrationcallresult_2[0][0])  # получаем запись с месседжем об ошибке по нашей записи импорта

                if res[0][0] == 'Parent Org Unit of outer id {0} was not found Stop-on-error is false'.format(unic_id):  # шаг 4 если верный месседж в ошибке
                    res = find_org_unit_in_wfm(unic_id, is_main_org_unit_valid)  # ищем оргЮнит в Бд по указанным параметрам

                    if len(res) == 0:  # шаг 5 ошибочный оргЮнит не создан в БД
                        pass

                    else:
                        valid_test_flag = False
                        print(1)
                else:
                    valid_test_flag = False
                    print(2)
            else:
                valid_test_flag = False
                print(3)
        else:
            valid_test_flag = False
            print(4)
    else:
        valid_test_flag = False
        print(5)

    assert valid_test_flag is True


# IntegrRegress - 3 - Создание нового сотрудника и назначения напрямую в ВФМ
def test_create_ep():
    valid_test_flag = True
    closed = False  # назначение открыто
    is_valid_org_unit = True  # указан валидный оргЮнит

    unic_id = create_unic_id()

    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_ep(closed, is_valid_org_unit, unic_id)

    if '"success":true' in imprt_res and '"callType":"EMPLOYEE_POSITION_FULL_IMPORT"' in imprt_res:  # шаг 1 - если получен верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 - если сформирована запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # если запись импорта - успешно
                res = find_ep_in_wfm(unic_id, closed, is_valid_org_unit)  # ищем нашего сотрудника в БД по параметрам

                if len(res) == 1:  # если сотрудник сформирован в ПБД
                    pass

                else:
                    valid_test_flag = False
            else:
                valid_test_flag = False
        else:
            valid_test_flag = False
    else:
        valid_test_flag = False

    assert valid_test_flag is True


# IntegrRegress - 4 - Закрытие существующего назначения напрямую в ВФМ
def test_close_ep():
    valid_test_flag = True
    closed = True  # создание закрытого назначения
    is_valid_org_unit = True  # указан валидный ОЮ

    unic_id = create_unic_id()

    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_ep(closed, is_valid_org_unit, unic_id)

    if '"success":true' in imprt_res.text and '"callType":"EMPLOYEE_POSITION_FULL_IMPORT"' in imprt_res.text:  # шаг 1-получен верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2-создана запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 - запись импорта - успешно
                res = find_ep_in_wfm(unic_id, closed, is_valid_org_unit) # ищем наше закрытое назначение в БД по параметрам

                if len(res) == 1:  # шаг 4 - создано закрытое назначение в БД
                    pass

                else:
                    valid_test_flag = False
            else:
                valid_test_flag = False
        else:
            valid_test_flag = False
    else:
        valid_test_flag = False

    assert valid_test_flag is True


# IntegrRegress - 5 - Создание нового сотрудника и назначения, когда в ВФМ нет оргЮнита, для которого импортируется сотрудник напрямую в ВФМ
def test_create_ep_without_orgunit():
    valid_test_flag = True
    closed = False  # создание открытого назначения
    is_valid_org_unit = False  # указан не валидный ОЮ

    unic_id = create_unic_id()

    # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_ep(closed, is_valid_org_unit, unic_id)

    if '"success":false' in imprt_res.text and '"callType":"EMPLOYEE_POSITION_FULL_IMPORT"' in imprt_res.text:  # шаг 1 - верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана запись импорта

            if max_of_integrationcallresult_2[0][1] is False:
                sql_request = """
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(max_of_integrationcallresult_2[0][0])
                res = run_sql_wfm(sql_request)  # для нашей записи импорта запоминаем месседж об ошибке

                if res[0][0] == 'Organization Unit of outer id amogus was not found'.format(unic_id):  # шаг 3 верный месседж об ошибке
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
                    and ep.enddate is null
                    and e.outerid = '{0}'
                    and e.lastname = '{0}'
                    and p.outerid = '{0}'
                    """.format(unic_id, datetime.date.today())
                    res = run_sql_wfm(sql_request)  # ищем ошибочное назначение по параметрам в БД

                if len(res) == 0:  # в бд нет назначением по отправленным параметрам
                    pass

                else:
                    valid_test_flag = False
                    print(1)
            else:
                valid_test_flag = False
                print(2)
        else:
            valid_test_flag = False
            print(3)
    else:
        valid_test_flag = False
        print(4)

    assert valid_test_flag is True


# IntegrRegress - 6 - Создание нового отсутствия напрямую в ВФМ
def test_create_sr():
    valid_test_flag = True
    is_ep_valid = True

    unic_id = create_unic_id()

    # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_sr(is_ep_valid, unic_id)

    if '"success":true' in imprt_res.text and '"callType":"SCHEDULE_REQUESTS_IMPORT"' in imprt_res.text:  # шаг 1 - верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 2 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 если запись импорта - успешно
                sql_request = """
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
                    """.format(unic_id, str(datetime.date.today().replace(day=1)), str(datetime.date.today().replace(day=10)))
                res = run_sql_wfm(sql_request)  # ищем наше отсутствие в БД по параметрам

                if len(res) == 1:  # шаг 4 если отсутствие создано в БД
                    pass

                else:
                    valid_test_flag = False
                    print(1)
            else:
                valid_test_flag = False
                print(2)
        else:
            valid_test_flag = False
            print(3)
    else:
        valid_test_flag = False
        print(4)

    assert valid_test_flag is True


# IntegrRegress - 7 - Создание нового отсутствия с привязкой к назначению, которого нет в ВФМ, прямой импорт в ВФМ
def test_create_sr_with_invalid_ep():
    valid_test_flag = True
    is_ep_valid = False

    unic_id = create_unic_id()

    # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_sr(is_ep_valid, unic_id)

    if '"success":false' in imprt_res.text and '"callType":"SCHEDULE_REQUESTS_IMPORT"' in imprt_res.text:  # шаг 1 - верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is False:  # шаг 3 новая запись импорта - не успешно
                sql_request = """
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(max_of_integrationcallresult_2[0][0])
                res = run_sql_wfm(sql_request)  # для нашей записи импорта запоминаем месседж об ошибке

                if res[0][0] == 'Position of the outer id {0} was not found'.format(unic_id):  # шаг 4 верный месседж об ошибке
                    sql_request = """select * from schedule_request sr
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
                    """.format(unic_id, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10))  # ищем в БД создаваемое отсутсвие
                    res = run_sql_wfm(sql_request)

                    if len(res) == 0:  # шаг 5 отсутствие не создано
                        pass

                    else:
                        valid_test_flag = False
                        print(1)
                else:
                    valid_test_flag = False
                    print(2)
            else:
                valid_test_flag = False
                print(3)
        else:
            valid_test_flag = False
            print(4)
    else:
        valid_test_flag = False
        print(5)

    assert valid_test_flag is True


# IntegrRegress - 8 - Создание записи удаления по назначению, которое есть в ВФМ, прямой импорт в ВФМ
def test_removed_ep():
    valid_test_flag = True
    closed = False
    is_valid_org_unit = True
    data_type = 'EMPLOYEE_POSITION'

    unic_id = create_unic_id()

    _ = create_ep(closed, is_valid_org_unit, unic_id)

    # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_removed_entity(unic_id, data_type)

    if '"success":true' in imprt_res.text and '"callType":"REMOVED_OBJECTS"' in imprt_res.text:  # шаг 1 верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 1 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 запись импорта - успешно
                sql_reeuest = """
                select * from log_jremoved
                where employee_outer_id = '{0}'
                and position_outer_id = '{0}'
                and start_date = '2021-01-01'
                and end_date is null
                and type = '{1}'
                """.format(unic_id, data_type)
                res = run_sql_wfm(sql_reeuest)

                if len(res) == 1:  # шаг 4 создана запись удаления в БД
                    sql_reeuest = """
                    select * from employeeposition ep
                    where ep.card_number = '{0}'
                    """.format(unic_id)
                    res = run_sql_wfm(sql_reeuest)

                    if len(res) == 0:  # шаг 5 назначение удалено фактически в БД
                        pass

                    else:
                        valid_test_flag = False
                        print(1)
                else:
                    valid_test_flag = False
                    print(2)
            else:
                valid_test_flag = False
                print(3)
        else:
            valid_test_flag = False
            print(4)
    else:
        valid_test_flag = False
        print(5)

    assert valid_test_flag is True


# IntegrRegress - 9 - Создание записи удаления по отсутствию, которое есть в ВФМ, прямой импорт в ВФМ
def test_removed_sr():
    valid_test_flag = True
    is_ep_valid = True
    data_type = 'SCHEDULE_REQUEST'

    unic_id = create_unic_id()

    _ = create_sr(is_ep_valid, unic_id)

    # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_removed_entity(unic_id, data_type)

    if '"success":true' in imprt_res.text and '"callType":"REMOVED_OBJECTS"' in imprt_res:  # шаг 1 верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult_2[0][0] == max_of_integrationcallresult[0][0] + 1:  # шаг 2 создана новая запись импорта
            print(max_of_integrationcallresult_2[0][1])

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 запись импорта - успешно
                sql_requset = """
                select * from log_jremoved
                where employee_outer_id = '{0}'
                and position_outer_id = '{0}'
                and start_date = '{1}'
                and end_date = '{2}'
                and type = '{3}'
                """.format(unic_id, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10), data_type)
                res = run_sql_wfm(sql_requset)

                if len(res) == 1:  # шаг 4 создана запись удаления в БД
                    sql_request = """
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
                    """.format(unic_id, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10))
                    res = run_sql_wfm(sql_request)

                    if len(res) == 0:  # шаг 5 - отсутствие удалено в БД
                        pass

                    else:
                        valid_test_flag = False
                        print(1)
                else:
                    valid_test_flag = False
                    print(2)
            else:
                valid_test_flag = False
                print(3)
        else:
            valid_test_flag = False
            print(4)
    else:
        valid_test_flag = False
        print(5)

    assert valid_test_flag is True


#  IntegrRegress - 10 - Создание записи удаления по назначению, которого нет в ВФМ, прямой импорт в ВФМ
def test_removed_invalid_ep():
    valid_test_flag = True  # флаг того, что тест успешен
    unic_id = 'amogus3'  # уникальный айдишник, по которому будем удалять назначение
    data_type = 'EMPLOYEE_POSITION'


# запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = get_max_of_integrationcallresult()

    imprt_res = create_removed_entity(unic_id, data_type)  # отправляем запрос на удаление по указанным параметрам

    if '"success":false' in imprt_res.text and '"callType":"REMOVED_OBJECTS"' in imprt_res.text:  # шаг 1 - проверка ответа на запрос
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult_2[0][0] == max_of_integrationcallresult[0][0] + 1:  # Шаг 2 - проверка, что сформирована новая запись импорта в БД

            if max_of_integrationcallresult_2[0][1] is False:  # шаг 3 - сформированная запись импорта - неуспешно
                sql_request = """
                select ievent.message from integrationevent ievent
                join integrationcallresult icall
                on ievent.integrationcallresult_id = icall.id
                where icall.id = {0}
                """.format(max_of_integrationcallresult_2[0][0])
                res = run_sql_wfm(sql_request)

                if res[0][0] == 'There is no employee with outer id: {0}'.format(unic_id):  # шаг 4 проверка верного сообщения об ошибке в БД  # Шаг 4 - в бд не создалось записи удаленной сущености по несуществ. назначению
                    sql_request = """
                    select * from log_jremoved lj
                    where lj.employee_outer_id = '{0}'
                    """.format(unic_id)
                    res = run_sql_wfm(sql_request)

                    if len(res) == 0:  # Шаг 5 - в бд не создалось записи удаленной сущености по несуществ. назначению
                        pass

                    else:
                        valid_test_flag = False
                        print(1)

                else:
                    valid_test_flag = False
                    print(2)
            else:
                valid_test_flag = False
                print(3)

        else:
            valid_test_flag = False
            print(4)

    else:
        valid_test_flag = False  # если условие не успешно - флаг успешности теста меняется
        print(5)

    assert valid_test_flag is True


# IntegrRegress - ??? - Повторная отправка, ранее загруженного отсутствия в ВФМ (не происходит дублирование записей)
def test_create_sr_double():
    valid_test_flag = True
    is_ep_valid = True

    unic_id = create_unic_id()

    # запоминает последнюю запись импорта, чтобы исследовать запись импорту, созданную в тесте
    max_of_integrationcallresult = get_max_of_integrationcallresult()

    for _ in range(2):
        imprt_res = create_sr(is_ep_valid, unic_id)

    if '"success":true' in imprt_res.text and '"callType":"SCHEDULE_REQUESTS_IMPORT"' in imprt_res.text:  # шаг 1 - верный ответ
        max_of_integrationcallresult_2 = get_max_of_integrationcallresult()  # снова получаем самую новую запись импорта в таблице

        if max_of_integrationcallresult[0][0] + 3 == max_of_integrationcallresult_2[0][0]:  # шаг 2 создана новая запись импорта

            if max_of_integrationcallresult_2[0][1] is True:  # шаг 3 если запись импорта - успешно
                sql_request = """
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
                """.format(unic_id, datetime.date.today().replace(day=1), datetime.date.today().replace(day=10))
                res = run_sql_wfm(sql_request)  # ищем наши отсутствия в БД по параметрам

                if len(res) == 1:  # шаг 4 если отсутствие не дублировалось
                    pass
                else:
                    valid_test_flag = False
                    print(1)
            else:
                valid_test_flag = False
                print(2)
        else:
            valid_test_flag = False
            print(3)
    else:
        valid_test_flag = False
        print(4)

    assert valid_test_flag is True

from API_wrapper import *


def create_org_unit(is_main_org_unit_valid, unic_id):
    if is_main_org_unit_valid:
        main_org_unit_id = 'MainOrgUnit'
    else:
        main_org_unit_id = 'amogus'

    url = create_api('org-units')
    body = create_org_unit_json(unic_id, main_org_unit_id)
    query_params = {}

    imprt_res = send_post_api(url, body, query_params)
    return imprt_res


def create_ep(closed, is_org_unit_valid, unic_id):
    if closed:
        end_date = str(datetime.date.today())
    else:
        end_date = None

    if is_org_unit_valid:
        org_unit_id = "b471e2cd-9af3-11e8-80da-42f2e9dc7849"
    else:
        org_unit_id = 'amogus'

    url = create_api('employee-positions-full')
    body = create_ep_json(unic_id, end_date, org_unit_id)
    query_params = {'concrete-dates': 'true', 'process-shifts': 'delete', 'start-date-shift-filter': 'true'}

    imprt_res = send_post_api(url, body, query_params)
    return imprt_res


def create_sr(is_ep_valid, unic_id):
    closed = False
    is_org_unit_valid = True
    if is_ep_valid:
        _ = create_ep(closed, is_org_unit_valid, unic_id)
    else:
        unic_id = 'amogus2'

    url = create_api('schedule-requests')
    body = create_sr_json(unic_id)
    query_params = {'stop-on-error': 'false', 'delete-intersections': "true", 'split-requests': 'true', 'process-shifts': 'delete', 'start-date-shift-filter': 'true'}

    imprt_res = send_post_api(url, body, query_params)
    return imprt_res


def create_removed_entity(unic_id, data_type):
    if data_type == 'EMPLOYEE_POSITION':
        start_date = '2021-01-01'
        end_date = None
    else:
        start_date = str(datetime.date.today().replace(day=1))
        end_date = str(datetime.date.today().replace(day=10))

    url = create_api('removed')
    body = create_removed_entity_json(unic_id, data_type, start_date, end_date)
    queryParams = {'stop-on-error': 'false', 'open-prev-employee-position': 'false'}

    imprt_res = send_post_api(url, body, queryParams)
    return imprt_res

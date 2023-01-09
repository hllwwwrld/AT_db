import requests
import json
from db_conn import requests_auth, wfm_addres
import datetime

wfm_url = wfm_addres()


def create_api(api_type):
    return "{0}/api/v1/integration-json/{1}".format(wfm_url, api_type)


def send_post_api(url, body, query_params):
    return requests.post(url, data=body, auth=(requests_auth()), params=query_params)


def string_to_json(string):
    return json.dumps(string)


def create_org_unit_json(unic_id, main_org_unit_id):
    body = [
        {
            "outerId": unic_id,
            "active": True,
            "availableForCalculation": None,
            "dateFrom": "1970-01-01",
            "name": unic_id,
            "organizationUnitTypeOuterId": 5,
            "parentOuterId": main_org_unit_id,
            "zoneId": None,
            "properties": None
        }
    ]
    body = string_to_json(body)
    return body


def create_ep_json(unic_id, end_date, org_unit_id):
    body = [
        {
            "startWorkDate": "2021-01-01",
            "endWorkDate": end_date,
            "number": unic_id,
            "employee": {
                "outerId": unic_id,
                "firstName": "Владимир",
                "lastName": unic_id,
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
                "outerId": unic_id,
                "chief": None,
                "organizationUnit": {
                    "outerId": org_unit_id
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
    body = string_to_json(body)
    return body


def create_sr_json(unic_id):
    body = [
        {
            "employeeOuterId": unic_id,
            "positionOuterId": unic_id,
            "type": "750873459435863200",
            "startDate": str(datetime.date.today().replace(day=1)),
            "startTime": "00:00",
            "endDate": str(datetime.date.today().replace(day=10)),
            "endTime": "23:59"
        }
    ]
    body = string_to_json(body)
    return body


def create_removed_entity_json(unic_id, data_type, start_date, end_date):
    body = [
        {
            "employeeOuterId": unic_id,
            "positionOuterId": unic_id,
            "type": data_type,
            "startDate": start_date,
            "endDate": end_date
        }
    ]
    body = string_to_json(body)
    return body
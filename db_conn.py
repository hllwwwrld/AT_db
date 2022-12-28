import psycopg2


def requests_auth():
    return 'superuser', 'Wsxzaq1!'


def conn_intgr():
    return psycopg2.connect(database="magnit_outside_integration",
                            user="a_kosolapov",
                            password="ZpIUcEozd3zsD",
                            host="stage-t-db.goodt.me",
                            port="5432")


# ПОДКЛЮЧЕНИЕ К БД ВФМ
def conn_wfm():
    return psycopg2.connect(database="magnit_wfm_qa",
                            user="a_kosolapov",
                            password="ZpIUcEozd3zsD",
                            host="stage-t-db.goodt.me",
                            port="5432")

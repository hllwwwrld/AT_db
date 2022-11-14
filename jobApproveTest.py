import psycopg2


# ПОДКЛЮЧЕНИЕ К ПБД
def connWfm():
    return psycopg2.connect(database="magnit_birzha_test",
                            user="a_kosolapov",
                            password="TcmYez0uzXgK9",
                            host="stage-t-db.goodt.me",
                            port="5432")


def create_file():
    cursor = connWfm().cursor()
    cursor.execute("""
    select s.id from shift s
    where startdatetime  >= '2022-10-28'
    and startdatetime < '2022-10-29'
    and worked_status = 'NOT_APPROVED'
    """)
    shiftsToApprove = cursor.fetchall()
    shiftsToApprove = [str(i)[1:-2] for i in shiftsToApprove]
    shiftsToApprove = ', '.join(shiftsToApprove)
    sa = open('sa.txt', 'w')
    sa.write(shiftsToApprove)
    print(shiftsToApprove)


s = input()
cursor = connWfm().cursor()
cursor.execute(""" 
select s.id from shift s
where s.id in ({0})
and (s.worked_status != 'APPROVED'
or  updated::text not like '2022-10-27 23:59%')
""".format(s))
res = cursor.fetchall()
res = [str(i)[1:-2] for i in res]
print(len(res), res, sep='\n')
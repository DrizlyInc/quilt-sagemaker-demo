import requests
from app.eta.util import SnowflakeConnector


def get_test_rows(target_db, requests=20):
    sql = f"""
    SELECT distinct * 
    FROM
    (SELECT  store_id, delivery_zipcode, null hod, null dow
    FROM {target_db}.datascience.etas_training_no_3pd
    ORDER BY RANDOM()
    LIMIT {requests}) T
    UNION
    SELECT *
    FROM
    (SELECT store_id, delivery_zipcode, hod::string, dow
    FROM {target_db}.datascience.etas_training_no_3pd
    ORDER BY RANDOM()
    LIMIT {requests}) U
    UNION
    SELECT -1, 'failing zipcode', null hod, null dow;
    """
    return SnowflakeConnector.get_json_from_sql(sql)

test_rows = get_test_rows('STAGE_JS', requests=10)
print(len(test_rows))
print(requests.post('http://localhost:8080/invocations', json={'eta_requests':test_rows}).text)
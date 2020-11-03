import requests
from app.eta.util import SnowflakeConnector


def get_test_rows(target_db, requests=20):
    sql = f"""
    SELECT distinct *
    FROM
    (SELECT store_id, TRY_CAST(delivery_zipcode as int) delivery_zipcode
    FROM {target_db}.datascience.etas_training_no_3pd
    ORDER  BY RANDOM()
    LIMIT {requests}) U
    UNION
    SELECT -1, null;
    """
    return SnowflakeConnector.get_json_from_sql(sql)

test_rows = get_test_rows('PROD', requests=10)
print(len(test_rows))

requests_list = [{'eta_requests':test_rows},
            {'eta_requests': []},
            {'eta_requests': [{}]},
            {'eta_requests': {}},
            {},
            "KLSDJFJ:SLGU:LSUDGLKGMSNDV>SDCJSKDJF"
    ]

for request in requests_list:
    response = requests.post('http://localhost:8080/invocations', json=request)
    print(response.status_code)
    print(f"request:{request} response: {response.text}")
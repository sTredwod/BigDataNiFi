"""Create an actual runnable Apache NiFi dataflow using the NiFi 1.28 REST API.

This script creates/reuses a Process Group with three controller services,
ConsumeKafkaRecord_2_6, PutDatabaseRecord, and LogAttribute, and starts it.
The Python producer is only released by Docker Compose when setup succeeds.
"""
import os
import sys
import time

import requests

API = os.environ.get('NIFI_URL', 'http://nifi:8080/nifi-api').rstrip('/')
PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'student123')
GROUP_NAME = 'Lab 5 - Kafka JSON to PostgreSQL'
SESSION = requests.Session()
SESSION.headers.update({'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json'})


def call(method, path, payload=None, timeout=35):
    url = API + path
    try:
        response = SESSION.request(method, url, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json() if response.content else {}
    except requests.RequestException as error:
        description = ''
        if error.response is not None:
            description = error.response.text[:2000]
        raise RuntimeError(f'NiFi {method} {path}: {error}. {description}') from error


def wait_for_nifi():
    for attempt in range(1, 181):
        try:
            status = call('GET', '/flow/process-groups/root', timeout=8)
            if status.get('processGroupFlow'):
                print('NiFi REST API is ready.', flush=True)
                return status
        except Exception as error:
            if attempt % 10 == 1:
                print(f'Waiting for NiFi API ({attempt}/180): {str(error)[:180]}', flush=True)
        time.sleep(3)
    raise RuntimeError('NiFi API was not ready after 9 minutes; inspect docker compose logs nifi')


def group_flow(gid):
    data = call('GET', f'/flow/process-groups/{gid}')
    return data['processGroupFlow']['flow']


def revision(entity):
    return entity['revision']['version']


def create_group(root_id):
    parent_flow = group_flow(root_id)
    existing = [g for g in parent_flow.get('processGroups', [])
                if g.get('component', {}).get('name') == GROUP_NAME]
    if existing:
        gid = existing[0]['id']
        print(f'Reusing existing Process Group {GROUP_NAME} ({gid})', flush=True)
        return gid
    data = call('POST', f'/process-groups/{root_id}/process-groups', {
        'revision': {'version': 0},
        'component': {'name': GROUP_NAME, 'position': {'x': 300, 'y': 300}},
    })
    print(f'Created Process Group {GROUP_NAME}', flush=True)
    return data['id']


def create_service(gid, name, typ, props):
    flow = group_flow(gid)
    for existing in flow.get('controllerServices', []):
        if existing.get('component', {}).get('name') == name:
            current = call('GET', f"/controller-services/{existing['id']}")
            if current['component']['state'] == 'ENABLED':
                print(f'Controller Service already enabled: {name}', flush=True)
                return current
            entity = current
            break
    else:
        entity = call('POST', f'/process-groups/{gid}/controller-services', {
            'revision': {'version': 0},
            'component': {'type': typ, 'name': name},
        })
    sid = entity['id']
    if props:
        entity = call('PUT', f'/controller-services/{sid}', {
            'revision': {'version': revision(entity)},
            'component': {'id': sid, 'properties': props},
        })
    current = call('GET', f'/controller-services/{sid}')
    validations = current.get('component', {}).get('validationErrors', [])
    if validations:
        print(f'{name}: validation messages: {validations}', flush=True)
    result = call('PUT', f'/controller-services/{sid}/run-status', {
        'revision': {'version': revision(current)},
        'state': 'ENABLED',
    })
    for _ in range(40):
        current = call('GET', f'/controller-services/{sid}')
        if current['component']['state'] == 'ENABLED':
            print(f'Enabled Controller Service {name}', flush=True)
            return current
        time.sleep(2)
    raise RuntimeError(f'Controller Service {name} did not enable: {current["component"].get("validationErrors")}')


def create_processor(gid, name, typ, pos, props, auto_terminate=()):
    flow = group_flow(gid)
    for existing in flow.get('processors', []):
        if existing.get('component', {}).get('name') == name:
            entity = call('GET', f"/processors/{existing['id']}")
            if entity.get('component', {}).get('state') == 'RUNNING':
                return entity
            break
    else:
        entity = call('POST', f'/process-groups/{gid}/processors', {
            'revision': {'version': 0},
            'component': {'type': typ, 'name': name, 'position': pos},
        })
    pid = entity['id']
    config = {
        'properties': props,
        'autoTerminatedRelationships': list(auto_terminate),
        'schedulingStrategy': 'TIMER_DRIVEN',
        'schedulingPeriod': '1 sec',
        'concurrentlySchedulableTaskCount': 1,
    }
    entity = call('PUT', f'/processors/{pid}', {
        'revision': {'version': revision(entity)},
        'component': {'id': pid, 'config': config},
    })
    errors = entity.get('component', {}).get('validationErrors', [])
    if errors:
        print(f'{name}: validation messages: {errors}', flush=True)
    print(f'Configured processor: {name}', flush=True)
    return entity


def create_connection(gid, name, source, target, relationships):
    flow = group_flow(gid)
    for conn in flow.get('connections', []):
        if conn.get('component', {}).get('name') == name:
            print(f'Connection exists: {name}', flush=True)
            return conn
    return call('POST', f'/process-groups/{gid}/connections', {
        'revision': {'version': 0},
        'component': {
            'name': name,
            'source': {'id': source['id'], 'groupId': gid, 'type': 'PROCESSOR'},
            'destination': {'id': target['id'], 'groupId': gid, 'type': 'PROCESSOR'},
            'selectedRelationships': list(relationships),
            'backPressureObjectThreshold': 10000,
            'backPressureDataSizeThreshold': '1 GB',
        },
    })


def start_processor(proc):
    pid = proc['id']
    current = call('GET', f'/processors/{pid}')
    if current['component']['state'] == 'RUNNING':
        print(f'Already running: {current["component"]["name"]}', flush=True)
        return
    errors = current.get('component', {}).get('validationErrors', [])
    if errors:
        raise RuntimeError(f'Processor {current["component"]["name"]} invalid: {errors}')
    call('PUT', f'/processors/{pid}/run-status', {
        'revision': {'version': revision(current)},
        'state': 'RUNNING',
    })
    for _ in range(20):
        status = call('GET', f'/processors/{pid}')
        if status['component']['state'] == 'RUNNING':
            print(f'RUNNING: {status["component"]["name"]}', flush=True)
            return
        time.sleep(1)
    raise RuntimeError(f'Processor {pid} did not start')


def main():
    status = wait_for_nifi()
    root_id = status['processGroupFlow']['id']
    gid = create_group(root_id)

    reader = create_service(
        gid, 'JsonTreeReader - raw JSON records',
        'org.apache.nifi.json.JsonTreeReader',
        {'schema-access-strategy': 'infer-schema'},
    )
    writer = create_service(
        gid, 'JsonRecordSetWriter - JSON array',
        'org.apache.nifi.json.JsonRecordSetWriter',
        {'output-grouping': 'output-array'},
    )
    dbcp = create_service(
        gid, 'PostgreSQL DBCP Connection Pool',
        'org.apache.nifi.dbcp.DBCPConnectionPool',
        {
            'Database Connection URL': 'jdbc:postgresql://postgres:5432/bigdata_nifi',
            'Database Driver Class Name': 'org.postgresql.Driver',
            'database-driver-locations': '/opt/nifi/drivers/postgresql-42.7.14.jar',
            'Database User': 'student',
            'Password': PASSWORD,
            'Validation-query': 'SELECT 1',
        },
    )

    consumer = create_processor(
        gid, '01 - Consume Kafka JSON events',
        'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6',
        {'x': 80, 'y': 180},
        {
            'bootstrap.servers': 'kafka:9092',
            'topic': 'lab5_sales',
            'topic_type': 'names',
            'group.id': 'nifi-lab5-consumers',
            'auto.offset.reset': 'earliest',
            'max.poll.records': '500',
            'record-reader': reader['id'],
            'record-writer': writer['id'],
        },
    )
    putdb = create_processor(
        gid, '02 - UPSERT raw records into PostgreSQL',
        'org.apache.nifi.processors.standard.PutDatabaseRecord',
        {'x': 480, 'y': 180},
        {
            'put-db-record-record-reader': reader['id'],
            'db-type': 'PostgreSQL',
            'put-db-record-statement-type': 'UPSERT',
            'put-db-record-dcbp-service': dbcp['id'],
            'put-db-record-schema-name': 'public',
            'put-db-record-table-name': 'sales_raw',
            'put-db-record-update-keys': 'event_id',
        },
        ['success'],
    )
    error_logger = create_processor(
        gid, '03 - Log failed events',
        'org.apache.nifi.processors.standard.LogAttribute',
        {'x': 450, 'y': 470},
        {},
        ['success'],
    )
    create_connection(gid, 'Parsed Kafka records', consumer, putdb, ['success'])
    create_connection(gid, 'Kafka JSON parsing failures', consumer, error_logger, ['parse.failure'])
    create_connection(gid, 'Database failures and retries', putdb, error_logger, ['failure', 'retry'])

    for processor in [error_logger, putdb, consumer]:
        start_processor(processor)
    print(f'SUCCESS: NiFi flow created and running. Process Group ID: {gid}', flush=True)
    print('Flow: ConsumeKafkaRecord_2_6 -> PutDatabaseRecord (UPSERT) -> PostgreSQL', flush=True)
    print('Open http://localhost:8085/nifi and enter the Lab 5 Process Group.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'ERROR: NiFi bootstrap failed: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)


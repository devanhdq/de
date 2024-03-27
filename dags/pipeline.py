import json
from datetime import datetime, timedelta

import requests
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.mongo.hooks.mongo import MongoHook

MAIN_URL = "https://api.sofascore.com/api/v1/event"
header = {
    'Content-Type': 'application',
    'Accept': 'application',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}


def day_before_today():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


def save_event(ti):
    data = ti.xcom_pull(task_ids='events.extract_api_events')
    events = data.get('events')
    ids = []
    mongo_conn = MongoHook(conn_id='mongo_conn_id')
    collection = mongo_conn.get_collection('tournaments', 'sofascore')
    collection.insert_many(events)
    for event in events:
        ids.append(event.get('id'))
    ti.xcom_push(key='ids', value=ids)


def check_api_availability(ids, api_name) -> []:
    available_api = []
    for id_ in ids:
        try:
            url = f'{MAIN_URL}/{id_}/{api_name}'
            response = requests.get(url, headers=header, timeout=3)
            if response.status_code != 404:
                available_api.append(id_)
        except Exception as e:
            print(f"Have error when check api: {url}: {e}")
    return available_api


@dag(dag_id='sofascore-pipeline',
     start_date=datetime(2024, 2, 1),
     schedule_interval='@daily',
     catchup=False
     )
def sofa_dag():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task_group
    def events():
        events_api_available = HttpSensor(
            task_id='check_api_events',
            http_conn_id='events_conn_id',
            endpoint=f'{day_before_today()}/',
            headers=header
        )

        extract_events = SimpleHttpOperator(
            task_id='extract_api_events',
            http_conn_id='events_conn_id',
            endpoint=f'{day_before_today()}/',
            headers=header,
            method='GET',
            response_filter=lambda response: json.loads(response.text),
            log_response=True,
        )
        save_events = PythonOperator(
            task_id='save_api_events',
            python_callable=save_event
        )

        events_api_available >> extract_events >> save_events

    @task_group
    def lineups_process():
        @task
        def api_check(ti) -> []:
            ids = ti.xcom_pull(key='ids')
            id_available = check_api_availability(ids=ids, api_name="lineups")
            return id_available

        @task_group()
        def lineups(id_):
            @task
            def api_process(event_id):
                url = f"{MAIN_URL}/{event_id}/lineups"
                response = requests.get(url, headers=header, timeout=60)
                data = json.loads(response.text)
                data["tournament_id"] = event_id

                mongo_conn = MongoHook(conn_id='mongo_conn_id')
                collection = mongo_conn.get_collection('lineups', 'sofascore')
                collection.insert_one(data)

            api_process(id_)

        lineups.expand(id_=api_check())

    @task_group
    def statistics_process():
        @task
        def api_check(ti) -> []:
            ids = ti.xcom_pull(key='ids')
            id_available = check_api_availability(ids=ids, api_name="statistics")
            return id_available

        @task_group()
        def statistic(id_):
            @task
            def api_process(event_id):
                url = f"{MAIN_URL}/{event_id}/statistics"
                response = requests.get(url, headers=header, timeout=60)
                data = json.loads(response.text)
                data["tournament_id"] = event_id

                mongo_conn = MongoHook(conn_id='mongo_conn_id')
                collection = mongo_conn.get_collection('statistics', 'sofascore')
                collection.insert_one(data)

            api_process(id_)

        statistic.expand(id_=api_check())

    @task_group
    def h2h_process():
        @task
        def api_check(ti) -> []:
            ids = ti.xcom_pull(key='ids')
            id_available = check_api_availability(ids=ids, api_name="h2h")
            return id_available

        @task_group()
        def h2h(id_):
            @task
            def api_process(event_id):
                url = f"{MAIN_URL}/{event_id}/h2h"
                response = requests.get(url, headers=header, timeout=60)
                data = json.loads(response.text)
                data["tournament_id"] = event_id

                mongo_conn = MongoHook(conn_id='mongo_conn_id')
                collection = mongo_conn.get_collection('h2h', 'sofascore')
                collection.insert_one(data)

            api_process(id_)

        h2h.expand(id_=api_check())

    @task_group
    def best_players_process():
        @task
        def api_check(ti) -> []:
            ids = ti.xcom_pull(key='ids')
            id_available = check_api_availability(ids=ids, api_name="best-players")
            return id_available

        @task_group()
        def best_players(id_):
            @task
            def api_process(event_id):
                url = f"{MAIN_URL}/{event_id}/best-players"
                response = requests.get(url, headers=header, timeout=60)
                data = json.loads(response.text)
                data["tournament_id"] = event_id

                mongo_conn = MongoHook(conn_id='mongo_conn_id')
                collection = mongo_conn.get_collection('best_players', 'sofascore')
                collection.insert_one(data)

            api_process(id_)

        best_players.expand(id_=api_check())

    @task_group
    def incidents_process():
        @task
        def api_check(ti) -> []:
            ids = ti.xcom_pull(key='ids')
            id_available = check_api_availability(ids=ids, api_name="incidents")
            return id_available

        @task_group()
        def incidents(id_):
            @task
            def api_process(event_id):
                url = f"{MAIN_URL}/{event_id}/incidents"
                response = requests.get(url, headers=header, timeout=60)
                data = json.loads(response.text)
                data["tournament_id"] = event_id

                mongo_conn = MongoHook(conn_id='mongo_conn_id')
                collection = mongo_conn.get_collection('incidents', 'sofascore')
                collection.insert_one(data)

            api_process(id_)

        incidents.expand(id_=api_check())

    start >> events() >> [lineups_process(),
                          statistics_process(),
                          h2h_process(),
                          best_players_process(),
                          incidents_process()
                          ] >> end


sofa_dag()

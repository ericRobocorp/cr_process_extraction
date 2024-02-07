"""
This automation is uses Robocorp's APIs and Vault to download the run data for all the processes in the Workspace
"""

from robocorp.tasks import task, setup
from robocorp import vault
from pathlib import Path
from datetime import datetime
from RPA.Database import Database
import requests, csv, os, json

WKSPCID = None
APIKEY = None


@setup
def get_workspace_id(task):
    # gets the workspace ID needed for the API
    # gets API Key from Vault of the workspace to utilize the API
    global WKSPCID, APIKEY
    WKSPCID = os.getenv("RC_WORKSPACE_ID", None)
    secret = vault.get_secret("get_processes")
    APIKEY = secret["api"]


@task
def minimal_task():
    process_list = list_all_processes()
    list_json = get_run_data(process_list)
    list_json = clean_up_process(list_json)
    create_csv(list_json)
    # use function below if you want to add the data to a Database
    # insert_to_database(list_json)


def list_all_processes():
    # gets all the Process IDs for the workspace
    process_url = f"https://cloud.robocorp.com/api/v1/workspaces/{WKSPCID}/processes"
    package = requests.request(
        "get",
        process_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": "RC-WSKEY " + APIKEY,
        },
    )
    return package.json()["data"]


def get_run_data(process_list):
    # downloads all the runs for a Worksapce. Because of the way our API works each Process has to be queried twice,
    # once for completed runs and once for unresolved runs
    combined_data = []
    for item in process_list:
        url = f"https://cloud.robocorp.com/api/v1/workspaces/{WKSPCID}/process-runs?process_id={item['id']}&state=completed"
        package = requests.request(
            "get",
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": "RC-WSKEY " + APIKEY,
            },
        )
        load_json = package.json()["data"]
        combined_data.extend(load_json)
        url = f"https://cloud.robocorp.com/api/v1/workspaces/{WKSPCID}/process-runs?process_id={item['id']}&state=unresolved"
        package = requests.request(
            "get",
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": "RC-WSKEY " + APIKEY,
            },
        )
        load_json = package.json()["data"]
        # Unresolved process runs do not have the time ran, as a result a second function is called for each unresolved process run
        # which gets all the step runs, totals the duration of the runs, then adds it to the duration for the process run
        add_durations = get_unresolved_minutes(load_json)
        combined_data.extend(add_durations)

    return combined_data


def clean_up_process(list_json):
    # normalizes the data for better storage and readability
    for item in list_json:
        item["process_id"] = item["process"]["id"]
        item["process_name"] = item["process"]["name"]
        del item["process"]
        # below this was a different process
        item["started_at"] = datetime.strptime(
            item["started_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        item["created_at"] = datetime.strptime(
            item["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        if item["state"] != "unresolved":
            item["ended_at"] = datetime.strptime(
                item["ended_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
            )
    return list_json


def get_unresolved_minutes(runs):
    # adds the duration of all the step runs in order to have the duration saved for each unresolved process run
    for run_id in runs:
        url = f"https://cloud.robocorp.com/api/v1/workspaces/{WKSPCID}/step-runs?process_run_id={run_id['id']}"
        package = requests.request(
            "get",
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": "RC-WSKEY " + APIKEY,
            },
        )
        get_duration = package.json()["data"]
        run_id["duration"] = sum(item["duration"] for item in get_duration)
    return runs


def create_csv(list_json):
    # creates a CSV of all the data extracted
    output_dir = Path(os.environ.get("ROBOT_ARTIFACTS", "output"))
    csv_file_path = f"{output_dir}/combined.csv"
    with open(csv_file_path, mode="w", newline="") as csv_file:
        fieldnames = list_json[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(list_json)


def insert_to_database(list_json):
    # This is an example of code you can use for storing the data in a database
    # mysql is used in this example
    secrets = vault.get_secret("db_connect")
    db = Database()
    db.connect_to_database(
        "pymysql",  # Database module to use
        secrets["database"],  # Name of the database
        secrets["username"],  # Username
        secrets["password"],
        secrets["location"],  # Host address
    )
    for i in list_json:
        started_by = json.dumps(i["started_by"])
        upsert = (
            f"INSERT IGNORE INTO logs (id, state, process_id, process_name, duration, started_at, created_at, ended_at, started_by) "
            f"VALUES ('{i['id']}', '{i['state']}', '{i['process_id']}','{i['process_name']}', '{i['duration']}', '{i['started_at']}', '{i['created_at']}', '{i['ended_at']}', '{started_by}')"
        )
        stats = db.query(upsert)

    db.disconnect_from_database()

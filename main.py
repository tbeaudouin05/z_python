# Main module to get data from Redcat, upload it into Anaplan, and trigger the
# import actions within Anaplan.

import os
import base64
import csv
import timeit
import datetime
import yaml
import anaplan_api
import redcat_ops


print "Starting anaplan_data_load.main at {}".format(
    datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))

# Settings
with open("settings/config.yaml", "r") as config_file:
    config = yaml.load(config_file)

with open("settings/tables.yaml", "r") as tables_file:
    tables = yaml.load(tables_file)

with open("settings/files.yaml", "r") as files_file:
    files = yaml.load(files_file)


user_ap = "Basic " + base64.b64encode(config["anaplan"]["login"].strip("\n"))

header_get = {
    "Authorization": user_ap,
    "Accept": "application/json"
}

header_post = {
    "Authorization": user_ap,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

header_put = {
    "Authorization": user_ap,
    "Accept": "application/octet-stream"
}


# Download Data from Redcat
def get_data():
    for model in config["anaplan"]["models"]:
        print "{}: Downloading data for model: {}.".format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), model)
        timer_model = timeit.default_timer()

        for table in tables[model]:
            print "{}: Downloading table: {}.".format(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), table)
            query = redcat_ops.get_query(table)
            print "{}: Running query: {}.".format(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), table)
            data, cols = redcat_ops.run_query(
                config["redcat"]["host"],
                config["redcat"]["user"],
                config["redcat"]["pwd"],
                config["redcat"]["db"],
                config["redcat"]["port"], query)
            print "{}: Creating table: {}.".format(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), table)
            redcat_ops.create_file(cols, table)
            print "{}: Appending table: {}.".format(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), table)
            redcat_ops.append_to_file(data, table)

        print "{}: Finished model {} in {} seconds.".format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), model,
            str(int(timeit.default_timer() - timer_model)))


# Upload data into Anaplan
def upload_data():
    for model in config["anaplan"]["models"]:
        print "{}: Uploading data for model {}.".format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), model)
        timer_model = timeit.default_timer()

        file_ids = anaplan_api.get_file_id_dict(
            config["anaplan"]["model_ids"][model],
            config["anaplan"]["workspace_id"], header_get)
        print file_ids

        for file in files[model]:
            print "{}: Uploading table {}.".format(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), file)
            timer_table = timeit.default_timer()
            upload_status_code = anaplan_api.upload_file(files["upload"][model][file], file_ids[file],
                config["anaplan"]["model_ids"][model],
                config["anaplan"]["workspace_id"], header_put)
            print "{} uploaded in {} seconds with status code: {}".format(file,
                str(int(timeit.default_timer() - timer_table)), upload_status_code)

        print "{}: Finished model {} in {} seconds.".format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), model,
            str(int(timeit.default_timer() - timer_model)))

# Triggering import actions
def trigger_imports():
    for model in config["anaplan"]["models"]:
        print "{}: Triggering imports for model {}.".format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), model)
        timer_model = timeit.default_timer()

        import_ids = anaplan_api.get_import_id_dict(
            config["anaplan"]["model_ids"][model],
            config["anaplan"]["workspace_id"], header_get)
        print import_ids

        for file in files[model]:
            imp = "import " + file
            print "{}: Triggering {}.".format(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), imp)
            timer_import = timeit.default_timer()

            print import_ids[imp]

            import_status_code = anaplan_api.trigger_import(import_ids[imp],
                config["anaplan"]["model_ids"][model],
                config["anaplan"]["workspace_id"], header_post)
            print "{} ran in {} seconds with status code: {}".format(imp,
                str(int(timeit.default_timer() - timer_import)), import_status_code)

        print "{}: Finished model {} in {} seconds.".format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), model,
            str(int(timeit.default_timer() - timer_model)))


def run_full_process():
    get_data()
    upload_data()
    trigger_imports()

if __name__ == "__main__":
    run_full_process()

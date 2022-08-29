#!/usr/bin/python
import psycopg2
from config import config
import json
import datetime, time
import schedule
import subprocess
import os
import copy

from data_models import alert_template
from custom_error import Custom_error  
from mappers import model_id_to_sensor  
import requests

def get_last_ts():
    """Retrives a timestamp of the last observed notification."""

    print("Obtaining last timestamp ...")
    try:
        with open('lastts.txt', 'r') as f:
            lastts = f.read()
        print("Reading it from file ...")
    except Error:
        lastts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print("No timestamp found, setting to current timestamp ({})...".format(lastts))
    return(lastts)

def write_last_ts(ts):
    """Writes the time of ts (last retrieved notification)"""

    with open('lastts.txt', 'w') as f:
        f.write(ts)

def get_last_notifications(lastts):
    """
    Connect to the PostgreSQL database server and retrieves all the
    notifications since last notification timestamp.
    """

    conn = None

    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL fetch new')
        cur.execute('SELECT id, user_id, model_id, title, content, time FROM notifications WHERE time > \'{}\''.format(lastts))

        # display the PostgreSQL database server version
        rows = cur.fetchall()

        obj = []

        for row in rows:
            # convert tuple to a list for easier manipulation
            items = list(row)
            # to milliseconds
            items[5] = items[5].timestamp() * 1000
            obj.append({
                "id": items[0],
                "user_id": items[1],
                "model_id": items[2],
                "title": items[3],
                "content": items[4],
                "time": items[5]
            })

            # write last timestamp
            lastts = row[5].strftime("%Y-%m-%d %H:%M:%S.%f")

        # close the communication with the PostgreSQL
        cur.close()
        write_last_ts(lastts)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:

        if conn is not None:
            conn.close()
            print('Database connection closed.')

    return obj

def postToFiware(data_model, entity_id, update):
    """Posts data model to the perscribed entity id"""
    
    global base_url
    global fiware_headers

    print(data_model)

    params = (
        ("options", "keyValues"),
    )
    if update:
        dm_type = data_model["type"]
        data_model.pop("type")

        # Try sending it to already existing entity (url)
        response = requests.post(base_url + entity_id + "/attrs/" , headers=fiware_headers, params=params, data=json.dumps(data_model) )

        # Otherwise add type and id and create new entity
        if response.status_code > 300:
            data_model["type"] = dm_type
            data_model["id"] = entity_id
            response = requests.post(base_url , headers=fiware_headers, params=params, data=json.dumps(data_model) )

    else:
        data_model["id"] = entity_id
        response = requests.post(base_url , headers=fiware_headers, params=params, data=json.dumps(data_model) )

    if (response.status_code > 300):
        raise Custom_error(f"Error sending to the API. Response stauts code: {response.status_code}")

def postToFiware_ld(data_model, entity_id):
    if(debug):
        print(print(json.dumps(data_model, indent=4, sort_keys=True)))

    # URL contstruction
    url = base_url + entity_id + "/attrs"

    response = requests.post(url, headers=fiware_headers, data=json.dumps(data_model) )
    
    # TODO test if it failed because the entity is not yet created
    if(response.status_code == 301):
        # Create entity
        url = self.create_url
        response = requests.post(url, headers=fiware_headers, data=json.dumps(data_model) )
        
        # Check if creatin was sucesfull
        if (response.status_code > 300):
            print(f"Error creating an entity", flush=True)

    # Check if upload was successful
    if (response.status_code > 300):
        print(f"Error sending to the API. Response status conde {response.status_code}", flush=True)
    try:
        if(type(eval(response.content.decode("utf-8"))) is not str):
            status_code = eval(response.content.decode("utf-8")).get("status_code")
            # Test for errors and log them
            if (status_code > 300):
                message = eval(response.content.decode("utf-8")).get("message")
                print(f"Error sending to the API. Response status conde {status_code}", flush=True)
                print(f"Response body content: {message}")
                # raise Custom_error(f"Error sending to the API. Response stauts code: {response.status_code}")
    except:
        print(response.content)

def postToFiware_context_ld(data_model, entity_id):
    """
    Posts entities to the API. Note that if an entity does not
    exist this does not create it (the entity needs to be manualy
    created beforehand).
    """
    body = data_model

    # URL contstruction
    url = base_url + entity_id + "/attrs"

    debug = True

    if(debug):
        print(f"URL ld: {url}")
        print(json.dumps(body, indent=4, sort_keys=True)) 
    
    # Assumes that entity is created and can just do a patch
    response = requests.patch(url, headers=fiware_headers, data=json.dumps(body))

    # Check if upload was successful
    if (response.status_code > 300):
        print(f"Error sending to the API. Response status conde {response.status_code}", flush=True)
    try:
        if(type(eval(response.content.decode("utf-8"))) is not str):
            status_code = eval(response.content.decode("utf-8")).get("status_code")
            # Test for errors and log them
            if (status_code > 300):
                message = eval(response.content.decode("utf-8")).get("message")
                print(f"Error sending to the API. Response status conde {status_code}", flush=True)
                print(f"Response body content: {message}")
                # raise Custom_error(f"Error sending to the API. Response stauts code: {response.status_code}")
    except:
        print(response.content)

def create_data_model(obj, entity_id):
    """Create the data model to post to FIWARE API from the object obtained 
    from the postgres."""
    data_model = copy.deepcopy(alert_template)

    # time to datetime (assumes timestamp in milliseconds)
    time_stamp = datetime.datetime.utcfromtimestamp(obj["time"]/1000)
    data_model["dateIssued"]["value"] = (time_stamp).isoformat("T", "seconds") + ".00Z"
    title = obj["title"]
    content = obj["content"]

    data_model["description"]["value"] = f"Title: {title}, Content: {content}"

    # Add entity id field (not required)
    #data_model["id"] = entity_id

    # Add context field
    data_model["@context"] = context

    # Sign and append signature
    data_model = sign(data_model)

    return data_model

def job():
    """Job for the scheduler, retrieving new notifications."""

    lastts = get_last_ts()
    obj = get_last_notifications(lastts)[-1]
    model_id = obj["model_id"]
    
    # Construct the entity (Alert) id TODO
    entity_id = f"urn:ngsi-ld:Alert:RO-Braila-{model_id_to_sensor[model_id]}-state-analysis-tool" 

    # PUT NAIADES FIWARE code here uzem sm zadnjega
    # Create data model to be sent
    data_model = create_data_model(obj, entity_id)


    # Try sendong the model
    try:
        postToFiware_context_ld(o, entity_id)
    except Exception as e:
        print(e, flush=True)

def sign(data_model):
    # Try signing the message with KSI tool (requires execution in
    # the dedicated container)
    try:
        signature = encode(data_model)
    except Exception as e:
        print(f"Signing failed", flush=True)
        signature = "signatureFailed"
    
    # Add signature to the message
    data_model["ksiSignature"] = {
        "type": "Property",
        "value": signature
    }

    return data_model

def encode(output_dict):
    global API_user
    global API_pass
    # Less prints
    debug = False

    # Transforms the JSON string ('dataJSON') to file (json.txt)
    os.system('echo %s > json.txt' %output_dict)
    #Sign the file using your credentials
    os.system(f'ksi sign -i json.txt -o json.txt.ksig -S http://5.53.108.232:8080 --aggr-user {API_user} --aggr-key {API_pass}') 
    
    # get the signature
    with open("json.txt.ksig", "rb") as f:
        encodedZip = base64.b64encode(f.read())
        if debug:
            print(encodedZip.decode())

    # Checking if the signature is correct
    verification = subprocess.check_output(f'ksi verify -i json.txt.ksig -f json.txt -d --dump G -X http://5.53.108.232:8081 --ext-user {API_user} --ext-key {API_pass} -P http://verify.guardtime.com/ksi-publications.bin --cnstr E=publications@guardtime.com | grep -xq "    OK: No verification errors." ; echo $?', shell=True)
    
    # Raise error if it is not correctly signed 
    assert int(verification) == 0

    return encodedZip       

if __name__ == '__main__':
    global base_url
    global create_url
    global context
    global fiware_headers
    global API_user
    global API_pass
    global debug
    #Read FIWARE configuration
    with open("config/config.json") as configuration:
        conf = json.load(configuration)
        base_url = conf["base_url"]
        create_url = conf["create_url"]
        context = conf["context"]
        fiware_headers = conf["headers"]
        API_user = conf["API_user"]
        API_pass = conf["API_pass"]
        debug = conf["debug"] == "True"

    # scheduling each second (change to a more reasonable duration)
    # in production
    schedule.every(1).seconds.do(job)

    # infinite loop
    while True:
        schedule.run_pending()
        time.sleep(1)

def test():
    """
    A method only used for testing the uplaod """
    global base_url
    global create_url
    global context
    global fiware_headers
    global API_user
    global API_pass
    global debug
    #Read FIWARE configuration
    with open("config/config.json") as configuration:
        conf = json.load(configuration)
        base_url = conf["base_url"]
        create_url = conf["create_url"]
        context = conf["context"]
        fiware_headers = conf["headers"]
        API_user = conf["API_user"]
        API_pass = conf["API_pass"]
        debug = conf["debug"] == "True"

    obj = {
        'id': 1,
        'user_id': 12,
        'model_id': 101,
        'title': 'Test alert',
        'content': 'Test content of the alert',
        'time': 1649931489368.622
    }
    
    # Construct the entity (Alert) id
    model_id = obj["model_id"]
    entity_id = f"urn:ngsi-ld:Alert:RO-Braila-{model_id_to_sensor[model_id]}-state-analysis-tool" 

    data_model = create_data_model(obj, entity_id)

    postToFiware_context_ld(data_model, entity_id)
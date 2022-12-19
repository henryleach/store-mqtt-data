import paho.mqtt.client as mqtt
import datetime
import os
import sqlite3
import configparser
import logging
from pathlib import Path
import re
import schemas_and_tables as S


# Setup the logger, default to debug, will change in main()
# based on config file values
log_format = "%(asctime)s - %(levelname)s - %(message)s"
# If you're not calling main(), update the desired logging level here.
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(__name__)

LOGGING_LEVELS ={"DEBUG": logging.DEBUG,
                 "INFO": logging.INFO,
                 "WARNING": logging.WARNING,
                 "ERROR": logging.ERROR,
                 "CRITICAL": logging.CRITICAL}

def decode_env_message(msg, env_tables):
    """ Decode an environment MQTT Message to a dict
    for insertion into the DB.
    Expecting a message topic of:
    `env/<environment measurement>/<station_id>`
    and a payload of the relevant value.
    """

    routing = msg.topic.split("/")

    # Check that it's one we can deal with
    if routing[1] not in env_tables.keys():
        logging.warn("Env type {routing[1]} not recognised, and not stored.")
        return 1
    
    # Reformat to a dict we can use for insertion later
    decoded = {"station_id": routing[2],
               "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).timestamp(),
               "measure_type": env_tables[routing[1]]["measure"],
               "measure_value": float(msg.payload.decode()),
               "measure": routing[1]}

    return decoded


def archive_env_measurement(decoded, abs_db_path, env_tables):
    """ Write the decoded reading onto the relevant table of environmental
    measurements
    """

    # Duplicate decoded data slightly, to make the dict and
    # named assignments work.
    decoded[decoded["measure_type"]] = decoded["measure_value"]
    
    table = env_tables[decoded["measure"]]["table"]
             

    insert_statement = (f"INSERT INTO {table.tablename}({table.cols_as_string()}) "
                        f"VALUES({table.named_placeholders()})")

    with sqlite3.connect(abs_db_path) as conn:
        cur = conn.cursor()
        cur.execute(insert_statement, decoded)

    logger.debug(f"Archived {decoded['measure_type']}: {decoded['measure_value']} into {table.tablename}")

    return None

    
def update_env_latest(decoded, abs_db_path, archive_interval_s, env_tables, last_update_table):
    """ Update the lastUpdates table with a value,
    and if update more than archive_interval_s seconds ago
    also the main archive table for that measure type.
    """
    
    # Now just missing last_archive_time_utc, but have to check what the
    # current value in the table is first.

    query = ("SELECT last_archive_time_utc FROM lastUpdates "
             "WHERE station_id == ? "
             "AND measure_type == ?")
    
    with sqlite3.connect(abs_db_path) as conn:
       cur = conn.cursor()
       res = cur.execute(query, (decoded["station_id"], decoded["measure_type"]))

       result = res.fetchall()
       
       if result == []:
           # No result yet
           last_archive = 0
       else:
           last_archive = result[0][0]
       # Exit the DB connection

    if (decoded['timestamp_utc'] - last_archive) > archive_interval_s:
        # means we should also archive these values
        # return None if successful
        archive_failure = archive_env_measurement(decoded, abs_db_path, env_tables)
        if not archive_failure:
            decoded["last_archive_time_utc"] = decoded['timestamp_utc']
        else:
            # failed to archive
            decoded["last_archive_time_utc"] = last_archive

    else:
        decoded["last_archive_time_utc"] = last_archive

    # With the latest update time dict complete, we can
    # update the latest table. Does it make sense to do
    # within the above DB connection, or create a new one as below?

    insert_statement = (f"REPLACE INTO {last_update_table.tablename}"
                        f"({last_update_table.cols_as_string()}) "
                        f"VALUES({last_update_table.named_placeholders()})")

    with sqlite3.connect(abs_db_path) as conn:
       cur = conn.cursor()
       cur.execute(insert_statement, decoded)

    logger.debug(f"updated lastUpdates table")
       
    return None
    

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""

    rc_meanings = {0: "0: Connection Successful",
                   1: "1: Connection Refused - Incorrect Protocol Version",
                   2: "2: Connection Refused - Invalid Client Identifier",
                   3: "3: Connection Refused - Server Unavailable",
                   4: "4: Connection Refused - Bad Username or Password",
                   5: "5: Connection Refused - Not Authorised"}

    if rc in rc_meanings:
        rc_string = rc_meanings[rc]
    else:
        rc_string = f"{rc}: Unknown response"
    
    logger.warning(f"Connected to {client._host}:{client._port} with result: {rc_string}")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # can subscribe to multiple topics by passing a list of tuples, second entry is QoS.
    # Don't wildcard subscribe to everything.

    conn_subs = [(_[0], _[2]) for _ in userdata["subscriptions"]]

    client.subscribe(conn_subs)
    
    return None

def on_env_message(client, userdata, msg):
    """ Write the recorded value to the last Update
        table, and the archive table if enough
        time has elapsed
    """
    
    decoded = decode_env_message(msg, userdata["env_tables"])

    logger.debug(f"Received and decoded: {str(decoded)}")

    update_env_latest(decoded,
                      userdata["db_abs_path"],
                      userdata["archive_interval_s"],
                      userdata["env_tables"],
                      userdata["last_update_table"])

    return None

def archive_gas_reading(msg, gas_table, abs_db_path):
    """ Add the last used volume of gas into the
    archive. 
    """

    routing = msg.topic.split("/")

    decoded = {"timestamp_utc": datetime.datetime.now(datetime.timezone.utc).timestamp(),
               "station_id": routing[2],
               "volume_l": int(msg.payload.decode()),
               "is_meter_reading": False} # always false, other only manually added.

    logger.debug(f"Received and decoded: {str(decoded)}")

    insert_statement = (f"INSERT INTO {gas_table.tablename}("
                        f"{gas_table.cols_as_string()}) "
                        f"VALUES({gas_table.named_placeholders()})")

    with sqlite3.connect(abs_db_path) as conn:
        cur = conn.cursor()
        cur.execute(insert_statement, decoded)

    logger.debug(f"Archived volume_l: {decoded['volume_l']} into {gas_table.tablename}")
        
    return None

def on_gas_message(client, userdata, msg):
    """ Callback function to write a gas
        reading into the database.
        This doesn't add an entry to the lastUpdates table
        Expects a message with a topic in the format:
        `utility/gas/<station_id>`
    """

    archive_gas_reading(msg,
                        userdata["gas_table"],
                        userdata["db_abs_path"])

    return None


def main():
    """ Run an MQTT subscriber indefinitely, storing the information
        in a sqlite3 DB specified in the config file
        `store-data-config.ini` in the calling directory
    """
    
    config = configparser.ConfigParser()

    # Create an absolute path for reading the config file,
    # path is relative to this file's location,
    # otherwise you can get errors with things like cron
    # running it from other directories.
    config_file_name = "store-mqtt-data.conf"
    config_abs_path = Path(__file__).parent / config_file_name
    config.read(str(config_abs_path))

    logging.info(f"Read config file: {str(config_abs_path)}")

    db_path = config.get("storage-settings","db_path", fallback=None)

    if not db_path:
        raise RuntimeError(f"No DB path specified, please add to {config_abs_path}")
    
    db_abs_path = os.path.abspath(db_path)
    logging.info(f"Using database: {db_abs_path}")


    # Now set the user desired log level
    user_log_level = config.get("client", "log_level", fallback="INFO").upper()
    logging.info(f"Now logging at level: {user_log_level}")
    logger.setLevel(LOGGING_LEVELS[user_log_level])
    
    # Collect the subscription topics, callback functions and QoS
    # settings in one place:
    subscriptions = [("env/temp/+", on_env_message, 0),
                     ("env/humidity/+", on_env_message, 0),
                     ("utility/gas/+", on_gas_message, 0)]

    # Create tables in SQLite3DB
    tables_to_create = [S.last_update_table, S.stations_table, S.gas_table]

    tables_to_create += [S.env_tables[_]["table"] for _ in S.env_tables.keys()]

    # Keep track of creation success.
    tables_created = []

    for table in tables_to_create:
        tables_created.append(S.create_table(db_abs_path, table))

    if any(tables_created):
        # Not all tables where created, which will probably have
        # already thrown an exception, but if not:
        raise RuntimeError("Not all tables created in SQLite DB as required.")


    archive_interval_s = config.getint("storage-settings", "archive_interval_s", fallback=600)
    # Various things that we have to make available to all
    # callback functions. Ends up being pretty exhaustive
    # as we have to pass the same object to all functions,
    # if they need all the details or not. 
    client_userdata = {"db_abs_path": db_abs_path,
                       "last_update_table": S.last_update_table,
                       "env_tables": S.env_tables,
                       "gas_table": S.gas_table,
                       "archive_interval_s": archive_interval_s,
                       "subscriptions": subscriptions} 

    client_id = config.get("client", "client_id", fallback=None)
    client = mqtt.Client(client_id=client_id,
                         userdata=client_userdata)

    # set username and password
    uname = config.get("client", "username", fallback=None)
    paswd = config.get("client", "password", fallback=None)
    client.username_pw_set(uname, password=paswd)

    client.on_connect = on_connect

    for sub in subscriptions:
        client.message_callback_add(sub[0], sub[1])

    client.connect(config.get("mqtt_server", "host", fallback="127.0.0.1"),
                   config.getint("mqtt-server", "port", fallback=1883),
                   config.getint("mqtt-server", "timeout", fallback=60))

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()

    
if __name__ == "__main__":
    main()

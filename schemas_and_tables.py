import re
import sqlite3

""" Contains the table class, plus schemas and resulting
    table objects needed for the storing of the home monitoring
    data in the Sqlite3 DB, as well as the create_table()
    function.
"""


class table:
    """ Object to hold a simple definition for an SQLite
    schema and provide some commonly used reformatting
    for the Python sqlite3 module.
    It does no checking that the schema is a valid
    SQLite schema.
    """

    def __init__(self, tablename, schema_string):
        """ Takes a table name and schema as a
        string, e.g.
        `colA INTEGER PRIMARY KEY, colB STRING, colC INTEGER` etc.
        or has `PRIMARY KEY(colA, colB)` defined.
        It's very sensitive to have comma-space separation between
        entries; if there's only a comma it's likely to produce
        weird results.
        """

        self.tablename = tablename
        self.schema = {}
        self.primarykeys = ["rowid"]  # default if no other defined

        # Get the primary key, if defined on a row of its own
        pk_regexp = re.compile("(?i)PRIMARY KEY\((.*?)\)", flags=re.I)

        pk_match = pk_regexp.search(schema_string)

        if pk_match:
            # save the keys
            self.primarykeys = [_.strip() for _ in pk_match.group(1).split(", ")]
            # and remove from the input so it doesn't confuse the rest of the logic
            schema_string = schema_string.replace((", " + pk_match.group(0)), "")
        
        for col in schema_string.split(", "):

            # Then split each column description
            details = col.split(" ")
            colname = details[0].strip()
            coltype = details[1].upper().strip()
            if len(details) > 2:
                colattributes = ' '.join(details[2:]).upper()
                if "PRIMARY KEY" in colattributes:
                    primarykeys = [colname]
            else:
                colattributes = None
                
            self.schema[colname] = {"type": coltype,
                                     "attributes": colattributes}
    
    def colnames(self):
        """ Return a list of column names
        """
        return list(self.schema.keys())

    def cols_and_attribs(self):
        """ Return the column names and their attributes as a list """

        return [" ".join(filter(None, [_, self.schema[_]['type'], self.schema[_]['attributes']])).strip() for _ in self.colnames()]

    def print_schema(self):
        """ Print the schema in SQLite3 compatible format """

        print_form = ", ".join(self.cols_and_attribs())

        if len(self.primarykeys) > 1:
            # means we have more than one PK, so need to add that

            print_form += ", PRIMARY KEY(" + ", ".join(self.primarykeys) + ")"

        return print_form

    def __repr__(self):
        """ Return tablename and columns suitable for following `CREATE TABLE` statement """

        return f"{self.tablename}({self.print_schema()})"
    
    def __str__(self):
        """ Pretty print the table name, column names, types and attributes
        """
        return f"{self.tablename}\n" + ",\n".join(self.cols_and_attribs())

    def cols_as_string(self):
        """ Return the column names in a comma and space
            separated string.
        """

        return ", ".join(self.colnames())
        
    def named_placeholders(self):
        """ Return a string of column names formatted
            as named placeholders as used by the Python sqlite3
            module
        """

        return ":" + ", :".join(self.colnames())


def create_table(abs_db_path:str, table:table)-> None:
    """ Create a table in the named SQLite3 DB from a table object """

    with sqlite3.connect(abs_db_path) as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS {repr(table)}")

    return None

# -- Schemas --

TEMPERATURE_SCHEMA = ("timestamp_utc TIMESTAMP, "
                      "station_id STRING, "
                      "temp_c FLOAT")

HUMIDITY_SCHEMA = ("timestamp_utc TIMESTAMP, "
                   "station_id STRING, "
                   "humidity_pct FLOAT")

TEST_SCHEMA = ("timestamp_utc TIMESTAMP, "
               "topic STRING, "
               "message STRING")

# Contains the info, and operating history
# of and 'stations' (e.g. sensors).
STATION_SCHEMA = ("station_id STRING, "
                  "location STRING, "
                  "sublocation STRING, "
                  "description STRING, "
                  "from_timestamp_utc TIMESTAMP, "
                  "to_timestamp_utc TIMESTAMP, "
                  "is_current BOOLEAN")

# Generic schema to keep the last input of all sensor
# types for quicker access. Only one entry per location.
LAST_UPDATE_SCHEMA = ("station_id STRING NOT NULL, "
                      "timestamp_utc TIMESTAMP, "
                      "measure_type STRING NOT NULL, "
                      "measure_value FLOAT, "
                      "last_archive_time_utc TIMESTAMP, "
                      "PRIMARY KEY(station_id, measure_type)")

# For recording gas meter values
GAS_SCHEMA = ("timestamp_utc TIMESTAMP, "
              "station_id STRING, "
              "volume_l INTEGER, "
              "is_meter_reading BOOLEAN")

# -- Create table objects from schemas:

last_update_table = table("lastUpdates", LAST_UPDATE_SCHEMA)

stations_table = table("stations", STATION_SCHEMA)

env_tables = {"temp": {"table": table("temperature", TEMPERATURE_SCHEMA),
                       "measure": "temp_c"},
              "humidity": {"table": table("humidity", HUMIDITY_SCHEMA),
                           "measure": "humidity_pct"}
              }

gas_table = table("gasUse", GAS_SCHEMA)

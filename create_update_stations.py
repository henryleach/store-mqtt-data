import schemas_and_tables as S
import sqlite3
import datetime
import configparser
import os
import re
import argparse
import csv

def iso_datetime_with_timezone(iso_string):
    """ Will add UTC timezone to a iso string if none
        present and return it as a datetime object.
    """
    # in case it's written with a Z, as this doesn't seem
    # to work for me, even if the docs say it should.
    iso_string = iso_string.replace("Z", "+00:00")
    
    match = re.search("(\+\d\d:\d\d$)", iso_string)

    if not match:
        # no timezone found
        # so add UTC
        iso_string += "+00:00"

    return datetime.datetime.fromisoformat(iso_string)


def add_or_update_station(abs_db_path,
                          station_id,
                          location,
                          sublocation,
                          description=None,
                          from_timestamp_utc=None,
                          to_timestamp_utc=None,
                          current=True):
    """ Add or update a station entry. If start and end dates are given
    then the station is not considered current. If no end date is given
    then the station is considered current, and will add an end date
    to any current entry for that station.
    from_/to_timestamp_utc should be string in isoformat.
    """

    station_data = {"station_id": station_id,
                    "location": location,    
                    "sublocation": sublocation,
                    "description": description}

    if from_timestamp_utc:
        from_timestamp_utc = iso_datetime_with_timezone(from_timestamp_utc)
    else:
        from_timestamp_utc = datetime.datetime.now(datetime.timezone.utc)


    if to_timestamp_utc:
        # Make sure it has a timezone
        to_timestamp_utc = iso_datetime_with_timezone(to_timestamp_utc)

    insert_statement = (f"INSERT INTO {S.stations_table.tablename}"
                        f"({S.stations_table.cols_as_string()}) "
                        f"VALUES({S.stations_table.named_placeholders()})")

    if to_timestamp_utc and not current:
        # Historic entry, so just add it to the table
        station_data["from_timestamp_utc"] = from_timestamp_utc.timestamp()
        station_data["to_timestamp_utc"] = to_timestamp_utc.timestamp()
        station_data["is_current"] = False
        
        with sqlite3.connect(abs_db_path) as conn:
            cur = conn.cursor()
            cur.execute(insert_statement, station_data)

        return None #  exit early, we're done

    # Find if there is a current entry for this station, if so close it.
    select_current = ("SELECT rowid, * FROM stations "
                      "WHERE station_id == ? "
                      "AND is_current == TRUE")

    with sqlite3.connect(abs_db_path) as conn:
        cur = conn.cursor()
        res = cur.execute(select_current, (station_data["station_id"],))
        result = res.fetchall()
        
    if result:
        # Means there was (at least one) current
        # row returned we should mark as no longer current
        
        # Prepare the updated data, using the add time, minus 1 second
        # as the end time of the previous entry.
        closure_timestamp = from_timestamp_utc.timestamp() - 1 # take 1 sec off

        # Could be more than 1, there shouldn't be, but...
        updates = [(closure_timestamp, False, _[0]) for _ in result]

        update_statement = (f"UPDATE {S.stations_table.tablename} SET "
                            "to_timestamp_utc = ?, is_current = ? "
                            "WHERE rowid == ?")

        with sqlite3.connect(abs_db_path) as conn:
                cur = conn.cursor()
                cur.executemany(update_statement, updates)

    # Now add the new, current, entry to the table.
    station_data["from_timestamp_utc"] = from_timestamp_utc.timestamp()
    station_data["to_timestamp_utc"] = None
    station_data["is_current"] = True

    with sqlite3.connect(abs_db_path) as conn:
                cur = conn.cursor()
                cur.execute(insert_statement, station_data)
            
    return None

def stations_from_csv(csv_file_path, restore=False):
    """ TODO: Add rows from a CSV file into the stations table.
        Setting restore to true will delete any existing
        stations table and replace it with the content of the
        csv file, including the rowId primary key.
    """
   
    abs_csv_file = os.path.abspath(csv_file_path)

    with open(abs_csv_file, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        stations = [row for row in reader]

    # This is all text, change the timestamps into floats
    # and the empty strings to None.

    # Is it better to read all the csv data, and then load it
    # via python and executemany(), or just prepare the file
    # and instruct SQLite to read directly from the CSV instead?
        
    return stations

def main():
    """ Allow command line adding of stations, or adding
        of a csv dump from the stations table.
    """

    parser = argparse.ArgumentParser(prog="create_update_stations",
                                     description=("Update home monitoring "
                                                  "station statuses and "
                                                  "location information."))
    parser.add_argument("station_id",
                        help=("Unique station ID of a station"
                              " for which to add entry"))
    parser.add_argument("location",
                        help="Station's location")
    parser.add_argument("-f", "--from_timestamp_utc",
                        help=("UTC time from which the station"
                              " is active in this location. "
                              "Uses current time if not specified. "
                              "ISO Format: `YYYY-MM-DDTHH:MM:SS+00:00` "
                              "If timezone omitted +00:00 is used."))
    parser.add_argument("-s", "--sublocation",
                        help=("Station's sublocation"))
    parser.add_argument("-t", "--to_timestamp_utc",
                        help=("UTC time until which the station"
                              " was at that location. "
                              "Omitting --to_timestamp_utc "
                              "implies that this is the current "
                              "location. "
                              "ISO format required."))
    parser.add_argument("-d", "--description",
                        help=("Optional description of the "
                              "station or location"))
    parser.add_argument("-db", "--database",
                        help=("Path to Sqlite3 DB to update. If "
                              "not specified location defined "
                              "in `store-mqtt-data.conf` "
                              "is used."))

    args = parser.parse_args()

    if args.database:
        db_abs_path = os.path.abspath(args.database)
    else:
        config = configparser.ConfigParser()

        config_file_name = "store-mqtt-data.conf"
        config_abs_path = os.path.abspath(config_file_name)
        config.read(str(config_abs_path))
        db_path = config.get("storage-settings", "db_path", fallback=None)
        abs_db_path = os.path.abspath(db_path)

    if args.to_timestamp_utc:
        current = False
    else:
        current = True
    
    add_or_update_station(abs_db_path,
                          args.station_id,
                          args.location,
                          args.sublocation,
                          description=args.description,
                          from_timestamp_utc=args.from_timestamp_utc,
                          to_timestamp_utc=args.to_timestamp_utc,
                          current=current)
    
if __name__ == "__main__":
    main()

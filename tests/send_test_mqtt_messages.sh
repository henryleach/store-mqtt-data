#!/bin/bash

# Send some sample MQTT messages to an MQTT broker using mosquitto_pub
# to test the python script that saves them to a DB. Reduce the archive
# interval in the config to 2 sec

host="127.0.0.1"
port="1883"
username="mybasicuser"
password="goldiflops"
clientname="Test-Script"

# Expected results for tests:
# 1 temp should be archived and added to lastUpdates
# 2 humidity should be archived and added to lastUpdates
# 3 should be archived in gas readings table
# 4 should be ignored as not one of the configured subscriptions
# 5 temp should update lastUpdates and be archived (assuming the archive interval is short enough)
# 6 should updates lastUpdates but not be archived as it's the same value as the previous measure.

declare -a topics=("env/temp/test_station1"  
	"env/humidity/test_station1"  
	"utility/gas/test_station2"  
	"foo/bar/total/nonsense"  
	"env/temp/test_station1"
	"env/temp/test_station1")   

messages=(20 40 10 99 21 21)

for (( i=0; i<${#topics[@]}; i++))
do
    echo "${topics[$i]} with ${messages[$i]}"
    mosquitto_pub -h "$host" -p "$port" -i "$clientname" -u "$username" -P "$password" -t "${topics[$i]}" -m "${messages[$i]}"
    sleep 3s
done


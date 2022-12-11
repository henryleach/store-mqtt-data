#!/bin/bash

# Send some sample MQTT messages to an MQTT broker using mosquitto_pub
# to test the python script that saves them to a DB.

host="127.0.0.1"
port="1883"
username="mybasicuser"
password="goldiflops"
clientname="Test-Script"

# Expected results for tests:
# 1 should be archived and added to lastUpdates
# 2 should be archived and added to lastUpdates
# 3 should be archived in gas readings table
# should be ignored as not one of the configured subscriptions
# should update lastUpdates, but not be archived as too soon

declare -a topics=("env/temp/test_station1"  
	"env/humidity/test_station1"  
	"utility/gas/test_station2"  
	"foo/bar/total/nonsense"  
	"env/temp/test_station1")   

messages=(20 40 10 99 21)

for (( i=0; i<${#topics[@]}; i++))
do
    echo "${topics[$i]} with ${messages[$i]}"
    mosquitto_pub -h "$host" -p "$port" -i "$clientname" -u "$username" -P "$password" -t "${topics[$i]}" -m "${messages[$i]}"
    sleep 1s
done


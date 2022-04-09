from json import dumps
from kafka import KafkaProducer

import sys
import getopt
import random
from datetime import datetime, timedelta
import time

machine_list = ['mixer', 'oven', 'packing']
sensor_list = ['mass', 'rotation', 'fillpressure', 'bowltemp', 'ovenspeed', 'provetemp', 'oventemp1', 'oventemp2', 'oventemp3', 'cooltemp1', 'cooltemp2', 'packspeed', 'packcounter']
machine_sensors = {'mass':{"machine":'mixer'}, 'rotation':{"machine":'mixer'}, 'fillpressure':{"machine":'mixer'}, 'bowltemp':{"machine":'mixer'}, 'ovenspeed':{"machine":'oven'},
        'provetemp':{"machine":'oven'}, 'oventemp1':{"machine":'oven'}, 'oventemp2':{"machine":'oven'}, 'oventemp3':{"machine":'oven'}, 'cooltemp1':{"machine":'oven'},
        'cooltemp2':{"machine":'oven'}, 'packspeed':{"machine":'packing'}, 'packcounter':{"machine":'packing'}}
sensor_units = {'mass':'kg', 'rotation':'rpm', 'fillpressure':'kPa', 'bowltemp':'DegC', 'ovenspeed':'m/s', 'provetemp':'DegC', 'oventemp1':'DegC',
        'oventemp2':'DegC', 'oventemp3':'DegC', 'cooltemp1':'DegC', 'cooltemp2':'DegC', 'packspeed':'m/s', 'packcounter':'number'}
sensor_mean_values = {'mass':{"mean":150, "stddev":4}, 'rotation':{"mean":80, "stddev":20}, 'fillpressure':{"mean":500, "stddev":60},
        'bowltemp':{"mean":25, "stddev":1}, 'ovenspeed':{"mean":0.2, "stddev":0.05}, 'provetemp':{"mean":28, "stddev":0.5},
        'oventemp1':{"mean":180, "stddev":2}, 'oventemp2':{"mean":180, "stddev":0.5}, 'oventemp3':{"mean":220, "stddev":0.1}, 'cooltemp1':{"mean":15, "stddev":2},
        'cooltemp2':{"mean":15, "stddev":5}, 'packspeed':{"mean":0.2, "stddev":0.05}, 'packcounter':{"mean":5, "stddev":0.1}}

kafkaBrokers = ['192.168.0.10:9092']
kafkaTopic = 'SensorReadings'

producer = KafkaProducer(bootstrap_servers=kafkaBrokers,key_serializer=lambda k: k.encode('ascii','ignore'),value_serializer=lambda x: dumps(x).encode('utf-8'))
argv = sys.argv[1:]

def main():
    print("Starting.... ")
    default_loops = 2
    default_sensor = "all"
    default_speed = "max"
    default_machine = "all"
    default_time = "max"

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'l:s:r:m:t:', ['loop_count=', 'sensor=', 'rate=', 'machine=', 'time='])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    if len(opts) > 3:
        usage()
    else:
        for opt, arg in opts:
            if opt in ("-l", "--loop_count"):
                default_loops = int(arg)
            elif opt in ("-s", "--sensor"):
                if arg in sensor_list:
                    default_sensor = arg
                else:
                    usage_sensor()
                    sys.exit(1)
            elif opt in ("-r", "--rate"):
                default_speed = arg
            elif opt in ("-m", "--machine"):
                default_machine = arg
            elif opt in ("-t", "--time"):
                default_time = int(arg)
            else:
                assert False, "unhandled option"


        default_sensor = default_sensor.lower()
        default_machine = default_machine.lower()

        if not default_time == "max":
            endtime = datetime.now() + timedelta(seconds=default_time)
            while datetime.now() < endtime:
                sensor_messages = simulatedResponse(default_sensor, default_speed, default_machine)

                for sensor_message in sensor_messages:
                    future = producer.send(kafkaTopic, key=sensor_message["sensor"], value=sensor_message)
                    response = future.get(timeout=10)
                    #print(sensor_message)
        else:
            for x in range(default_loops):
                sensor_messages = simulatedResponse(default_sensor, default_speed, default_machine)

                for sensor_message in sensor_messages:
                    future = producer.send(kafkaTopic, key=sensor_message["sensor"], value=sensor_message)
                    response = future.get(timeout=10)
                    #print(sensor_message)
    print("Finishing.... ")

def simulatedResponse(sensor_name, str_rate_sec, machine_name):
    sensor_messages = []
    if not str_rate_sec == "max":
        try:
            rate_sec = float(str_rate_sec)
            if rate_sec > 0:
                time.sleep(1 / rate_sec)
        except:
            pass

    if machine_name == "all":
        for machine in machine_list:
            if sensor_name == "all":
                sensors = get_sensors(machine)
                for sensor, mc in sensors:
                    sensor_record = {
                        "sensor" : sensor,
                        "machine" : machine,
                        "units" : sensor_units[sensor],
                        "time" : int(round(time.time() * 1000)),
                        "value" : round(random.gauss(sensor_mean_values[sensor]["mean"], sensor_mean_values[sensor]["stddev"]),3)
                    }
                    sensor_messages.append(sensor_record)
            else:
                if is_machinesensor(machine, sensor_name):
                    sensor_record = {
                        "sensor" : sensor_name,
                        "machine" : machine,
                        "units" : sensor_units[sensor_name],
                        "time" : int(round(time.time() * 1000)),
                        "value" : round(random.gauss(sensor_mean_values[sensor_name]["mean"], sensor_mean_values[sensor_name]["stddev"]),3)
                    }
                    sensor_messages.append(sensor_record)
    else:
        if sensor_name == "all":
            sensors = get_sensors(machine_name)
            for sensor, mc in sensors:
                sensor_record = {
                    "sensor" : sensor,
                    "machine" : machine_name,
                    "units" : sensor_units[sensor],
                    "time" : int(round(time.time() * 1000)),
                    "value" : round(random.gauss(sensor_mean_values[sensor]["mean"], sensor_mean_values[sensor]["stddev"]),3)
                }
                sensor_messages.append(sensor_record)
        else:
            if is_machinesensor(machine_name, sensor_name):
                sensor_record = {
                    "sensor" : sensor_name,
                    "machine" : machine_name,
                    "units" : sensor_units[sensor_name],
                    "time" : int(round(time.time() * 1000)),
                    "value" : round(random.gauss(sensor_mean_values[sensor_name]["mean"], sensor_mean_values[sensor_name]["stddev"]),3)
                }
                sensor_messages.append(sensor_record)
            else:
                usage_machinesensor()
                sys.exit(0)

    return sensor_messages

def get_sensors(machine_name):
    return_list = []
    for machine_sensor in machine_sensors:
        if machine_sensors[machine_sensor]["machine"] == machine_name:
            return_list.append((machine_sensor, machine_name))
    return return_list

def is_machinesensor(machine_name, sensor_name):
    return_val = False
    sensors = get_sensors(machine_name)
    for sensor, mc in sensors:
        if mc == machine_name and sensor == sensor_name:
            return_val = True
            break
    return return_val

def usage():
    print ('usage: producer.py [-l --loop_count <loop_count>], [-s --sensor <sensor_name>], [-r --rate <readings_per_second>], [-m --machine <machine_Name>]')

def usage_sensor():
    print ('usage: -s --sensor <sensor_name> (sensor_name must be one of ', sensor_list, ')')

def usage_machinesensor():
    print ('usage: machine_name - sensor_name combination must be one of ', machine_sensors)

main()
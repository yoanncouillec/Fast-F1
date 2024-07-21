import json
import time


from influxdb import InfluxDBClient
import os
from dotenv import load_dotenv
import re
import sys
import zlib
import base64

load_dotenv()

influx_client = InfluxDBClient(os.getenv("INFLUX_HOST"), os.getenv("INFLUX_PORT"), os.getenv("INFLUX_USERNAME"), os.getenv("INFLUX_PASSWORD"), os.getenv("INFLUX_DATABASE"), ssl=True, verify_ssl=True)

print(influx_client.get_list_database())


file = open("hungary.txt")
errorcount =0
while 1:
    where = file.tell()
    line = file.readline()
    if not line:
        time.sleep(1)
        file.seek(where)
    else:
        #print(line)
        line = line.replace("'", '"') \
            .replace('True', 'true') \
            .replace('False', 'false')
        try:
            data = json.loads(line)
            print(data)
            if data[0] == "Position.z":
                pass
                # decoded_data = zlib.decompress(base64.b64decode(data[1]), -zlib.MAX_WBITS)
                # json_decoded_data = json.loads(decoded_data)
                # #print(json_decoded_data)
                # positions = json_decoded_data["Position"]
                # for position in positions:
                #     timestamp = position["Timestamp"]
                #     print(timestamp)
                #     entries = position["Entries"]
                #     #print(json.dumps(entries, indent=4))
                #     for car in entries.keys():
                #         car_entry = entries[car]
                #         #print(car_entry)
                #         car_entry_status = car_entry["Status"]
                #         car_entry_x = car_entry["X"]
                #         car_entry_y = car_entry["Y"]
                #         car_entry_z = car_entry["Z"]
                #         json_body = [
                #                 {
                #                     "measurement": "f1.car_position",
                #                     "time": timestamp,
                #                     "tags": {
                #                         "race": "hungary",
                #                         "car": car,
                #                         "axis": "x",
                #                         "status": car_entry_status
                #                     },
                #                     "fields": {
                #                         "value": int(car_entry_x)
                #                     }
                #                 },
                #                 {
                #                     "measurement": "f1.car_position",
                #                     "time": timestamp,
                #                     "tags": {
                #                         "race": "hungary",
                #                         "car": car,
                #                         "axis": "y",
                #                         "status": car_entry_status
                #                     },
                #                     "fields": {
                #                         "value": int(car_entry_y)
                #                     }
                #                 },
                #                 {
                #                     "measurement": "f1.car_position",
                #                     "time": timestamp,
                #                     "tags": {
                #                         "race": "hungary",
                #                         "car": car,
                #                         "axis": "z",
                #                         "status": car_entry_status
                #                     },
                #                     "fields": {
                #                         "value": int(car_entry_z)
                #                     }
                #                 },
                #             ]
                #         influx_client.write_points(json_body)
            elif data[0] == "CarData.z":
                pass
                # decoded_data = zlib.decompress(base64.b64decode(data[1]), -zlib.MAX_WBITS)
                # json_decoded_data = json.loads(decoded_data)
                # print(json_decoded_data)
                # entries = json_decoded_data["Entries"][0]

                # #print(json.dumps(entries, indent=4))
                # timestamp = entries["Utc"]
                # cars = entries["Cars"]
                # for car in cars.keys():
                #     car_data = cars[car]
                #     rpm = car_data["Channels"]["0"]
                #     speed = car_data["Channels"]["2"]
                #     gear = car_data["Channels"]["3"]
                #     throttle = car_data["Channels"]["4"]
                #     brake = car_data["Channels"]["5"]
                #     drs = car_data["Channels"]["45"]
                #     json_body = [
                #         {
                #             "measurement": "f1.car_data",
                #             "time": timestamp,
                #             "tags": {
                #                 "race": "hungary",
                #                 "car": car,
                #                 "channel": 0,
                #                 "label": "rpm"
                #             },
                #             "fields": {
                #                 "value": int(rpm)
                #             }
                #         },
                #         {
                #             "measurement": "f1.car_data",
                #             "time": timestamp,
                #             "tags": {
                #                 "race": "hungary",
                #                 "car": car,
                #                 "channel": 2,
                #                 "label": "speed"
                #             },
                #             "fields": {
                #                 "value": int(speed)
                #             }
                #         },
                #         {
                #             "measurement": "f1.car_data",
                #             "time": timestamp,
                #             "tags": {
                #                 "race": "hungary",
                #                 "car": car,
                #                 "channel": 4,
                #                 "label": "throttle"
                #             },
                #             "fields": {
                #                 "value": int(throttle)
                #             }
                #         },
                #         {
                #             "measurement": "f1.car_data",
                #             "time": timestamp,
                #             "tags": {
                #                 "race": "hungary",
                #                 "car": car,
                #                 "channel": 5,
                #                 "label": "brake"
                #             },
                #             "fields": {
                #                 "value": int(brake)
                #             }
                #         },
                #         {
                #             "measurement": "f1.car_data",
                #             "time": timestamp,
                #             "tags": {
                #                 "race": "hungary",
                #                 "car": car,
                #                 "channel": 45,
                #                 "label": "drs"
                #             },
                #             "fields": {
                #                 "value": int(drs)
                #             }
                #         },
                #         {
                #             "measurement": "f1.car_data",
                #             "time": timestamp,
                #             "tags": {
                #                 "race": "hungary",
                #                 "car": car,
                #                 "channel": 0,
                #                 "label": "rpm"
                #             },
                #             "fields": {
                #                 "value": int(rpm)
                #             }
                #         },
                #     ]
                #     influx_client.write_points(json_body)


                    #print(json.dumps(car_data, indent=4))
                #print(json.dumps(entries, indent=4))
            elif data[0] == "TimingData":
                pass
                # content = data[1]
                # timestamp = data[2]
                # #print(timestamp)
                # lines = content["Lines"]
                # #print(lines)
                # car = list(lines.keys())[0]
                # car_content = lines[car]
                # #print(car)
                # #print(car_content)
                # if "GapToLeader" in car_content.keys() and car_content["GapToLeader"] != "":
                #     gap_to_leader_value = car_content["GapToLeader"]
                #     result_leader = re.search(r"LAP ([0-9]+)", gap_to_leader_value)
                #     result_lapped = re.search(r"([0-9]+) L", gap_to_leader_value)
                #     if result_leader:
                #         #print(result_leader.groups())
                #         lap = int(result_leader.group(1))
                #     elif result_lapped:
                #         lapped = float(result_lapped.group(1))
                #     else:
                #         gap_to_leader_interval = float(gap_to_leader_value)
                #         #print(gap_to_leader_interval)
                #         json_body = []
                #         json_body.append({
                #                 "measurement": "f1.gap_to_leader",
                #                 "time": timestamp,
                #                 "tags": {
                #                     "race": "hungary",
                #                     "car": car,
                #                 },
                #                 "fields": {
                #                     "value": float(gap_to_leader_interval)

                #                 }
                #             })
                #         influx_client.write_points(json_body)

                #     #print(gap_to_leader_value)
                # if "Sectors" in car_content.keys():
                #     sectors = car_content["Sectors"]
                #     #print(sectors)
                #     if '0' in sectors.keys():
                #         sector = 0
                #         sectors_content_0 = sectors['0']
                #         #print(sectors_content_0)
                #         if "Value" in sectors_content_0:
                #             sectors_content_0_value = sectors_content_0["Value"]
                #             if sectors_content_0_value != "":
                #                 #print("{:5s} {:3d} {:10s} {}".format(car, sector, sectors_content_0_value, timestamp))
                #                 json_body = []
                #                 json_body.append({
                #                         "measurement": "f1.sectors",
                #                         "time": timestamp,
                #                         "tags": {
                #                             "race": "hungary",
                #                             "sector": sector,
                #                             "car": car,
                #                         },
                #                         "fields": {
                #                             "value": float(sectors_content_0_value)

                #                         }
                #                     })
                #                 influx_client.write_points(json_body)
                #     if '1' in sectors.keys():
                #         sector = 1
                #         sectors_content_1 = sectors['1']
                #         #print(sectors_content_0)
                #         if "Value" in sectors_content_1:
                #             sectors_content_1_value = sectors_content_1["Value"]
                #             if sectors_content_1_value != "":
                #                 print("{:5s} {:3d} {:10s} {}".format(car, sector, sectors_content_1_value, timestamp))
                #                 json_body = []
                #                 json_body.append({
                #                         "measurement": "f1.sectors",
                #                         "time": timestamp,
                #                         "tags": {
                #                             "race": "hungary",
                #                             "sector": sector,
                #                             "car": car,
                #                         },
                #                         "fields": {
                #                             "value": float(sectors_content_1_value)

                #                         }
                #                     })
                #                 influx_client.write_points(json_body)
                #     if '2' in sectors.keys():
                #         sector = 2
                #         sectors_content_2 = sectors['2']
                #         #print(sectors_content_0)
                #         if "Value" in sectors_content_2:
                #             sectors_content_2_value = sectors_content_2["Value"]
                #             if sectors_content_2_value != "":
                #                 print("{:5s} {:3d} {:10s} {}".format(car, sector, sectors_content_2_value, timestamp))
                #                 json_body = []
                #                 json_body.append({
                #                         "measurement": "f1.sectors",
                #                         "time": timestamp,
                #                         "tags": {
                #                             "race": "hungary",
                #                             "sector": sector,
                #                             "car": car,
                #                         },
                #                         "fields": {
                #                             "value": float(sectors_content_2_value)

                #                         }
                #                     })
                #                 influx_client.write_points(json_body)
        except json.JSONDecodeError:
            errorcount += 1
            continue
        messages = data['M'] if 'M' in data and len(data['M']) > 0 else {}
        #print(messages)
        for inner_data in messages:
            hub = inner_data['H'] if 'H' in inner_data else ''
            if hub.lower() == 'streaming':
                # method = inner_data['M']
                message = inner_data['A']
                #print(message)

        #print(line), # already has newline
        #print(data)

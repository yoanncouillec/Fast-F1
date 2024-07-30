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

influx_client = InfluxDBClient(
    os.getenv("INFLUX_HOST"),
    os.getenv("INFLUX_PORT"),
    os.getenv("INFLUX_USERNAME"),
    os.getenv("INFLUX_PASSWORD"),
    os.getenv("INFLUX_DATABASE"),
    ssl=True,
    verify_ssl=True,
)

print(influx_client.get_list_database())

race = "spa"
session = "quali"
filename = "spa.quali.txt"

def parse_position_z(data):
    decoded_data = zlib.decompress(
        base64.b64decode(data), -zlib.MAX_WBITS
    )
    json_decoded_data = json.loads(decoded_data)
    # print(json_decoded_data)
    positions = json_decoded_data["Position"]
    for position in positions:
        timestamp = position["Timestamp"]
        # print(timestamp)
        entries = position["Entries"]
        # print(json.dumps(entries, indent=4))
        for car in entries.keys():
            car_entry = entries[car]
            # print(car_entry)
            car_entry_status = car_entry["Status"]
            car_entry_x = car_entry["X"]
            car_entry_y = car_entry["Y"]
            car_entry_z = car_entry["Z"]
            json_body = [
                {
                    "measurement": "f1.car_position",
                    "time": timestamp,
                    "tags": {
                        "race": race,
                        "session": session,
                        "car": car,
                        "axis": "x",
                        "status": car_entry_status,
                    },
                    "fields": {"value": int(car_entry_x)},
                },
                {
                    "measurement": "f1.car_position",
                    "time": timestamp,
                    "tags": {
                        "race": race,
                        "session": session,
                        "car": car,
                        "axis": "y",
                        "status": car_entry_status,
                    },
                    "fields": {"value": int(car_entry_y)},
                },
                {
                    "measurement": "f1.car_position",
                    "time": timestamp,
                    "tags": {
                        "race": race,
                        "session": session,
                        "car": car,
                        "axis": "z",
                        "status": car_entry_status,
                    },
                    "fields": {"value": int(car_entry_z)},
                },
            ]
            influx_client.write_points(json_body)

def parse_car_data_z(data):
    decoded_data = zlib.decompress(
        base64.b64decode(data), -zlib.MAX_WBITS
    )
    json_decoded_data = json.loads(decoded_data)
    # print(json_decoded_data)
    entries = json_decoded_data["Entries"][0]

    # print(json.dumps(entries, indent=4))
    timestamp = entries["Utc"]
    cars = entries["Cars"]
    for car in cars.keys():
        car_data = cars[car]
        rpm = car_data["Channels"]["0"]
        speed = car_data["Channels"]["2"]
        gear = car_data["Channels"]["3"]
        throttle = car_data["Channels"]["4"]
        brake = car_data["Channels"]["5"]
        drs = car_data["Channels"]["45"]
        json_body = [
            {
                "measurement": "f1.car_data",
                "time": timestamp,
                "tags": {
                        "race": race,
                        "session": session,
                    "car": car,
                    "channel": 0,
                    "label": "rpm",
                },
                "fields": {"value": int(rpm)},
            },
            {
                "measurement": "f1.car_data",
                "time": timestamp,
                "tags": {
                        "race": race,
                        "session": session,
                    "car": car,
                    "channel": 2,
                    "label": "speed",
                },
                "fields": {"value": int(speed)},
            },
            {
                "measurement": "f1.car_data",
                "time": timestamp,
                "tags": {
                        "race": race,
                        "session": session,
                    "car": car,
                    "channel": 4,
                    "label": "throttle",
                },
                "fields": {"value": int(throttle)},
            },
            {
                "measurement": "f1.car_data",
                "time": timestamp,
                "tags": {
                        "race": race,
                        "session": session,
                    "car": car,
                    "channel": 5,
                    "label": "brake",
                },
                "fields": {"value": int(brake)},
            },
            {
                "measurement": "f1.car_data",
                "time": timestamp,
                "tags": {
                        "race": race,
                        "session": session,
                    "car": car,
                    "channel": 45,
                    "label": "drs",
                },
                "fields": {"value": int(drs)},
            },
            {
                "measurement": "f1.car_data",
                "time": timestamp,
                "tags": {
                        "race": race,
                        "session": session,
                    "car": car,
                    "channel": 0,
                    "label": "rpm",
                },
                "fields": {"value": int(rpm)},
            },
        ]
        influx_client.write_points(json_body)

        # print(json.dumps(car_data, indent=4))
    # print(json.dumps(entries, indent=4))

def parse_timing_data(content, timestamp):
    # print(timestamp)
    lines = content["Lines"]
    # print(lines)
    car = list(lines.keys())[0]
    car_content = lines[car]
    # print(car)
    # print(car_content)
    if (
        "GapToLeader" in car_content.keys()
        and car_content["GapToLeader"] != ""
    ):
        gap_to_leader_value = car_content["GapToLeader"]
        result_leader = re.search(r"LAP ([0-9]+)", gap_to_leader_value)
        result_lapped = re.search(r"([0-9]+) L", gap_to_leader_value)
        if result_leader:
            # print(result_leader.groups())
            lap = int(result_leader.group(1))
        elif result_lapped:
            lapped = float(result_lapped.group(1))
        else:
            gap_to_leader_interval = float(gap_to_leader_value)
            # print(gap_to_leader_interval)
            json_body = []
            json_body.append(
                {
                    "measurement": "f1.gap_to_leader",
                    "time": timestamp,
                    "tags": {
                        "race": race,
                        "session": session,
                        "car": car,
                    },
                    "fields": {"value": float(gap_to_leader_interval)},
                }
            )
            influx_client.write_points(json_body)

        # print(gap_to_leader_value)
    if "Sectors" in car_content.keys():
        sectors = car_content["Sectors"]
        # print(sectors)
        if "0" in sectors.keys():
            sector = 0
            sectors_content_0 = sectors["0"]
            # print(sectors_content_0)
            if "Value" in sectors_content_0:
                sectors_content_0_value = sectors_content_0["Value"]
                if sectors_content_0_value != "":
                    # print("{:5s} {:3d} {:10s} {}".format(car, sector, sectors_content_0_value, timestamp))
                    json_body = []
                    json_body.append(
                        {
                            "measurement": "f1.sectors",
                            "time": timestamp,
                            "tags": {
                                "race": race,
                                "session": session,
                                "sector": sector,
                                "car": car,
                            },
                            "fields": {
                                "value": float(sectors_content_0_value)
                            },
                        }
                    )
                    influx_client.write_points(json_body)
        if "1" in sectors.keys():
            sector = 1
            sectors_content_1 = sectors["1"]
            # print(sectors_content_0)
            if "Value" in sectors_content_1:
                sectors_content_1_value = sectors_content_1["Value"]
                if sectors_content_1_value != "":
                    # print(
                    #     "{:5s} {:3d} {:10s} {}".format(
                    #         car, sector, sectors_content_1_value, timestamp
                    #     )
                    # )
                    json_body = []
                    json_body.append(
                        {
                            "measurement": "f1.sectors",
                            "time": timestamp,
                            "tags": {
                                "race": race,
                                "session": session,
                                "sector": sector,
                                "car": car,
                            },
                            "fields": {
                                "value": float(sectors_content_1_value)
                            },
                        }
                    )
                    influx_client.write_points(json_body)
        if "2" in sectors.keys():
            sector = 2
            sectors_content_2 = sectors["2"]
            # print(sectors_content_0)
            if "Value" in sectors_content_2:
                sectors_content_2_value = sectors_content_2["Value"]
                if sectors_content_2_value != "":
                    # print(
                    #     "{:5s} {:3d} {:10s} {}".format(
                    #         car, sector, sectors_content_2_value, timestamp
                    #     )
                    # )
                    json_body = []
                    json_body.append(
                        {
                            "measurement": "f1.sectors",
                            "time": timestamp,
                            "tags": {
                                "race": race,
                                "session": session,
                                "sector": sector,
                                "car": car,
                            },
                            "fields": {
                                "value": float(sectors_content_2_value)
                            },
                        }
                    )
                    influx_client.write_points(json_body)

def parse_weather_data(data, timestamp):
    #print("WEATHER"+str(data))
    air_temp = data["AirTemp"]
    humidity = data["Humidity"]
    pressure = data["Pressure"]
    rainfall = data["Rainfall"]
    track_temp = data["TrackTemp"]
    wind_direction = data["WindDirection"]
    wind_speed = data["WindSpeed"]
    kf = data["_kf"]
    json_body = [
        {
            "measurement": "f1.weather_data.humidity",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": float(humidity)
            },
        },
        {
            "measurement": "f1.weather_data.air_temp",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": float(air_temp)
            },
        },
        {
            "measurement": "f1.weather_data.pressure",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": float(pressure)
            },
        },
        {
            "measurement": "f1.weather_data.rainfall",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": float(rainfall)
            },
        },
        {
            "measurement": "f1.weather_data.track_temp",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": float(track_temp)
            },
        },
        {
            "measurement": "f1.weather_data.wind_direction",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": float(wind_direction)
            },
        },
        {
            "measurement": "f1.weather_data.wind_speed",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": float(wind_speed)
            },
        },
    ]
    influx_client.write_points(json_body)

def parse_lap_count(data, timestamp):
    current_lap = data["CurrentLap"]
    json_body = [
        {
            "measurement": "f1.lap_count",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
            },
            "fields": {
                "value": int(current_lap)
            },
        },
    ]
    influx_client.write_points(json_body)

def parse_track_status(data, timestamp):
    status = data["Status"]
    message = data["Message"]
    json_body = [
        {
            "measurement": "f1.track_status",
            "time": timestamp,
            "tags": {
                "race": race,
                "session": session,
                "status": status,
                "message": message,
            },
            "fields": {
                "value": 1
            },
        },
    ]
    influx_client.write_points(json_body)

def parse_race_control_messages(data, timestamp):
    messages = data["Messages"]
    print(data)
    json_body = []
    for k in messages.keys():
        message = messages[k]
        category = message["Category"]
        if category == "Flag":
            scope = message["Scope"]
            if scope == "Driver":
                lap = message["Lap"]
                flag = message["Flag"]
                racing_number = message["RacingNumber"]
                content = message["Message"]
                json_body.append(
                    {
                        "measurement": "f1.race_control_messages",
                        "time": timestamp,
                        "tags": {
                            "race": race,
                            "session": session,
                            "lap": lap,
                            "category": category,
                            "flag": flag,
                            "scope": scope,
                            "racing_number": racing_number,
                            "message": content,
                        },
                        "fields": {
                            "value": 1
                        },
                    },
                )
                influx_client.write_points(json_body)
            elif scope == "Track":
                flag = message["Flag"]
                content = message["Message"]
                json_body.append(
                    {
                        "measurement": "f1.race_control_messages",
                        "time": timestamp,
                        "tags": {
                            "race": race,
                            "session": session,
                            "category": category,
                            "flag": flag,
                            "scope": scope,
                            "message": content,
                        },
                        "fields": {
                            "value": 1
                        },
                    },
                )
                influx_client.write_points(json_body)
        elif category == "Other":
            content = message["Message"]
            json_body.append(
                {
                    "measurement": "f1.race_control_messages",
                    "time": timestamp,
                    "tags": {
                        "race": race,
                        "session": session,
                        "category": category,
                        "message": content,
                    },
                    "fields": {
                        "value": 1
                    },
                },
            )
            influx_client.write_points(json_body)    


file = open(filename)

errorcount = 0
while 1:
    where = file.tell()
    line = file.readline()
    if not line:
        time.sleep(1)
        file.seek(where)
    else:
        #print(line)
        line = line.replace("'", '"').replace("True", "true").replace("False", "false")
        try:
            data = json.loads(line)
            #print(data)
            timestamp = data[2]
            print(timestamp)

            if data[0] == "Position.z":
                parse_position_z(data[1])
                pass

            elif data[0] == "CarData.z":
                parse_car_data_z(data[1])
                pass

            elif data[0] == "TimingData":
                #print("TimingData")
                #print(data[1])
                parse_timing_data(data[1], data[2])

            elif data[0] == "TimingStats":
                print("TimingStats")
                #print(data[1])

            elif data[0] == "WeatherData":
                print("WeatherData")
                parse_weather_data(data[1], timestamp)

            elif data[0] == "TopThree":
                print("TopThree")
                #print(data[1])

            elif data[0] == "SessionData":
                print("SessionData")
                #print(data[1])

            elif data[0] == "TrackStatus":
                print("TrackStatus")
                parse_track_status(data[1], data[2])

            elif data[0] == "LapCount":
                print("LapCount")
                #parse_lap_count(data[1], data[2])

            elif data[0] == "TimingAppData":
                print("TimingAppData")
                #print(data[1])

            elif data[0] == "ExtrapolatedClock":
                print("ExtrapolatedClock")
                #print(data[1])

            elif data[0] == "DriverList":
                print("DriverList")
                #print(data[1])

            elif data[0] == "RaceControlMessages":
                print("RaceControlMessages")
                parse_race_control_messages(data[1], data[2])

            elif data[0] == "Hearbeat":
                print("Hearbeat")
                #print(data[1])
                    
        except json.JSONDecodeError:
            errorcount += 1
            continue
        messages = data["M"] if "M" in data and len(data["M"]) > 0 else {}
        # print(messages)
        for inner_data in messages:
            hub = inner_data["H"] if "H" in inner_data else ""
            if hub.lower() == "streaming":
                # method = inner_data['M']
                message = inner_data["A"]
                # print(message)

        # print(line), # already has newline
        # print(data)

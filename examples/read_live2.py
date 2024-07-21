import fastf1
from fastf1.livetiming.data import LiveTimingData

livedata = LiveTimingData('output.txt')
session = fastf1.get_testing_session(2024, 6, 23)
session.load(livedata=livedata)

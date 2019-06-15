from joblib import Parallel, delayed
from scrapper import *


apikey = ""


# Trip preferences
min_days = 5
max_days = 14
min_outbound_date = "2019-07-01"
max_inbound_date = "2020-01-07"

# Airports
origin = "AMS"
destinations = ["CEB", "MNL", "BKK", "HKT", "KUL"]


dates_tuples = generate_dates(
    _min_outbound_date=min_outbound_date,
    _max_inbound_date=max_inbound_date,
    _min_days=min_days,
    _max_days=max_days
)

all_params = []
for destination in destinations:
    for dt in dates_tuples:
        params = {
            "inboundDate": dt[1],
            "cabinClass": "economy",
            "children": 0,
            "infants": 0,
            "groupPricing": "false",
            "country": "ES",
            "currency": "EUR",
            "locale": "en-US",
            "originPlace": origin + "-sky",
            "destinationPlace": destination + "-sky",
            "outboundDate": dt[0],
            "adults": 1
        }

        all_params.append(params)


def launch_make_query(_params):
    make_query(_apikey=apikey, params=_params, wait_time=1, max_retries=3)


print("Total requests:", len(all_params))
all_params = np.random.choice(all_params, size=len(all_params))
Parallel(n_jobs=8, verbose=100)(delayed(make_query)(i) for i in all_params)
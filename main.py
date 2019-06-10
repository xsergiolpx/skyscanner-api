from joblib import Parallel, delayed
from scrapper import *


apikey = "8b389077edmsh58c90253df8b79ap1043d9jsn949bb7116add"


destinations = ["CEB", "MNL", "BKK", "HKT", "KUL"]
# Trip preferences
min_days = 5
max_days = 15
min_outbound_date = "2019-12-18"
max_inbound_date = "2020-01-07"

# Airports
origin = "AMS"

dates_tuples = generate_dates(_min_outbound_date=min_outbound_date,
                              _max_inbound_date=max_inbound_date,
                              _min_days=min_days,
                              _max_days=max_days)

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


# Test query
#test = make_query(_apikey=apikey, params=params, wait_time=1, max_retries=3)
#print(test)


def launch_make_query(_params):
    make_query(_apikey=apikey, params=_params, wait_time=1, max_retries=3)


all_params = np.random.choice(all_params, size=len(all_params))
Parallel(n_jobs=8, verbose=100)(delayed(make_query)(i) for i in all_params)
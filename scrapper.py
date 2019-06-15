import requests
import json
import pandas as pd
import time
from dateutil.parser import parse
import datetime

import multiprocessing
from collections import ChainMap
import numpy as np
import collections


def flatten_dict(fruitColourMapping):
    finalMap = {}
    for d in fruitColourMapping:
        finalMap.update(d)
    return finalMap


def list_of_dicts_2_dict_of_lists(list_of_dicts):
    r = collections.defaultdict(list)

    for d in list_of_dicts:
        for k, v in d.items():
            r[k].append(v)
    return r


def get_session(_apikey, _params):
    url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/v1.0"

    for i in range(5):
        try:
            headers = {"X-RapidAPI-Key": _apikey, "Content-Type": "application/x-www-form-urlencoded"}
            r = requests.post(url, data=_params, headers=headers)
            _session_key = r.headers["Location"].split("/")[-1]
            return _session_key
        except Exception as e:
            print(e)
            time.sleep(2)
    return ""


def generate_dates(_min_outbound_date, _max_inbound_date, _min_days, _max_days):
    _dates_tuples = []
    computed_outbound_date = parse(_min_outbound_date) - datetime.timedelta(days=1)
    while computed_outbound_date <= parse(_max_inbound_date) - datetime.timedelta(days=_min_days):
        computed_outbound_date = computed_outbound_date + datetime.timedelta(days=1)

        for d in range(_min_days, _max_days + 1):
            computed_inbound_date = computed_outbound_date + datetime.timedelta(days=d)
            if computed_inbound_date <= parse(_max_inbound_date):
                _dates_tuples.append((str(computed_outbound_date.date()), str(computed_inbound_date.date())))
    return _dates_tuples


def lookup_id(json_dict, id_lookup, key_dict):
    for l in json_dict[key_dict]:
        if l["Id"] == id_lookup:
            # leg_carriers = []
            return l
    return {}


def extract_info_leg(one_result, response, bound, prefix=""):
    """
    {'Id': '9970-1906301855--32439-2-9451-1907011510',
     'SegmentIds': [46, 47, 48],
     'OriginStation': 9970,
     'DestinationStation': 9451,
     'Departure': '2019-06-30T18:55:00',
     'Arrival': '2019-07-01T15:10:00',
     'Duration': 1515,
     'JourneyMode': 'Flight',
     'Stops': [10342, 11493],
     'Carriers': [954],
     'OperatingCarriers': ['China Southern', 'KLM'],
     'Directionality': 'Inbound',
     'FlightNumbers': [{'FlightNumber': '364', 'CarrierId': 954},
      {'FlightNumber': '661', 'CarrierId': 954},
      {'FlightNumber': '7711', 'CarrierId': 954}]}
    """
    outbound_id = one_result[bound]
    # Get legs info
    leg_outbound_response = lookup_id(response, outbound_id, "Legs")

    # Get carrier names
    leg_outbound_carriers = []
    for carrier in leg_outbound_response["OperatingCarriers"]:
        if type(carrier) == str:
            leg_outbound_carriers.append(carrier)
        else:
            leg_outbound_carriers.append(lookup_id(response, carrier, "Carriers")["Name"])

    leg_outbound_response["OperatingCarriers"] = leg_outbound_carriers

    # Get stop names

    leg_outbound_stops = []

    for stop in leg_outbound_response["Stops"]:
        if type(stop) == str:
            leg_outbound_stops.append(stop)
        else:
            leg_outbound_stops.append(lookup_id(response, stop, "Places")["Code"])

    leg_outbound_response["Stops"] = leg_outbound_stops

    return {prefix + "_" + "Departure": leg_outbound_response["Departure"],
            prefix + "_" + "Arrival": leg_outbound_response["Arrival"],
            prefix + "_" + "Duration": leg_outbound_response["Duration"],
            prefix + "_" + "Stops": leg_outbound_response["Stops"],
            prefix + "_" + 'OperatingCarriers': leg_outbound_response['OperatingCarriers'],
            prefix + "_" + 'NumberStops': len(leg_outbound_response["Stops"]),
            }


def make_query(params, _apikey="8b389077edmsh58c90253df8b79ap1043d9jsn949bb7116add", max_retries=5, wait_time=1):
    filename = str(datetime.datetime.now()) + '__'.join([key + '_' + str(params[key]) for key in params]) + '.parquet'
    session_key = get_session(_apikey, params)
    url_results = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/uk2/v1.0/" + session_key + "?sortType=price&sortOrder=asc&pageIndex=0&pageSize=50"
    prices = []
    list_info_result = []

    for i in range(max_retries):
        if i > 0: print("Retrying...", i)
        time.sleep(wait_time)
        try:
            r = requests.get(url_results, headers={"X-RapidAPI-Key": _apikey})
            response_json = json.loads(r.text)

            for result in response_json["Itineraries"]:
                dict_inbound_leg = extract_info_leg(result, response_json, "InboundLegId", "inbound")
                dict_outbound_leg = extract_info_leg(result, response_json, "OutboundLegId", "outbound")
                price = result['PricingOptions'][0]["Price"]
                prices.append(price)
                list_this_loop = [{"price": price,
                                   "originPlace": params["originPlace"].replace("-sky", ""),
                                   "destinationPlace": params["destinationPlace"].replace("-sky", "")},
                                  dict_inbound_leg,
                                  dict_outbound_leg
                                  ]
                list_info_result.append(list_this_loop)

            if len(prices) > 0:
                df_return = pd.DataFrame(
                    list_of_dicts_2_dict_of_lists([flatten_dict(d) for d in list_info_result])).sort_values("price")
                df_return["query_date"] = str(datetime.datetime.now())
                df_return.to_parquet('data/' + filename, compression="gzip")
                return 0

        except Exception as e:
            print(e)

    #
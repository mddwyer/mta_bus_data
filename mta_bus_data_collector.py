#!/usr/bin/env python3

"""
#########################
MTA Bus Data Collector
#########################

:Description:
    Queries MTA bus time developer API using locally stored API key. Script runs every minute
    for a day on user specified bus line. 

:Output:
    Locally stored CSV of a day of bus locations. 

:Usage:
    Run on server or locally

:Libraries and Dependencies:
    - Python 3.5 or higher
    - pandas
    - datetime
    - urllib
    - json
    - time


Written by Matt Dwyer (dwyer.mattd@gmail.com)
"""

import pandas as pd
import datetime as dt
import urllib.request
import json
import time


def collect_data(bus_line):
    """
    :Description:
        Requests JSONs from MTA bus time website, using local API key.
    :Params:
        bus_line (str): Name of the bus line.
    :Returns:
        List of JSONs, each bus being an element of the list.
    :Dependencies:
        - urllib
        - json
    :Notes:
        Relies on api_key.txt file in same folder as script.
    :Example:
        api_data = collect_data('B38')
    """
    
    with open('api_key.txt', 'r') as file:
        key = file.read()

    url = 'http://bustime.mta.info/api/siri/vehicle-monitoring.json?key=' + key + '&VehicleMonitoringDetailLevel=calls&LineRef=' + bus_line

    api_data = urllib.request.urlopen(url)
    api_data = api_data.read().decode('utf-8')
    api_data = json.loads(api_data)

    api_data = api_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']

    return api_data


def process_data(api_data):
    """
    :Description:
        Cycles through buses in list of JSONs, pulls relevants fields, and appends data into 
        pandas dataframe.
    :Params:
        api_data (list): List of JSONs
    :Returns:
        Pandas DataFrame.
    :Dependencies:
        - pandas
        - datetime
    :Example:
        df = process_data(api_data)
    """

    df = pd.DataFrame()

    for bus_num in range(len(api_data)):

        df_temp = pd.DataFrame()

        bus_data = api_data[bus_num]
        
        df_temp['bus_line'] = [bus_data['MonitoredVehicleJourney']['LineRef']]
        df_temp['date_time'] = [bus_data['RecordedAtTime']]
        df_temp['lat'] = [bus_data['MonitoredVehicleJourney']['VehicleLocation']['Latitude']]
        df_temp['lon'] = [bus_data['MonitoredVehicleJourney']['VehicleLocation']['Longitude']]
        df_temp['vehicle_id'] = [bus_data['MonitoredVehicleJourney']['VehicleRef']]

        df = df.append(df_temp, ignore_index=True)

    df['date_time'] = pd.to_datetime(df['date_time']).dt.tz_localize(None)

    return df


if __name__ == '__main__':

    date_str = dt.datetime.today().strftime('%Y-%m-%d')

    bus_line = 'B38'

    print('Running script on ' + date_str + ' for ' + bus_line)

    df = pd.DataFrame()

    # run for 1 day at 30 second intervals
    for i in range(2880):

        try:

            api_data = collect_data(bus_line)

            df_temp = process_data(api_data)

            df = df.append(df_temp, ignore_index=True)

            print(i, len(df))

            time.sleep(30)
            
        except:

            continue

    df.to_csv('mta_bus_data_' + bus_line + '_' + date_str + '.csv', index=False)


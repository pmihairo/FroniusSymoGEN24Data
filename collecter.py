import requests
import pprint
import time
import pandas as pd
import sqlite3
import logging
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime


# collector.py
# This will connect to the Fronius Symo and log data to a sqlite
# database
# Make sure you add ip.ip.ip.ip fronius to your /etc/hosts file or
# Set the variable hostname to your Symo's ip address or hostname
#
# This will create a sqlite db called fronius.sqlite and add
# two tables called Site & Inverters
# It will then start logging data every 5 seconds
# Todo:
# 1. Error Handling
# 2. CLean up exit - use a signal handler or something


hostname = "fronius"
Influx_url = "http://influxdb:8086"
Influx_token = "6KafZNXsMJYnMEujxgwq8jUqLf7b1IMdvXuZhLL7G3fl5mUhkRJZvQXue0AORJr6DwE7oZ-8JhnePs_3c83pZQ=="
Influx_org = "Mihais Org"
Influx_site_bucket = "SiteBucket"
Influx_meter_bucket = "MeterBucket"


def getData(hostname,dataRequest):
    """
    All Request's come via this function.  It builds the url from args
    hostname and dataRequest.  It is advised to have a fronius hostname
    entry in /etc/hosts.  There is no authentication required, it is assumed
    you are on a local, private network.
    """
    try:
        url = "http://" + hostname + dataRequest
        r = requests.get(url,timeout=60)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.Timeout:
        print("Request: {} failed ".format(url))
    except requests.exceptions.RequestException as e:
        print("Request failed with {}".format(e))

    exit()




def GetPowerFlowRealtimeData():
    """
    This request provides detailed information about the local energy grid.
    The values replied represent the current state. Because of data has multiple
    asynchrone origins it is a matter of facts that the sum of all
    powers (grid, load and generate) will differ from zero.
    """
    dataRq = '/solar_api/v1/GetPowerFlowRealtimeData.fcgi'
    return getData(hostname,dataRq)

def GetMetersRealtimeData():
    """
    This request provides detailed information about the local energy grid from the meter.
    The values replied represent the current state. Because of data has multiple
    asynchrone origins it is a matter of facts that the sum of all
    powers (grid, load and generate) will differ from zero.
    """
    dataRq = '/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System'
    return getData(hostname,dataRq)



def PowerFlowRealtimeData(jPFRD):
# Collect the Inverter Data
# Does not include Optional Fields at this time
    Inverters = dict()
    Site = dict()
# There could be more than 1 inverter here -  Bitcoin Miners :)
    for i in jPFRD['Body']['Data']['Inverters']:
        Inverters['DeviceId'] = i
        Inverters['DT'] = jPFRD['Body']['Data']['Inverters'][i]['DT']
        Inverters['P'] = jPFRD['Body']['Data']['Inverters'][i]['P']

# Collect Site data (single row)
        Site['Timestamp'] = jPFRD['Head']['Timestamp']
        Site['Version'] = jPFRD['Body']['Data']['Version']
        Site['E_Day'] = jPFRD['Body']['Data']['Site']['E_Day']
        Site['E_Total'] = jPFRD['Body']['Data']['Site']['E_Total']
        Site['E_Year'] = jPFRD['Body']['Data']['Site']['E_Year']
        Site['Meter_Location'] = jPFRD['Body']['Data']['Site']['Meter_Location']
        Site['Mode'] = jPFRD['Body']['Data']['Site']['Mode']
        Site['P_Akku'] = jPFRD['Body']['Data']['Site']['P_Akku']
# TODO: Make Site(P_Akku) not 'None' 
        Site['P_Grid'] = jPFRD['Body']['Data']['Site']['P_Grid']
        Site['P_Load'] = jPFRD['Body']['Data']['Site']['P_Load']
        Site['P_PV'] = jPFRD['Body']['Data']['Site']['P_PV']
        Site['rel_Autonomy'] = jPFRD['Body']['Data']['Site']['rel_Autonomy']
        Site['rel_SelfConsumption'] = jPFRD['Body']['Data']['Site']['rel_SelfConsumption']
    return [Site, Inverters]


def MetersRealtimeData(jPFRD):
# Collect the Inverter Data
# Does not include Optional Fields at this time
    Meters = dict()
# There could be more than 1 inverter here -  Bitcoin Miners :)
    for i in jPFRD['Body']['Data']:
        Meters['Timestamp'] = jPFRD['Head']['Timestamp']
        Meters['DeviceId'] = i        
        Meters['Current_L1'] = jPFRD['Body']['Data'][i]['ACBRIDGE_CURRENT_ACTIVE_MEAN_01_F32']
        Meters['Current_L2'] = jPFRD['Body']['Data'][i]['ACBRIDGE_CURRENT_ACTIVE_MEAN_02_F32']
        Meters['Current_L3'] = jPFRD['Body']['Data'][i]['ACBRIDGE_CURRENT_ACTIVE_MEAN_03_F32']
        Meters['Current_Total'] = jPFRD['Body']['Data'][i]['ACBRIDGE_CURRENT_AC_SUM_NOW_F64']
        Meters['Voltage_L12'] = jPFRD['Body']['Data'][i]['ACBRIDGE_VOLTAGE_MEAN_12_F32']
        Meters['Voltage_L23'] = jPFRD['Body']['Data'][i]['ACBRIDGE_VOLTAGE_MEAN_23_F32']
        Meters['Voltage_L31'] = jPFRD['Body']['Data'][i]['ACBRIDGE_VOLTAGE_MEAN_31_F32']
        Meters['Comp_Mode_Enable_U16'] = jPFRD['Body']['Data'][i]['COMPONENTS_MODE_ENABLE_U16']
        Meters['Comp_Mode_Visible_U16'] = jPFRD['Body']['Data'][i]['COMPONENTS_MODE_VISIBLE_U16']
        Meters['Comp_TimeStamp'] = jPFRD['Body']['Data'][i]['COMPONENTS_TIME_STAMP_U64']
        Meters['Manufacturer'] = jPFRD['Body']['Data'][i]['Details']['Manufacturer']
        Meters['Model'] = jPFRD['Body']['Data'][i]['Details']['Model']
        Meters['Serial'] = jPFRD['Body']['Data'][i]['Details']['Serial']
        Meters['Grid_Frequency'] = jPFRD['Body']['Data'][i]['GRID_FREQUENCY_MEAN_F32']
        Meters['EnergyActiveMinus'] = jPFRD['Body']['Data'][i]['SMARTMETER_ENERGYACTIVE_ABSOLUT_MINUS_F64']
        Meters['EnergyActivePlus'] = jPFRD['Body']['Data'][i]['SMARTMETER_ENERGYACTIVE_ABSOLUT_PLUS_F64']
        Meters['EnergyActiveConsumed'] = jPFRD['Body']['Data'][i]['SMARTMETER_ENERGYACTIVE_CONSUMED_SUM_F64']
        Meters['EnergyActiveProduced'] = jPFRD['Body']['Data'][i]['SMARTMETER_ENERGYACTIVE_PRODUCED_SUM_F64']
        Meters['EnergyReActiveConsumed'] = jPFRD['Body']['Data'][i]['SMARTMETER_ENERGYREACTIVE_CONSUMED_SUM_F64']
        Meters['EnergyReActiveProduced'] = jPFRD['Body']['Data'][i]['SMARTMETER_ENERGYREACTIVE_PRODUCED_SUM_F64']
        Meters['PowerFactorL1'] = jPFRD['Body']['Data'][i]['SMARTMETER_FACTOR_POWER_01_F64']
        Meters['PowerFactorL2'] = jPFRD['Body']['Data'][i]['SMARTMETER_FACTOR_POWER_02_F64']
        Meters['PowerFactorL3'] = jPFRD['Body']['Data'][i]['SMARTMETER_FACTOR_POWER_03_F64']
        Meters['PowerFactorTotal'] = jPFRD['Body']['Data'][i]['SMARTMETER_FACTOR_POWER_SUM_F64']
        Meters['PowerActiveL1'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERACTIVE_01_F64']
        Meters['PowerActiveL2'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERACTIVE_02_F64']
        Meters['PowerActiveL3'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERACTIVE_03_F64']
        Meters['PowerActiveMeanL1'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERACTIVE_MEAN_01_F64']
        Meters['PowerActiveMeanL2'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERACTIVE_MEAN_02_F64']
        Meters['PowerActiveMeanL3'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERACTIVE_MEAN_03_F64']
        Meters['PowerActiveMeanSum'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERACTIVE_MEAN_SUM_F64']
        Meters['PowerApparentL1'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERAPPARENT_01_F64']
        Meters['PowerApparentL2'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERAPPARENT_02_F64']
        Meters['PowerApparentL3'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERAPPARENT_03_F64']
        Meters['PowerApparentSum'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERAPPARENT_MEAN_SUM_F64']
        Meters['PowerReActiveL1'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERREACTIVE_01_F64']
        Meters['PowerReActiveL2'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERREACTIVE_02_F64']
        Meters['PowerReActiveL3'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERREACTIVE_03_F64']
        Meters['PowerReActiveMeanSum'] = jPFRD['Body']['Data'][i]['SMARTMETER_POWERREACTIVE_MEAN_SUM_F64']
        Meters['SmartMeterLocation'] = jPFRD['Body']['Data'][i]['SMARTMETER_VALUE_LOCATION_U16']
        Meters['VoltageL1'] = jPFRD['Body']['Data'][i]['SMARTMETER_VOLTAGE_01_F64']
        Meters['VoltageL2'] = jPFRD['Body']['Data'][i]['SMARTMETER_VOLTAGE_02_F64']
        Meters['VoltageL3'] = jPFRD['Body']['Data'][i]['SMARTMETER_VOLTAGE_03_F64']
        Meters['VoltageMeanL1'] = jPFRD['Body']['Data'][i]['SMARTMETER_VOLTAGE_MEAN_01_F64']
        Meters['VoltageMeanL2'] = jPFRD['Body']['Data'][i]['SMARTMETER_VOLTAGE_MEAN_02_F64']
        Meters['VoltageMeanL3'] = jPFRD['Body']['Data'][i]['SMARTMETER_VOLTAGE_MEAN_03_F64']


    return [Meters]



### Just Initial Testing Code
def TestPowerFlowRealtimeData():
    client = InfluxDBClient(url=Influx_url, token=Influx_token, org=Influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    pp = pprint.PrettyPrinter(indent=4)
    cnt = 0
    while cnt < 3:
        cnt = cnt + 1
        Site, Inverters = PowerFlowRealtimeData(GetPowerFlowRealtimeData())
        Meters = MetersRealtimeData(GetMetersRealtimeData())
#        pp.pprint(Site)
#        pp.pprint(Inverters)
#        pp.pprint(Meters)
        print (str(Site))


        time.sleep(3)



def initSQL():
    cn = sqlite3.connect("Fronius.sqlite")
    return cn

def InitPowerFlowRealtimeData(cn):

    # Setup
    # Initialise the DataFrames use pandas to setUp the tables initially
    # This is being lazy, build a proper CREATE

    Site, Inverters = PowerFlowRealtimeData(GetPowerFlowRealtimeData())
    dSite = pd.DataFrame(data=Site,index=[0])
    dSite.reset_index()
    dInverters = pd.DataFrame(data=Inverters,index=[0])
    dInverters.reset_index()
    dMeters = pd.DataFrame(data=Inverters,index=[0])
    dMeters.reset_index()

    dSite.to_sql("Site",cn,if_exists="append")
    dInverters.to_sql("Inverters",cn,if_exists="append")
    dMeters.to_sql("Meters",cn,if_exists="append")
    return [dSite, dInverters, dMeters]

def writeSQL(cn,cur,table,row):
    columns = ', '.join(row.keys())
    placeholders = ':'+', :'.join(row.keys())
    query = 'INSERT INTO %s (%s) VALUES (%s)' % (table,columns, placeholders)
    cur.execute(query, row)
    cn.commit()

def mainDB():
    cn = initSQL()
    cur = cn.cursor()
    dSite, dInverters, dMeters = InitPowerFlowRealtimeData(cn)
    while True:
        try:
            Site, Inverters = PowerFlowRealtimeData(GetPowerFlowRealtimeData())
            Meters = MetersRealtimeData(GetMetersRealtimeData())

            writeSQL(cn,cur,table="Site",row=Site)
            writeSQL(cn,cur,table="Inverters",row=Inverters)
            writeSQL(cn,cur,table="Meters",row=Meters)

            # Loop every 5 seconds
            print(str(Site['Timestamp']) + ' Load: ' + str(Site['P_Load']) )
            time.sleep(5)
        except:
            time.sleep(60)
            print("sleeping")
    cn.close()
        
def main():
    client = InfluxDBClient(url=Influx_url, token=Influx_token, org=Influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    while True:
        try:
            Site, Inverters = PowerFlowRealtimeData(GetPowerFlowRealtimeData())
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            write_api.write(Influx_site_bucket, Influx_org, 
                [{
                "measurement": "SiteValues", 
                "tags": {"location": "home", "Version": Site['Version']}, 
                "fields": 
                    {
                    "P_Akku": Site['P_Akku'], 
                    "P_Grid": Site['P_Grid'], 
                    "P_PV": Site['P_PV'], 
                    "P_Load": Site['P_Load'],
                    "rel_Autonomy": Site['rel_Autonomy'],
                    "rel_SelfConsumption": Site['rel_SelfConsumption']
                    }, 
                "time": str(now)}
                ])
            time.sleep(2)

            Meters = MetersRealtimeData(GetMetersRealtimeData())

            for i in range(len(Meters)):
                now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                write_api.write(Influx_meter_bucket, Influx_org, 
                    [{
                    "measurement": "MeterValues", 
                    "tags": 
                        {
                        "location": "home", 
                        "MeterManufacturer": Meters[i]['Manufacturer'], 
                        "MeterModel": Meters[i]['Model'], 
                        "MeterSerial": Meters[i]['Serial']                    }, 
                    "fields": 
                        {
                        "Current_L1": Meters[i]['Current_L1'], 
                        "Current_L2": Meters[i]['Current_L2'], 
                        "Current_L3": Meters[i]['Current_L3'], 
                        "Current_Total": Meters[i]['Current_Total'], 

                        "Voltage_L12": Meters[i]['Voltage_L12'], 
                        "Voltage_L23": Meters[i]['Voltage_L23'], 
                        "Voltage_L31": Meters[i]['Voltage_L31'], 

                        "Grid_Frequency": Meters[i]['Grid_Frequency'], 

                        "EnergyActiveMinus": Meters[i]['EnergyActiveMinus'], 
                        "EnergyActivePlus": Meters[i]['EnergyActivePlus'], 

                        "EnergyActiveConsumed": Meters[i]['EnergyActiveConsumed'], 
                        "EnergyActiveProduced": Meters[i]['EnergyActiveProduced'], 

                        "EnergyReActiveConsumed": Meters[i]['EnergyReActiveConsumed'], 
                        "EnergyReActiveProduced": Meters[i]['EnergyReActiveProduced'], 

                        "PowerFactorL1": Meters[i]['PowerFactorL1'], 
                        "PowerFactorL2": Meters[i]['PowerFactorL2'], 
                        "PowerFactorL3": Meters[i]['PowerFactorL3'], 
                        "PowerFactorTotal": Meters[i]['PowerFactorTotal'], 

                        "PowerActiveL1": Meters[i]['PowerActiveL1'], 
                        "PowerActiveL2": Meters[i]['PowerActiveL2'], 
                        "PowerActiveL3": Meters[i]['PowerActiveL3'], 

                        "PowerActiveMeanL1": Meters[i]['PowerActiveMeanL1'], 
                        "PowerActiveMeanL2": Meters[i]['PowerActiveMeanL2'], 
                        "PowerActiveMeanL3": Meters[i]['PowerActiveMeanL3'], 
                        "PowerActiveMeanSum": Meters[i]['PowerActiveMeanSum'], 

                        "PowerApparentL1": Meters[i]['PowerApparentL1'], 
                        "PowerApparentL2": Meters[i]['PowerApparentL2'], 
                        "PowerApparentL3": Meters[i]['PowerApparentL3'], 
                        "PowerApparentSum": Meters[i]['PowerApparentSum'], 

                        "PowerReActiveL1": Meters[i]['PowerReActiveL1'], 
                        "PowerReActiveL2": Meters[i]['PowerReActiveL2'], 
                        "PowerReActiveL3": Meters[i]['PowerReActiveL3'], 
                        "PowerReActiveMeanSum": Meters[i]['PowerReActiveMeanSum'], 

                        "VoltageL1": Meters[i]['VoltageL1'], 
                        "VoltageL2": Meters[i]['VoltageL2'], 
                        "VoltageL3": Meters[i]['VoltageL3'], 

                        "VoltageMeanL1": Meters[i]['VoltageMeanL1'], 
                        "VoltageMeanL2": Meters[i]['VoltageMeanL2'], 
                        "VoltageMeanL3": Meters[i]['VoltageMeanL3']

                        }, 
                    "time": str(now)}
                    ])            
            time.sleep(3)
        except:
            time.sleep(60)
            print("sleeping")


if __name__ == "__main__":
#    mainDB()
    main()
#    TestPowerFlowRealtimeData()



#pd.read_sql_query("SELECT * from Inverters", cn)
#pd.read_sql_query("SELECT * from Site", cn)


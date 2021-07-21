# FroniusSymoGEN24Data
- Extracts data from Fronius SYMO GEN24 and the smart meter over the solar rest api and sends it to an InfluxDB. You can connect graphana to that InfluxDB and create pretty graphs. 
- Assumes the Fronius setup is On-Grid and you have also the Fronius SmartMeter.
- Reads and saves data for all 3 phases sepparately.
- Polls data every 5 seconds. 
- Uses some undocumented methods, so may stop working if Fronius updates their software. 
- Works well with Fronius software V12.

You will need to:
- Have fronius resolved by local host (add to /etc/hosts), or modify the constant. 
- Have an InfluxDB set up and replace the URL, token and other data (Org, Site Bucket, Meter Bucket) in the file. 

ToDo:
- If you have more than one meter, more than one inverter, make sure that all data collection works. This is tested with only one meter and one inverter. 

Based off https://github.com/lxrocks/fronius (Thanks!!)
# FroniusSymoGEN24Data

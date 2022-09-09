from influxdb import DataFrameClient
import time

host = '203.64.131.98'
port = 8086
user = 'sam'
password = '12345678'
dbname = 'SensorData'
dbname1 = 'Predicted'

client = DataFrameClient(host, port, user, password, dbname)


temp = client.query('SELECT last("value") FROM "temperature" WHERE time > now() - 7d GROUP BY time(1m);')
humid = client.query('SELECT last("value") FROM "humidity" WHERE time > now() - 7d GROUP BY time(1m);') 
soil = client.query('SELECT last("value") FROM "SoilMoisture" WHERE time > now() - 7d GROUP BY time(1m);')
ec = client.query('SELECT last("value") FROM "conductivity" WHERE time > now() - 7d GROUP BY time(1m);')

t=temp['temperature'].dropna()
h=humid['humidity'].dropna()
s=soil['SoilMoisture'].dropna()
ec=ec['conductivity'].dropna()
new_df1=t.loc[(t['last'] >= 20)]
new_df2=h.loc[(h['last'] >= 40)]
new_df3=s.loc[(s['last'] >= 20) & (s['last'] <=80)]
new_df4=ec.loc[(ec['last'] >= 100)]

new_df1.to_csv("1temp.csv")
new_df2.to_csv("1humid.csv")
new_df3.to_csv("1soil.csv")
new_df4.to_csv("1ec.csv")




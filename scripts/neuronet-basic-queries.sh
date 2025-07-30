#!/bin/bash
#
since=-10m
date=`date "+%Y%m%d_%H%M%S"`

#GET proxmox
curl --location 'http://10.255.40.16:8086/api/v2/query?org=nextworks' \
--header 'Authorization: Token AK35JfgefqFN96DGYjoprx6M-UFk05AZDIAeizRVeINXiD42CVExfZ_afXuLvCSFHvyOZS6MWPVz8itjfEkKzQ==' \
--header 'Content-Type: application/vnd.flux' \
--data '
from(bucket: "monitoring")
  |> range(start: '$since')
  |> filter(fn: (r) => 
  r.vm_name =~ /^neuronet-/ and 
  r.plugin == "proxmox")
' > proxmoxData_$date$since.csv

#FILTER inventory-server-id
cut -f12,17 -d"," proxmoxData_$date$since.csv | grep -ve inventory-server-id | sort | uniq | grep -ve '^[[:space:]]*$' > hostData_$date$since.csv


#READ host
declare -a inventory_ids=()

while IFS=',' read -r id _; do
  inventory_ids+=("$id")
done < "hostData_${date}${since}.csv"

#####START FOR
#GET pdu
for id in "${inventory_ids[@]}"; do
curl --location 'http://10.255.40.16:8086/api/v2/query?org=nextworks' \
--header 'Authorization: Token AK35JfgefqFN96DGYjoprx6M-UFk05AZDIAeizRVeINXiD42CVExfZ_afXuLvCSFHvyOZS6MWPVz8itjfEkKzQ==' \
--header 'Content-Type: application/vnd.flux' \
--data '
from(bucket: "monitoring")
  |> range(start: '$since')
  |> filter(fn: (r) => 
  r.plugin == "pdu" and
  r["inventory-server-id"] == "'$id'" 
  )
' >> pduData_$date$since.csv


#GET scaphandre HOST
curl --location 'http://10.255.40.16:8086/api/v2/query?org=nextworks' \
--header 'Authorization: Token AK35JfgefqFN96DGYjoprx6M-UFk05AZDIAeizRVeINXiD42CVExfZ_afXuLvCSFHvyOZS6MWPVz8itjfEkKzQ==' \
--header 'Content-Type: application/vnd.flux' \
--data '
from(bucket: "monitoring")
  |> range(start: -10m)
  |> filter(fn: (r) =>
  r.plugin == "scaphandre" and
  r._field == "scaph_host_power_microwatts" and
  r.url =~ /'$id'/
  )
' >> scaphandreHostData_$date$since.csv
done
#####END FOR

#GET scaphandre VMs
curl --location 'http://10.255.40.16:8086/api/v2/query?org=nextworks' \
--header 'Authorization: Token AK35JfgefqFN96DGYjoprx6M-UFk05AZDIAeizRVeINXiD42CVExfZ_afXuLvCSFHvyOZS6MWPVz8itjfEkKzQ==' \
--header 'Content-Type: application/vnd.flux' \
--data '
from(bucket: "monitoring")
  |> range(start: '$since')
  |> filter(fn: (r) => 
  r.vm_name =~ /^neuronet-/ and 
  r.plugin == "scaphandre"
  )
' > scaphandreVmData_$date$since.csv


#GET k8s
curl --location 'http://10.255.40.16:8086/api/v2/query?org=nextworks' \
--header 'Authorization: Token AK35JfgefqFN96DGYjoprx6M-UFk05AZDIAeizRVeINXiD42CVExfZ_afXuLvCSFHvyOZS6MWPVz8itjfEkKzQ==' \
--header 'Content-Type: application/vnd.flux' \
--data '
from(bucket: "monitoring")
  |> range(start: '$since')
  |> filter(fn: (r) =>
  r.plugin == "k8s" and
  r["inventory-server-id"] =~ /Neuronet/)
' > k8sData_$date$since.csv


#GET kepler
curl --location 'http://10.255.40.16:8086/api/v2/query?org=nextworks' \
--header 'Authorization: Token AK35JfgefqFN96DGYjoprx6M-UFk05AZDIAeizRVeINXiD42CVExfZ_afXuLvCSFHvyOZS6MWPVz8itjfEkKzQ==' \
--header 'Content-Type: application/vnd.flux' \
--data '
from(bucket: "monitoring")
  |> range(start: '$since')
  |> filter(fn: (r) => 
  r.plugin == "kepler" and
  r["inventory-server-id"] =~ /Neuronet/)
' > keplerData_$date$since.csv

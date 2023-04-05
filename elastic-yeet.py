from elasticsearch import Elasticsearch, helpers
import csv
import requests
import sys
import urllib3
import time
urllib3.disable_warnings()
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("file",nargs=1,help="Filename")
parser.add_argument("--server",nargs="?",default="https://localhost:9200",help="Elastic Server address: https://localhost:9200")
parser.add_argument("--auth",nargs=2,default=['elastic','changeme'],help="User and Password seperated by space, requires to values: admin admin")
args = parser.parse_args()
try:
    es = Elasticsearch(hosts=args.server, verify_certs=False, basic_auth=(args.auth[0], args.auth[1]))
    csvname = args.file[0] 
    print("Attempting to yeet the file: "+csvname)
    indexname = csvname.split('.')[0]
    with open(csvname,'r') as csvstuff:
        reader = csv.DictReader(csvstuff)
        helpers.bulk(es, reader, index=indexname)
    print('Gotta let elasticsearch warm up, gonna slip for 5 seconds.')
    time.sleep(5)
    Check = requests.get(args.server+'/_cat/indices', auth=(args.auth[0], args.auth[1]), verify=False)
    brake = Check.text.split('\n')
    for i in brake:
        if indexname in i:
            stat = i.split(' ')
            while '' in stat:
                stat.remove('')
            print("Name: "+stat[2]+"\nStatus: "+stat[0]+"\nShard_name: "+stat[3]+"\nRecords: "+stat[6]+"\nSize: "+stat[8])

    dumpy = requests.get(args.server+'/'+indexname+'/_search', auth=(args.auth[0], args.auth[1]), verify=False, json={"size":stat[6], "query":{"match_all":{}}})
    print("\nHere's all the data currently in the index\n\n")
    print(dumpy.text)
except:
    sys.exit()

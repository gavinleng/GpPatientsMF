__author__ = 'G'

import sys
import urllib
import pandas as pd
import re
import argparse
import json
import datetime
import hashlib

# url = "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014-lsoa-male.csv"
# output_path = "tempGpPatientsMF.csv"
# required_indicators = ["2014", "01", "Male"]


def download(url, reqFields, outPath):
    reqReq = reqFields
    dName = outPath

    iYear = reqReq[0]
    iMonth = reqReq[1]
    iSex = reqReq[2]

    col = ['PRACTICE_CODE', 'ORG_NAME', 'Year', 'Month', 'Sex', 'LSOA_CODE', 'Value', 'pkey']

    # open url
    socket = openurl(url)

    raw_data = {}
    for j in col:
        raw_data[j] = []

    # operate this csv file
    logfile.write(str(now()) + ' csv file loading\n')
    print('csv file loading------')
    df = pd.read_csv(socket, dtype='unicode')

    for k in range(0, df.shape[1]):
        if re.match(r'E\d{8}$', str(df.iloc[0][k])):
            break

    if k == df.shape[1]:
        errfile.write(str(now()) + " Cannot find ecode in row " + str(2) + ". Please check the file at: " + str(url) + " . End progress\n")
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit("Cannot find ecode in row " + str(2) + ". Please check the file at: " + url)

    logfile.write(str(now()) + ' data reading\n')
    print('data reading------')
    for i in range(0, df.shape[0], 2):
        if str(df.iloc[i][0]):
            eList = df.iloc[i, k:].dropna().tolist()
            raw_data[col[5]] = raw_data[col[5]] + eList
            raw_data[col[6]] = raw_data[col[6]] + df.iloc[i+1, k:].dropna().tolist()
            raw_data[col[0]] = raw_data[col[0]] + [df.iloc[i][0]] * len(eList)
            raw_data[col[1]] = raw_data[col[1]] + [df.iloc[i][1]] * len(eList)

    raw_data[col[2]] = [iYear] * len(raw_data[col[0]])
    raw_data[col[3]] = [iMonth] * len(raw_data[col[0]])
    raw_data[col[4]] = [iSex] * len(raw_data[col[0]])
    logfile.write(str(now()) + ' data reading end\n')
    print('data reading end------')

    # create primary key by md5 for each row
    logfile.write(str(now()) + ' create primary key\n')
    print('create primary key------')
    keyCol = [0, 2, 3, 4, 5]
    raw_data[col[-1]] = fpkey(raw_data, col, keyCol)
    logfile.write(str(now()) + ' create primary key end\n')
    print('create primary key end------')

    # save csv file
    logfile.write(str(now()) + ' writing to file\n')
    print('writing to file ' + dName)
    dfw = pd.DataFrame(raw_data, columns=col)
    dfw.to_csv(dName, index=False)
    logfile.write(str(now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now()) + ' finished\n')
    print("finished")

def openurl(url):
    try:
        socket = urllib.request.urlopen(url)
        return socket
    except urllib.error.HTTPError as e:
        errfile.write(str(now()) + ' file download HTTPError is ' + str(e.code) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        errfile.write(str(now()) + ' file download URLError is ' + str(e.args) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download URLError = ' + str(e.args))
    except Exception:
        print('file download error')
        import traceback
        errfile.write(str(now()) + ' generic exception: ' + str(traceback.format_exc()) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('generic exception: ' + traceback.format_exc())

def fpkey(data, col, keyCol):
    mystring = ''
    pkey = []
    for i in range(len(data[col[0]])):
        print('pkey------' + str(i))
        for j in keyCol:
            mystring += str(data[col[j]][i])
        mymd5 = hashlib.md5(mystring.encode()).hexdigest()
        pkey.append(mymd5)

    return pkey

def now():
    return datetime.datetime.now()


parser = argparse.ArgumentParser(
    description='Extract online GP Patients by GP and LSOA Male & Female csv file to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_tempGpPatientsMF.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "url": "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014-lsoa-male.csv",
        "outPath": "tempGpPatientsMF.csv",
        "reqFields": ["2014", "01", "Male"]
    }

    #obj = {
        #"url": "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014-lsoa-male.csv",
        #"outPath": "tempGpPatientsM.csv",
        #"reqFields": ["2014", "01", "Male"]
    #}

    #obj = {
        #"url": "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014-lsoa-fem.csv",
        #"outPath": "tempGpPatientsF.csv",
        #"reqFields": ["2014", "01", "Female"]
    #}

    logfile = open("log_tempGpPatientsMF.log", "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open("err_tempGpPatientsMF.err", "w")

    with open("config_tempGpPatientsMF.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempGpPatientsF.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["reqFields"], oConfig["outPath"])

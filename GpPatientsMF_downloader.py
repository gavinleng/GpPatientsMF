__author__ = 'G'

import sys
sys.path.append('../harvesterlib')

import pandas as pd
import re
import argparse
import json

import now
import openurl
import datasave as dsave


# url = "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014-lsoa-male.csv"
# output_path = "tempGpPatientsMF.csv"
# required_indicators = ["2014", "01", "Male"]


def download(url, reqInfo, outPath, col, keyCol, digitCheckCol, noDigitRemoveFields):
    reqReq = reqInfo
    dName = outPath

    iYear = reqReq[0]
    iMonth = reqReq[1]
    iSex = reqReq[2]

    # open url
    socket = openurl.openurl(url, logfile, errfile)

    raw_data = {}
    for j in col:
        raw_data[j] = []

    # operate this csv file
    logfile.write(str(now.now()) + ' csv file loading\n')
    print('csv file loading------')
    df = pd.read_csv(socket, dtype='unicode')

    for k in range(0, df.shape[1]):
        if re.match(r'E\d{8}$', str(df.iloc[0][k])):
            break

    if k == df.shape[1]:
        errfile.write(str(now.now()) + " Cannot find ecode in row " + str(2) + ". Please check the file at: " + str(url) + " . End progress\n")
        logfile.write(str(now.now()) + ' error and end progress\n')
        sys.exit("Cannot find ecode in row " + str(2) + ". Please check the file at: " + url)

    logfile.write(str(now.now()) + ' data reading\n')
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
    logfile.write(str(now.now()) + ' data reading end\n')
    print('data reading end------')

    # save csv file
    dsave.save(raw_data, col, keyCol, digitCheckCol, noDigitRemoveFields, dName, logfile)


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
        "reqInfo": ["2014", "01", "Male"],
        #"url": "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014-lsoa-male.csv",
        #"outPath": "tempGpPatientsM.csv",
        #"reqInfo": ["2014", "01", "Male"],
        #"url": "http://www.hscic.gov.uk/catalogue/PUB13365/gp-reg-patients-01-2014-lsoa-fem.csv",
        #"outPath": "tempGpPatientsF.csv",
        #"reqInfo": ["2014", "01", "Female"],
        "colFields": ['PRACTICE_CODE', 'ORG_NAME', 'Year', 'Month', 'Sex', 'LSOA_CODE', 'Value'],
        "primaryKeyCol": ['PRACTICE_CODE', 'Year', 'Month', 'Sex', 'LSOA_CODE'],#[0, 2, 3, 4, 5],
        "digitCheckCol": ['Value'],#[6],
        "noDigitRemoveFields": ['Value'],#[6]
    }

    logfile = open("log_tempGpPatientsMF.log", "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open("err_tempGpPatientsMF.err", "w")

    with open("config_tempGpPatientsMF.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now.now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempGpPatientsMF.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now.now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["reqInfo"], oConfig["outPath"], oConfig["colFields"], oConfig["primaryKeyCol"], oConfig["digitCheckCol"], oConfig["noDigitRemoveFields"])

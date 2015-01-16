from lxml import etree
import requests
from pywqp import pywqp_client
import os
import gzip
from datetime import datetime

'''
Creates a log file to track the success or failure of each state and matches the
integrity of the download (number of records from headers). There are lots of little
logging blurbs throughout the script. Feel free to comment them out.
'''
time = str(datetime.now().time()).replace(':', '-').replace('.','-')
log_name = 'wqp_request_log_'+time+'.txt'
log_file = open(log_name, 'w')


def get_county_codes(state_code):
    '''
    Makes a request for the codes of all the counties in a state for WQP
    query purposes. Makes a dictionary of all the codes and names. Works
    for US country codes.
    :param state_code: WQP Code of the state we want all the county codes for
    :return: dictionary of all county codes (keys) and county names (values)
    '''
    base_url = 'http://www.waterqualitydata.us/Codes/countycode?'
    params = 'countrycode=US&statecode=' + str(state_code) + '&mimetype=xml'
    r = requests.get(base_url+params)
    root = etree.fromstring(r.content)
    codes = root.getchildren()
    code_dict = {}
    for code in codes:
        code_dict.update({code.attrib['value']:code.attrib['desc']})
    return code_dict

def get_codes():
    '''
    Goes to WQP and gets an XML of the state codes where our country
    code is US. Returns a dictionary of all the state codes and
    their human readable equivalent. It might be worth mentioning that
    the first state code (US:00) is called UNASSIGNED.
    '''
    base_url = 'http://www.waterqualitydata.us/Codes/statecode?countrycode=US&mimeType=xml'
    #Use requests to get a request from the url where our xml can be found
    r = requests.get(base_url)
    #Take the data dumped from that request and turn it into a xml readable by lxml
    root = etree.fromstring(r.content)
    #Use lxml to get a list of all the children in our xml
    codes = root.getchildren()
    code_dict = {}
    #Fill a dictionary object with the value (State Code) and
    #desc (State Name) as keys and values respectively.
    for code in codes:
        code_dict.update({code.attrib['value']:code.attrib['desc']})
    sort_name = sorted(code_dict.items())
    return code_dict

def get_wqp_data(statecode, state, countycode='0'):
    '''
    Given the state code and the state name this function asks pyWQP ever so nicely
    do download and stash (in the current working directory) a CSV of all the station data
    from that state.
    '''
    #Set up pywqp client
    wqp_client = pywqp_client.RESTClient()

    #Fill pywqp variables
    verb = 'get'

    #host_url = 'http://waterqualitydata.us'
    host_url = 'http://cida-eros-wqpprod.er.usgs.gov:8080/wqp-aggregator'

    resource_label = 'result'
    params = {'countrycode': 'US','statecode': statecode}
    stash_location = os.path.join(os.getcwd(), state+'.csv') #Defines the target location in the current working directory

    if countycode != '0':
        params = {'countrycode': 'US', 'statecode': statecode, 'countycode': countycode}
        county = countycode.replace(':', "-").replace('/', '')
        stash_location = os.path.join(os.getcwd(), state+'_'+county+'.csv')

    #Make the response based on all the wqp variables
    response = wqp_client.request_wqp_data(verb, host_url, resource_label, params, mime_type='text/csv')
    expected_records = response.headers['total-result-count']

    #Write to disk
    wqp_client.stash_response(response, stash_location)

    
    return stash_location, expected_records


'''
This is meant to be the main loop. The try except is to avoid WQP server timeouts or
crashes. Again, feel free to comment out the lines for logging.
'''
#Get a dictionary of all the state codes.
state_dict = get_codes()

for key in state_dict:

    state_name = state_dict[key]
    county_dict = get_county_codes(key)

    for county_key in county_dict:

        try:
            #Stash the csv and get the name.
            log_file.write(str(state_name))
            log_file.flush()

            file_name, expected_records = get_wqp_data(key, state_dict[key], county_key)

            #Write the csv data to a gzip file and delete the uncompressed version.
            log_file.write('    records in headers:' + str(expected_records))
            log_file.flush()

            #The csv files can be really big so this block will gunzip 'em up and remove the csv. Uncomment if
            #that sounds like something you want to do.
            #with open(file_name) as file_in:
            #    with gzip.open(os.path.basename(file_name)+'.gz','wb') as file_out:
            #        file_out.writelines(file_in)

            csv_rows = sum(1 for row in file_name)
            log_file.write('    records in csv:'+str(csv_rows))
            log_file.flush()

            #os.remove(file_name) #This removes the csv file. Uncomment if you plan on using the gzipping block

            log_file.write('    SUCCESS\n')
            log_file.flush()
        #Sometimes the request says "no" for whatever reason. If it fails just go back and try again.
        except:
            print 'FAILED'
            log_file.write('    FAILED\n')
            log_file.flush()

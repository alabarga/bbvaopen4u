#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, requests
from os import environ

"""
   bbva2cartob.py:

   Make an API call to BBVA API and put the result into a CartoDB table.
"""

bbva_url_stats = "https://apis.bbvabancomer.com/datathon/tiles/%s/%s/cards_cube"
cartodb_url_sql = "http://%s.cartodb.com/api/v2/sql"



def get_env_variable(var_name):
    try:
        return environ[var_name]
    except KeyError:
        msg = "Set the %s environment variable"
        error_msg = msg % var_name
        raise Exception(error_msg)


def read_secret_params():
    cdb_user = get_env_variable('cdb_user')
    cdb_apikey = get_env_variable('cdb_apikey')
    bbva_auth_str = get_env_variable('bbva_auth_str')

    return (cdb_user, cdb_apikey, bbva_auth_str)


def read_payload_from_file(file_path):
    myvars = {}
    with open(file_path) as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            if var:
                myvars[name.strip()] = var.replace("\n", "")

    return myvars


def main(argv):

    #TODO: Parse input args

    #Read secret params
    (cdb_user, cdb_api_key, bbva_auth_str) = read_secret_params()

    #We will need this header
    headers = {'Authorization' : 'Basic %s' % bbva_auth_str}


    #We have all the arguments for the query stored in a text file
    bbva_payload = read_payload_from_file(argv[1])

    sql = "SELECT longitude, latitude from demoscartodb.bytile order by total desc offset 135"

    payload = {'q': sql, 'api_key': cdb_api_key}
    r = requests.get(cartodb_url_sql % cdb_user, params=payload)

    data = r.json()

    if 'rows' in data:
        for point in data['rows']:

            #Raise the BBVA API query
            if 'latitude' in point and 'longitude' in point:
                r2 = requests.get(bbva_url_stats % (point['latitude'], point['longitude']), params=bbva_payload, headers=headers)

                #print r2.url

                data2 = r2.json()

                if 'metadata' in data2 and 'hash_description' in data2['metadata'] and 'ranges' in data2['metadata']['hash_description'] and 'data' in data2 and 'stats' in data2['data']:


                    # First, parse metadata
                    genders = {}
                    ages = {}

                    for range in data2['metadata']['hash_description']['ranges']:

                        if 'name' in range and 'values' in range:

                            if range['name'] == 'gender':
                                for gender in range['values']:
                                    if 'label' in gender and 'description' in gender:
                                        genders[str(gender['label'])] = str(gender['description'])

                            elif range['name'] == 'age':
                                for age in range['values']:
                                    if 'label' in age and 'description' in age:
                                        ages[str(age['label'])] = str(age['description'])


                    #Now, parses data
                    for stat in data2['data']['stats']:


                        insert_dict = {}


                        if 'date' in stat:
                            insert_dict['the_date'] = "'" + str(stat['date']) + "'"

                        if 'cube' in stat:
                            cubes = stat['cube']

                            # Here, we've flatenning the structure, to fit into our data model
                            for cube in cubes:

                                if 'hash' in cube:
                                    l = str(cube['hash']).split('#')

                                    insert_dict['age'] = "'" + ages[l[1]] + "'"
                                    insert_dict['gender'] = "'" + genders[l[0]] + "'"

                                if 'num_cards' in cube:
                                    insert_dict['num_cards'] = str(cube['num_cards'])


                                if 'num_payments' in cube:
                                    insert_dict['num_payments'] = str(cube['num_payments'])


                                if 'avg' in cube:
                                    insert_dict['avg'] = str(cube['avg'])



                                insert_dict['the_geom'] = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)" % (str(point['longitude']),str(point['latitude']))


                                sql2 = "insert into stats_by_gender_and_age(%s) values(%s)" % (','.join(insert_dict.keys()),','.join(insert_dict.values()))

                                print sql2

                                #Insert entry in CartoDB
                                payload2 = {'q': sql2, 'api_key': cdb_api_key}
                                r3 = requests.get(cartodb_url_sql % cdb_user, params=payload2)



if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception, e:
        print e
        sys.exit(1)



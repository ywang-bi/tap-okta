import requests
import json
import singer
import os
import datetime
import time
from singer import metadata

schemas = {}
LOGGER = singer.get_logger()
session = requests.session()
REQUIRED_CONFIG_KEYS = ['api_key', 'service_url']

KEY_PROPERTIES = {
    'application_groups': ['id'],
    'application_users': ['id'],
    'applications': ['id'],
    'group_users': ['id'],
    'groups': ['id'],
    'users': ['id']
}


def header_payload(p_data):
    header = {
        'accept': 'application/json',
        'content-type': 'application/json',
        'Authorization': 'SSWS ' + p_data['api_key']
    }

    return header


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES[schema_name])
    for field_name in schema['properties'].keys():

        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []
    for schema_name, schema in raw_schemas.items():
        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)
        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': metadata.to_list(mdata),
            'key_properties': KEY_PROPERTIES[schema_name],
        }

        streams.append(catalog_entry)
    return {'streams': streams}


def do_discover():
    LOGGER.info('Loading schemas')
    catalog = get_catalog()
    print(json.dumps(catalog, indent=2))


def do_sync(p_data,state,p_catalog):
    for stream in p_catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        mdata = stream['metadata']
        singer.write_schema(stream_id, schemas[stream_id], 'id')
        if stream_id == 'users':
            url = p_data['service_url'] + "users"
            url_pagination(stream_id, url, p_data,state)
        elif stream_id == 'groups':
            url = p_data['service_url'] + "groups"
            url_pagination(stream_id, url, p_data,state)
        elif stream_id == 'applications':
            url = p_data['service_url'] + "apps"
            url_pagination(stream_id, url, p_data,state)



def url_pagination(p_schema, p_url, p_data,p_state):
    groups_for_user = []
    lst_urs_assign_app = []
    next_url = p_url
    header = header_payload(p_data)
    while (next_url):
        time.sleep(2)
        response = requests.request("GET", next_url, headers=header)
        if (response.status_code == 200):
            response_links = requests.utils.parse_header_links(
                response.headers['Link'].rstrip('>').replace('>,<', ',<')
            )
            next_url = ""
            for linkobj in response_links:
                if linkobj['rel'] == 'next':
                    next_url = linkobj['url']
        else:
            exit(1)
        data = response.json()
        for record in data:
            if p_schema == 'groups':
                groups_for_user.append(record['id'])
            if p_schema == 'applications':
                lst_urs_assign_app.append(record['id'])
            singer.write_record(p_schema, record)
            singer.write_state(p_state)

    if p_schema == 'groups':
        get_groups_for_user(groups_for_user, p_data,p_state)

    if p_schema == 'applications':
        list_assigned_groups_app(lst_urs_assign_app, p_data,p_state)
        list_users_assigned_to_app(lst_urs_assign_app, p_data,p_state)


def get_groups_for_user(p_groups_for_user, p_data,p_state):
    for grp_list in p_groups_for_user:
        p_get_group_user_url = p_data['service_url'] + "groups/" + grp_list + "/users"
        url_pagination('group_users', p_get_group_user_url, p_data,p_state)


def list_users_assigned_to_app(p_lst_urs_assign_app, p_data,p_state):
    for usr_app_list in p_lst_urs_assign_app:
        p_url = p_data['service_url'] + "apps/" + usr_app_list + "/users"
        url_pagination('application_users', p_url, p_data,p_state)


def list_assigned_groups_app(p_lst_urs_assign_app, p_data,p_state):
    for usr_grp_list in p_lst_urs_assign_app:
        appid_groups_url = p_data['service_url'] + "apps/" + usr_grp_list + "/groups"
        url_pagination('application_groups', appid_groups_url, p_data,p_state)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    global schemas
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)
    return schemas


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.config:
        if args.discover:
            do_discover()
        else:
            if args.properties:
                catalog = args.properties
                load_schemas()
            else:
                catalog = get_catalog()
            do_sync(args.config, args.state, catalog)

    LOGGER.info('End Date Time : %s', datetime.datetime.now())


if __name__ == '__main__':
    LOGGER.info('Start Date Time : %s', datetime.datetime.now())
    main()

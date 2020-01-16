import requests
from requests.adapters import HTTPAdapter
import re
from datajoint.errors import DataJointError
# from datajoint.settings import config

HUB_PROTOCOL = 'hub://'
REQUEST_PROTOCOL = 'https://'
API_ROUTE = '/api'
# DATAJOINT_HUB_API_HOST = 'https://fakeminio.datajoint.io'
# DATAJOINT_HUB_API_HOST = 'http://fakeminio.datajoint.io:4000'
# DATAJOINT_HUB_API_HOST = 'https://fakeminio.datajoint.io:4000'
API_TARGETS = dict(GET_DB_FQDN='/v0.000/get_database_fqdn')

# djhub_adapter = HTTPAdapter(max_retries=3)
session = requests.Session()
session.mount('http://', HTTPAdapter(max_retries=3))

# session.mount('{}{}'.format(REQUEST_PROTOCOL, hub_path[0][1:]), djhub_adapter)


def get_host(host):
    # print(DATAJOINT_HUB_API_HOST)
    # host = '1.2.3.4'
    # host = '1.2.3.4:5678'
    # host = 'ever_gre-en'
    # host = 'ever_gre-en:1234'
    # host = 'djhub://raphael/datajoint'
    # host = 'djhub3://raphael/datajoint'
    hub_path = re.findall('/[:._\-a-zA-Z0-9]+', host)
    if re.match(HUB_PROTOCOL, host) and len(hub_path) > 2:
        # print(config['loglevel'])
        # session.mount('{}{}'.format(REQUEST_PROTOCOL, hub_path[0][1:]), djhub_adapter)
        try:
            resp = session.post('{}{}{}{}'.format(REQUEST_PROTOCOL, hub_path[0][1:], API_ROUTE, API_TARGETS['GET_DB_FQDN']),
                data={'org': hub_path[1][1:], 'project': hub_path[2][1:]}, timeout=10)
            if resp.status_code == 200:
                return resp.json()['database.host']
            elif resp.status_code == 404:
                raise DataJointError('DataJoint Hub database resource `{}/{}/{}` not found.'.format(hub_path[0][1:], hub_path[1][1:], hub_path[2][1:]))
            elif resp.status_code == 501:
                raise DataJointError('DataJoint Hub endpoint `{}{}{}{}` unavailable.'.format(REQUEST_PROTOCOL, hub_path[0][1:], API_ROUTE, API_TARGETS['GET_DB_FQDN']))
        except requests.exceptions.SSLError:
            raise DataJointError('TLS security violation on DataJoint Hub target `{}{}{}`.'.format(REQUEST_PROTOCOL, hub_path[0][1:], API_ROUTE))
        except requests.exceptions.ConnectionError:
            raise DataJointError('Unable to reach DataJoint Hub target `{}{}{}`.'.format(REQUEST_PROTOCOL, hub_path[0][1:], API_ROUTE))
    elif not re.match('.*/', host):
        return host
    else:
        raise DataJointError('Malformed database host `{}`.'.format(host))
        
# Makes sense. In that case maybe lets go with minimal redirect against unauthenticated API
#  and defer this conversation until the next round. In that case, as edgar had mentioned (and 
# I’m just remembering), will just use a url scheme in dj.config database.host, and that
#  triggers the db data lookup.
# In that case, maybe lets go with djhub://org/project and expect {database.host: a.b.c.d} 
# as response. As for method name, let me think that through but maybe 
# /v0.000/get_database_fqdn with POST of `{‘org’: X, ‘project’: Y}` for now. We have some 
# example python rest client code handy in
#  https://github.com/vathes/ibl-navigator/blob/master/backend/apiclient.py if that’s useful.




# points

# -store hub endpoint
# -connect through translated host
# -reattempt count on hub before defaulting
# - if loose connection on db, reattempt on db before reattempt on hub
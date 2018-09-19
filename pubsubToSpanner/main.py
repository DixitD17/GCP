# Imports the Google Cloud Client Library.
from google.cloud import spanner
import sys
import json


# [START function_load_spanner_single_three]
def function_load_spanner_single_three(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc3node'
    function_load_spanner_generic(data, instancename)

    print('function completed')


# [END function_load_spanner_single_three]

# [START function_load_spanner_single_six]
def function_load_spanner_single_six(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc6node'
    function_load_spanner_generic(data, instancename)

    print('function completed')


# [END function_load_spanner_single_six]

# [START function_load_spanner_multi_three]
def function_load_spanner_multi_three(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc3mnode'
    function_load_spanner_generic(data, instancename)

    print('function completed')


# [END function_load_spanner_multi_three]

# [START function_load_spanner_multi_six]
def function_load_spanner_multi_six(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc6mnode'
    function_load_spanner_generic(data, instancename)

    print('function completed')


# [END function_load_spanner_multi_six]

# [START function_load_spanner_generic]
def function_load_spanner_generic(data, instancename):
    """Background Cloud Function to be triggered by Pub/Sub listener.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         instancename : Spanner instance name.
    """
    import base64

    if 'data' in data:
        msg = base64.b64decode(data['data']).decode('utf-8')
        parseddata = json.loads(msg)
        productid: int = parseddata["product_id"]
        upc: int = parseddata["sku_upc_nbr"]
        div = parseddata["div_nbr"]
        avail = parseddata["availability"]["fiol"]
        ordermethod = parseddata["fulfillment"]["order_method"]
        pricelevel = parseddata["fulfillment"]["price_level"]
        pricelevelid = parseddata["fulfillment"]["price_level_id"]
        returnscode = parseddata["fulfillment"]["returns_code"]
        maxqty = parseddata["fulfillment"]["max_quantity"]
        reasoncode = parseddata["fulfillment"]["reason_code"]
        lowavailability = parseddata["fulfillment"]["low_availability"]
        shiprep = parseddata["fulfillment"]["ship_rep"]["days"]
        lastmodified = parseddata["last_push_ts"]

        spanner_client = spanner.Client()
        instance = spanner_client.instance(instancename)
        database = instance.database('inventory')
        count: int = 4

        try:
            while count < 800:
                with database.batch() as batch:
                    batch.insert(
                        table='availability_by_productid_upc',
                        columns=(
                            'productid', 'upc', 'division', 'fiaavail', 'ordermethod', 'pricelevel', 'pricelevelid',
                            'returnscode',
                            'maxqty', 'reasoncode', 'lowavailability', 'shiprep',),
                        values=[
                            (int(productid) + int(count), int(upc) + int(count), div, avail, ordermethod, pricelevel,
                             pricelevelid, returnscode, maxqty, reasoncode,
                             lowavailability, shiprep)])
                count += 1
        except Exception as e:
            print(f' Exception occurred : {e} ')

        print(f' Table has been update for UPC : {upc} and message {msg}')

    else:
        print('No data to add')

    print('spanner generic load function completed')


# [END function_load_spanner_generic]


# [START function_read_spanner_generic]
def function_read_spanner_generic(data, instancename):
    """Background Cloud Function to be triggered by Pub/Sub listener..
    Args:
         data (dict): The dictionary with data specific to this type of event.
         instancename : Spanner instance name.
    """
    import base64

    if 'data' in data:
        msg = base64.b64decode(data['data']).decode('utf-8')
        parseddata = json.loads(msg)
        productid: int = parseddata["product_id"]
        upc: int = parseddata["sku_upc_nbr"]
        pricelevel = parseddata["fulfillment"]["price_level"]

        spanner_client = spanner.Client()
        instance = spanner_client.instance(instancename)
        database = instance.database('inventory')
        count: int = 0

        try:
            while count < 1000:
                with database.snapshot() as snapshot:
                    keyset = spanner.KeySet(keys=[(int(productid) + int(count), int(upc) + int(count))])
                    results = snapshot.read(
                        table='availability_by_productid_upc',
                        columns=('productid', 'upc', 'fiaavail',),
                        keyset=keyset, )

                    for row in results:
                        print(u'productid: {}, upc: {}, fiaavail: {}'.format(*row))
                count += 1
        except Exception as e:
            print(f' Exception occurred : {e} ')

    else:
        print('No data to read')

    print('spanner generic read function completed')


# [END function_read_spanner_generic]

# [START function_load_spanner_single_three]
def function_read_spanner_single_three(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc3node'
    function_read_spanner_generic(data, instancename)

    print('function completed')


# [END function_read_spanner_single_three]

# [START function_read_spanner_single_six]
def function_read_spanner_single_six(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc6node'
    function_read_spanner_generic(data, instancename)

    print('function completed')


# [END function_read_spanner_single_six]

# [START function_read_spanner_multi_three]
def function_read_spanner_multi_three(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc3mnode'
    function_read_spanner_generic(data, instancename)

    print('function completed')


# [END function_read_spanner_multi_three]

# [START function_read_spanner_multi_six]
def function_read_spanner_multi_six(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc6mnode'
    function_read_spanner_generic(data, instancename)

    print('function completed')

# [END function_read_spanner_multi_six]

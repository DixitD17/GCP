# Imports the Google Cloud Client Library.
from google.cloud import spanner
import sys
import json


# [START functions_pubsub_trigger_spanner]
def pubsub_trigger_spanner(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    import base64

    if 'data' in data:
        msg = base64.b64decode(data['data']).decode('utf-8')
        parseddata = json.loads(msg)
        productid = parseddata["product_id"]
        upc = parseddata["sku_upc_nbr"]
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
        instance = spanner_client.instance('hubpoc')
        database = instance.database('inventory')

        with database.batch() as batch:
            batch.insert(
                table='availability_by_productid_upc',
                columns=(
                    'productid', 'upc', 'division', 'fiaavail', 'ordermethod', 'pricelevel', 'pricelevelid',
                    'returnscode',
                    'maxqty', 'reasoncode', 'lowavailability', 'shiprep',),
                values=[
                    (productid, upc, div, avail, ordermethod, pricelevel, pricelevelid, returnscode, maxqty, reasoncode,
                     lowavailability, shiprep)])

        print(f' Table has been update for UPC : {upc} and message {msg}')

    else:
        print('No data to add')

    print('function completed')


# [END functions_pubsub_trigger_spanner]

# [START functionspubsub_readwrite_spanner]
def pubsub_readwrite_spanner(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    import base64

    if 'data' in data:
        msg = base64.b64decode(data['data']).decode('utf-8')
        parseddata = json.loads(msg)
        productid = parseddata["product_id"]
        upc = parseddata["sku_upc_nbr"]
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
        instance = spanner_client.instance('hubpoc2')
        database = instance.database('inventory')
        print('Getting into Update Avail function ')

        def update_avail(transaction):
            print('In Update Avail function 1 ')
            # Read the timestamp.
            existing_avail_keyset = spanner.KeySet(keys=[(productid, upc)])
            existing_avail_result = transaction.read(
                table='availability_by_productid_upc', columns=('lastmodified',),
                keyset=existing_avail_keyset, limit=1)
            print('In Update Avail function 2 ')

            if existing_avail_result is None:
                print('Start inserting the data in the table ')
                with database.batch() as batch:
                    batch.insert(
                        table='availability_by_productid_upc',
                        columns=(
                            'productid', 'upc', 'division', 'fiaavail', 'ordermethod', 'pricelevel', 'pricelevelid',
                            'returnscode',
                            'maxqty', 'reasoncode', 'lowavailability', 'shiprep','lastmodified',),
                        values=[
                            (productid, upc, div, avail, ordermethod, pricelevel, pricelevelid, returnscode, maxqty,
                             reasoncode,
                             lowavailability, shiprep,lastmodified)])

                print(f' Table has been update for UPC : {upc} and message {msg}')

            else:
                print('Row Exists, Compare the timestamp ')
                existing_avail_row = list(existing_avail_result)[0]
                existing_avail_timestamp = existing_avail_row[0]
                print(f' new timestamp : {lastmodified} and previous timestamp {existing_avail_timestamp}')

                if lastmodified < existing_avail_timestamp:
                    # Raising an exception will automatically roll back the
                    # transaction.
                    raise ValueError(
                        'The data is not latest . Latest record is already present')

                # Update the rows.
                transaction.update(
                    table='availability_by_productid_upc',
                    columns=(
                        'productid', 'upc', 'division', 'fiaavail', 'ordermethod', 'pricelevel', 'pricelevelid',
                        'returnscode',
                        'maxqty', 'reasoncode', 'lowavailability', 'shiprep', 'lastmodified'),
                    values=[
                        (productid, upc, div, avail, ordermethod, pricelevel, pricelevelid, returnscode, maxqty, reasoncode,
                         lowavailability, shiprep, lastmodified)])

                database.run_in_transaction(update_avail)

            print('Transaction complete.')

    else:
        print('No data to add')

    print('pubsub_readwrite_spanner function completed')

# [END functions_pubsub_readwrite_spanner]

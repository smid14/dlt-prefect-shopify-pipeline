import dlt
from dlt.common import pendulum
from dlt.common.typing import TAnyDateTime, TDataItem
from typing import Iterable
from .adapters.shopify_client import ShopifyAPI
from .adapters.redis_client import RedisClient, Run


# Define a source named 'shopify' with maximum table nesting level of 0
@dlt.source(name='shopify', max_table_nesting=0)
def shopify_source(start_date: TAnyDateTime = None, end_date: TAnyDateTime = None):
    """
    A source function that retrieves data from Shopify API and saves it to Redis.

    Args:
        start_date (TAnyDateTime, optional): The start date for retrieving data. Defaults to None.
        end_date (TAnyDateTime, optional): The end date for retrieving data. Defaults to None.

    Returns:
        tuple: A tuple of resource functions for retrieving customers, discounts, gift cards, and orders.
    """

    shopify_api = ShopifyAPI()

    # Resource function for retrieving customers
    @dlt.resource(primary_key=('id', 'updated_at'), write_disposition="merge")
    def customers(updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental("updated_at", initial_value=start_date, end_value=end_date)) -> Iterable[TDataItem]:
        redis_client = RedisClient()
        try:
            fields = ['id,', 'created_at', 'updated_at', 'accepts_marketing_updated_at', 'first_name', 'last_name','addresses', 'state', 'email', 'accepts_marketing','email_marketing_consent', 'last_order_id', 'last_order_name', 'marketing_opt_in_level', 'note','tags', 'verified_email','total_spent','orders_count']
            params = create_params(redis_client = redis_client, redis_key = "dlt_shopify_customers", start_date= start_date, fields = fields)    
            yield shopify_api.get_customers(params=params)
            dlt_customers_state = dlt.current.resource_state()
            save_state(redis_client, dlt_customers_state, "dlt_shopify_customers")
        except:
            print('Error in customers')
        finally:
            redis_client.close()

    # Resource function for retrieving orders
    @dlt.resource(primary_key=('id', 'updated_at'), write_disposition="merge")
    def orders(updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental("updated_at", initial_value=start_date, end_value=end_date)) -> Iterable[TDataItem]:
        redis_client = RedisClient()
        try:         
            fields = ["app_id","buyer_accepts_marketing","cancel_reason","cancelled_at","cart_token","checkout_token","closed_at","confirmation_number","created_at","currency","current_total_discounts","current_total_discounts_set","current_total_duties_set","current_total_price","current_total_price_set","current_subtotal_price","current_subtotal_price_set","current_total_tax","current_total_tax_set","customer_locale","discount_applications","discount_codes","email","estimated_taxes","financial_status","fulfillment_status","gateway","id","landing_site","line_items","name","note","note_attributes","number","order_number","original_total_additional_fees_set","original_total_duties_set","payment_gateway_names","presentment_currency","processed_at","referring_site","refunds","source_name","source_identifier","source_url","subtotal_price","subtotal_price_set","tags","tax_lines","taxes_included","test","token","total_discounts","total_discounts_set","total_line_items_price","total_line_items_price_set","total_outstanding","total_price","total_price_set","total_shipping_price_set","total_tax","total_tax_set","total_tip_received","total_weight","updated_at","user_id","order_status_url"]
            params = create_params(redis_client = redis_client, redis_key = "dlt_shopify_orders", start_date = start_date, fields = fields)
            yield shopify_api.get_orders(params=params)
            dlt_state = dlt.current.resource_state()
            save_state(redis_client, dlt_state, "dlt_shopify_orders")
        except:
            print('Error in orders')
        finally:
            redis_client.close()

    return (customers, orders)


def create_params(redis_client, redis_key, start_date, fields):
    field_str = create_field_string(fields)
    updated_at_min, updated_at_max = get_state(redis_client, redis_key, start_date) 
    params = dict(
        updated_at_min=updated_at_min,
        updated_at_max=updated_at_max,
        fields=field_str    
    )
    return params


def create_field_string(fields):
    if fields is not None:
        fields_str = ','.join(fields)
        return fields_str



def get_state(redis_client, key, start_date):
    """
    Retrieves the state from Redis for a given key and start date.

    Args:
        redis_client: The Redis client.
        key (str): The key to retrieve the state from Redis.
        start_date (TAnyDateTime): The start date for retrieving data.

    Returns:
        dict: The parameters for retrieving data from Shopify API.
    """
    dlt_customers_state = redis_client.load_data(key)
    current_value = pendulum.now().isoformat()

    if dlt_customers_state is not None:
        last_value = dlt_customers_state["last_value"]
        return last_value, current_value
    else:
        return start_date, current_value
        

def save_state(redis_client, dlt_state, key):
    """
    Saves the state to Redis for a given key.

    Args:
        redis_client: The Redis client.
        dlt_state: The state to be saved.
        key (str): The key to save the state to Redis.
    """
    run = Run(
        initial_value=dlt_state["incremental"]["updated_at"]["initial_value"],
        last_value=dlt_state["incremental"]["updated_at"]["last_value"],
        unique_hashes=dlt_state["incremental"]["updated_at"]["unique_hashes"]
    )

    redis_client.save_data(key, run.__dict__)
from prefect.blocks.system import Secret 
from prefect.blocks.system import String

def get_shopify_secrets():
    return {
        "API_KEY": Secret.load("<your-shopify-api-key>").get(), 
        "API_PASSWORD": Secret.load("<your-shopify-api-password>").get(),
        "ACCESS_TOKEN": Secret.load("<your-shopify-access-token>").get(),
    }

def get_shopify_shop_details():
    return {
        "SHOPIFY_SHOP_NAME": '<your-shop-name>', 
        "SHOPIFY_API_VERSION": "2023-07", 
        "SHOPIFY_PAGE_LIMIT": 5 
    }

def get_redis_secrets():
    return {
        "REDIS_HOST": String.load("<your-redis-host>"),
        "REDIS_PORT": String.load("<your-redis-port>"),
        "REDIS_PASSWORD": Secret.load("<your-redis-password>").get()
    }


from shopify.settings import get_shopify_secrets, get_shopify_shop_details
import requests
from dlt.common.typing import  TDataItems, Dict
from typing import Any, Iterable, Optional
from ratelimiter import RateLimiter


class ShopifyAPI:    

    """
    A class representing the Shopify API.

    Attributes:
        api_key (str): The API key for authentication.
        api_password (str): The API password for authentication.
        base_url (str): The base URL of the Shopify API.
        page_limit (int): The maximum number of items per page.
        access_token (str): The access token for authentication.
    """

    def __init__(self) -> None:

        secrets = get_shopify_secrets()
        self.api_key = secrets['API_KEY']
        self.api_password = secrets['API_PASSWORD']
        self.base_url = self.build_base_url()
        self.page_limit = get_shopify_shop_details()['SHOPIFY_PAGE_LIMIT']
        self.access_token = secrets['ACCESS_TOKEN'] 

    def build_endpoint(self, endpoint) -> str: 
        """
        Builds the complete endpoint URL.

        Args:
            endpoint (str): The endpoint path.

        Returns:
            str: The complete endpoint URL.
        """
        return f"{self.base_url}/{endpoint}.json"
   
    @RateLimiter(max_calls=30, period=60)
    def make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """
        Makes a GET request to the specified URL.

        Args:
            url (str): The URL to make the request to.
            params (dict, optional): The query parameters for the request.

        Returns:
            requests.Response: The response object.
        """
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": f"{self.access_token}"
        }
        print(url)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response

    def get_all_pages(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Iterable[TDataItems]:
        """
        Retrieves all pages of data for a given endpoint.

        Args:
            endpoint (str): The endpoint path.
            params (dict, optional): The query parameters for the request.

        Yields:
            dict: The JSON response for each page.
        """
      #  params = {'limit': self.page_limit}
        url = self.build_endpoint(endpoint)
        while True: 
            #print("LINK: ", url)
            response = self.make_request(url, params)
            #  pprint.pprint(response.json())
            yield response.json()[endpoint]
            url = response.links.get("next", {}).get("url")
            #print("URL: ", url)
            if url is None:
                break
            params = None


    def get_customers(self, params: Optional[Dict[str, Any]] = None) -> Iterable[TDataItems]:
        """
        Retrieves all customers.

        Returns:
            generator: A generator that yields the JSON response for each page.
        """
        endpoint = "customers"
        data = self.get_all_pages(endpoint, params)   
        return data 

    def get_orders(self, params: Optional[Dict[str, Any]] = None) -> Iterable[TDataItems]:
        """
        Retrieves all orders.

        Returns:
            requests.Response: The response object.
        """
        endpoint = "orders"
        return self.get_all_pages(endpoint, params)

    def build_base_url(self): 
        """
        Builds the base URL for the Shopify API.

        Returns:
            str: The base URL.
        """
        shop_details = get_shopify_shop_details()
        return f"https://{self.api_key}:{self.api_password}@{shop_details['SHOPIFY_SHOP_NAME']}.myshopify.com/admin/api/{shop_details['SHOPIFY_API_VERSION']}"
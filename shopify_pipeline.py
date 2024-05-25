import dlt
from shopify import shopify_source
from prefect import task, flow, get_run_logger



@task(name="incremental_load_customers")
def incremental_load_customers(logger = None, resource: str = 'customers' , start_date: str = None) -> dict:
    dlt.secrets["destination.filesystem.bucket_url"] = f"<your-bucket-url>"

    pipeline = dlt.pipeline(
         pipeline_name='shopify', destination='filesystem', dataset_name='shopify'
        )
    
    load_info = pipeline.run(shopify_source(start_date).with_resources(resource), loader_file_format="parquet")
    logger.info(f"Incremental Load Customers Information: {load_info}")


@task(name="incremental_load_orders")
def incremental_load_orders(logger = None, resource: str = 'orders', start_date: str = None) -> dict: 
    dlt.secrets["destination.filesystem.bucket_url"] = f"<your-bucket-url>"

    pipeline = dlt.pipeline(
        pipeline_name='shopify', destination='filesystem', dataset_name='shopify' 
    )
    load_info = pipeline.run(shopify_source(start_date).with_resources(resource), loader_file_format="parquet")
    logger.info(f"Incremental Load Orders Information: {load_info}")


@flow(name="shopify_pipeline")
def shopify_pipeline(): 
    logger = get_run_logger()
    start_date = '2023-01-01'
    logger.info(f"Starting Shopify Pipeline with Inital Start Date: {start_date}")

    logger.info("Starting Task: incremental_load_customers_filesystem")
    incremental_load_customers(logger = logger, resource='customers', start_date=start_date)
    
    
    logger.info("Starting Task: incremental_load_orders_filesystem")    
    incremental_load_orders(logger = logger, resource='orders', start_date=start_date)


if __name__ == "__main__":    
    shopify_pipeline.deploy(
        name="dlt-shopify-pipeline", 
        work_pool_name="<your-work-pool-name>", 
        image="<your-image-name>",
    )
    
    

import os

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


def get_client():
    return RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1",
        paginator=PageNumberPaginator(page_param="page", base_page=1, total_path=None),
    )

# Optimized source (parallelized + env tuning)
@dlt.source
def jaffle_optimized_source():
    client = get_client()

    @dlt.resource(name="customers", write_disposition="replace")
    def customers():
        for page in client.paginate("customers"):
          yield page

    @dlt.resource(name="products", write_disposition="replace")
    def products():
        for page in client.paginate("products"):
          yield page

    @dlt.resource(name="orders", write_disposition="append", parallelized=True)
    def orders(updated_at=dlt.sources.incremental("ordered_at", initial_value="1970-01-01T00:00:00Z")):
        for page in client.paginate("orders"):
          yield page

    return customers, products, orders


pipeline = dlt.pipeline(
    pipeline_name="jaffle_pipeline",
    destination="duckdb",
    dataset_name="jaffle_shop",
    progress="log"
)

pipeline.run(jaffle_optimized_source())

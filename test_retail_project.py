import pytest
from lib.DataReader import read_customers
from lib.DataReader import read_orders
from lib.DataManipulation import filter_closed_orders
from lib.ConfigReader import get_app_config
from lib.DataManipulation import filter_orders_generic

# def test_read_orders_df(spark):
#     orders_df = read_orders(spark, "LOCAL")
#     orders_count = orders_df.count()
#     assert orders_count == 68884

# def test_read_customers_df(spark):
#     customers_count = read_customers(spark, "LOCAL").count()
#     assert customers_count == 12435 

# def test_filter_closed_orders(spark):
#     orders_df = read_orders(spark, "LOCAL")
#     closed_orders_count = filter_closed_orders(orders_df).count()
#     assert closed_orders_count == 7556  # Adjust this based on your test data

# def test_read_app_config():
#     config = get_app_config("LOCAL")
#     assert config["customers.file.path"] == "data/customers.csv"
#     assert config["orders.file.path"] == "data/orders.csv"

# def test_check_closed_count(spark):
#     orders_df = read_orders(spark, "LOCAL")
#     closed_orders_count = filter_orders_generic(orders_df,"CLOSED").count()
#     assert closed_orders_count == 7556  # Adjust this based on your test

@pytest.mark.parametrize("status, count", [
    ("CLOSED", 7556),
    ("PENDING_PAYMENT", 15030),
    ("COMPLETE", 22900)  # Adjust based on your test data
])

@pytest.mark.latest
def test_check_count(spark, status, count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_orders_count = filter_orders_generic(orders_df, status).count()
    assert filtered_orders_count == count

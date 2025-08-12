import pytest
from lib.Utils import get_spark_session

@pytest.fixture(scope="module")
def spark():
    "create a Spark session for testing"
    spark_session =  get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()
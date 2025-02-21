import pytest
import re
from estuary_cdk.capture.common import CRON_REGEX

pattern = re.compile(CRON_REGEX)

@pytest.mark.parametrize("cron", [
    # Empty input
    "", 
    # Wildcards for all positions.
    "* * * * *", 
    # Steps
    "*/5 * * * *",
    # Minimum values
    "0 0 1 1 0", 
    # Maximum values
    "59 23 31 12 6",
    # Lists
    "0,30,1 0,12 1,4,5,23 3,4,6 1,4",
    # Ranges
    "0-59 0-23 1-31 1-12 0-6",
])
def test_valid_cron(cron):
    assert pattern.match(cron) is not None

@pytest.mark.parametrize("cron", [
    # Number of arguments
    "*", 
    "* *", 
    "* * * *", 
    "* * * * * *",
    # Invalid characters
    "abc123 * * * *", 
    # Negative numbers
    "-1 * * * *", 
    # Beyond min/max
    "60 * * * *",
    "* 24 * * *",
    "* * 0 * *", 
    "* * 32 * *",
    "* * * 0 *",
    "* * * 13 *",
    "* * * * 7",
    # Invalid list syntax
    ",0,1 * * * *",
    "0,1, * * * *",
    # Invalid range syntax
    "0-1- * * * *",
    "0- * * * *",
])
def test_invalid_cron(cron):
    assert pattern.match(cron) is None

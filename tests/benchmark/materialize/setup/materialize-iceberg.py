"""Fill Polaris's bootstrapped client credential into a materialize-iceberg
config in place of CREDENTIAL_PLACEHOLDER.

Usage: materialize-iceberg.py CREDS_JSON CONFIG
"""

import json
import sys


def main():
    creds_path, config_path = sys.argv[1:3]
    with open(creds_path) as f:
        creds = json.load(f)
    with open(config_path) as f:
        text = f.read()
    credential = f"{creds['client_id']}:{creds['client_secret']}"
    with open(config_path, "w") as f:
        f.write(text.replace("CREDENTIAL_PLACEHOLDER", credential))


if __name__ == "__main__":
    main()

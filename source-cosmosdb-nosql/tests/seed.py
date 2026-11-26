"""Seed the local emulator with containers keyed by customer and item ID."""
import argparse

from azure.cosmos import CosmosClient, PartitionKey


# Public key for the local Cosmos DB emulator.
KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="


def main(endpoint: str) -> None:
    with CosmosClient(endpoint, KEY, enable_endpoint_discovery=False) as client:
        database = client.create_database_if_not_exists("shop")
        orders = database.create_container_if_not_exists("orders", PartitionKey("/customer"))
        events = database.create_container_if_not_exists("events", PartitionKey("/id"))
        for index, customer in ((1, "acme"), (2, "acme"), (3, "globex")):
            orders.upsert_item({"id": f"o{index}", "customer": customer, "status": "new", "canary": f"o{index}"})
        for index in (1, 2):
            events.upsert_item({"id": f"e{index}", "kind": "click", "canary": f"e{index}"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://localhost:8091/")
    main(parser.parse_args().endpoint)

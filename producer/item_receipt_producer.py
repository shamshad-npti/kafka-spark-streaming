import random
import argparse
import time
from kafka import KafkaProducer
from producer.sales_receipt import ItemOrder, SalesReceipt

CURRENT_RECEIPT_ID = 100000

def _next_receipt_id():
    global CURRENT_RECEIPT_ID
    CURRENT_RECEIPT_ID += 1
    return CURRENT_RECEIPT_ID

def _generate_dummy_item_order(num_items=100, price_range=(1.0, 100.0), quantity_range=(1, 10)):
    price = random.uniform(*price_range)
    quantity = random.randint(*quantity_range)

    return ItemOrder(
        item_id=random.randint(1, num_items),
        total_price=price * quantity,
        quantity=quantity
    )


def _generate_dummy_receipt(
        num_stores=20,
        num_customers=100000,
        receipt_id=None,
        item_range=(1, 10),
        quantity_range=(1, 10),
        num_items=100,
        price_range=(1.0, 100.0)
    ):

    item_count = random.randint(item_range[0], item_range[1])
    items = [_generate_dummy_item_order(
        num_items=num_items,
        price_range=price_range,
        quantity_range=quantity_range
    ) for _ in xrange(item_count)]

    store_id = random.randint(1, num_stores)
    customer_id = random.randint(1, num_customers)
    receipt_id = receipt_id or _next_receipt_id()

    return SalesReceipt(
        customer_id=customer_id,
        store_id=store_id,
        receipt_id=receipt_id,
        items=items
    )

def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic",
        help="Kafka topic",
        default="sales_receipts"
    )

    parser.add_argument(
        "--bootstrap_servers",
        help="kafka bootstraps servers",
        default="localhost:9092"
    )

    parser.add_argument(
        "--delay",
        help="how frequently message should be produced (in millisecond)",
        type=float,
        default=100
    )

    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda message: str(message).encode("utf-8")
    )

    while True:
        _ = producer.send(args.topic, _generate_dummy_receipt())
        time.sleep(args.delay * 1e-3)

if __name__ == '__main__':
    _main()

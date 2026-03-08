import random
import time
import json
from faker import Faker
from fastavro import writer, parse_schema

fake = Faker()

# Load schema
with open("customer.avsc") as f:
    schema = parse_schema(json.load(f))

def random_product():
    return {
        "productId": f"P-{random.randint(1,200):03d}",
        "productName": fake.word().title(),
        "quantity": random.randint(1,5),
        "unitPrice": round(random.uniform(10, 999), 2)
    }

def random_order(customer_id, order_num):
    num_items = random.randint(1,7)
    items = [random_product() for _ in range(num_items)]
    total = sum(i["quantity"] * i["unitPrice"] for i in items)

    return {
        "orderId": f"ORD-{customer_id}-{order_num}",
        "orderTimestamp": int(time.time() * 1000) - random.randint(0, 60*60*24*365*2*1000),
        "orderDetails": items,
        "orderTotal": round(total, 2)
    }

def random_customer(i):
    num_orders = random.randint(1,5)
    cid = f"CUST-{i:04d}"

    return {
        "customerId": cid,
        "firstName": fake.first_name(),
        "lastName": fake.last_name(),
        "email": fake.email() if random.random() > 0.2 else None,
        "orders": [random_order(cid, j+1) for j in range(num_orders)]
    }

records = [random_customer(i) for i in range(1, 1001)]

# Write Avro binary file
with open("customers.avro", "wb") as out:
    writer(out, schema, records)

print("Generated customers.avro with 1000 records.")
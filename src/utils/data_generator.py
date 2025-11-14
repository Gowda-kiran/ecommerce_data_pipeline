"""
Generate sample e-commerce data for testing and demonstration.
This module creates realistic transaction, customer, and product data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import random
import os

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


class EcommerceDataGenerator:
    """Generate realistic e-commerce data."""

    def __init__(self, num_customers=10000, num_products=500, num_transactions=50000):
        self.num_customers = num_customers
        self.num_products = num_products
        self.num_transactions = num_transactions
        self.customers = None
        self.products = None
        self.transactions = None

    def generate_customers(self):
        """Generate customer data."""
        print(f"Generating {self.num_customers} customers...")

        customers = []
        for i in range(self.num_customers):
            customer = {
                'customer_id': f'CUST{str(i+1).zfill(6)}',
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': fake.street_address(),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'zip_code': fake.zipcode(),
                'country': 'USA',
                'registration_date': fake.date_between(start_date='-3y', end_date='today'),
                'customer_segment': random.choice(['Premium', 'Regular', 'Occasional']),
                'loyalty_points': random.randint(0, 10000)
            }
            customers.append(customer)

        self.customers = pd.DataFrame(customers)
        return self.customers

    def generate_products(self):
        """Generate product data."""
        print(f"Generating {self.num_products} products...")

        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books',
                     'Toys', 'Beauty', 'Food & Beverage']

        products = []
        for i in range(self.num_products):
            category = random.choice(categories)
            product = {
                'product_id': f'PROD{str(i+1).zfill(5)}',
                'product_name': fake.catch_phrase(),
                'category': category,
                'subcategory': f'{category}_Sub{random.randint(1, 5)}',
                'brand': fake.company(),
                'price': round(random.uniform(5.0, 500.0), 2),
                'cost': round(random.uniform(2.0, 300.0), 2),
                'stock_quantity': random.randint(0, 1000),
                'supplier_id': f'SUP{random.randint(1, 50):03d}',
                'weight_kg': round(random.uniform(0.1, 50.0), 2),
                'is_active': random.choice([True, True, True, False])  # 75% active
            }
            # Ensure cost is less than price
            product['cost'] = min(product['cost'], product['price'] * 0.7)
            products.append(product)

        self.products = pd.DataFrame(products)
        return self.products

    def generate_transactions(self):
        """Generate transaction data."""
        print(f"Generating {self.num_transactions} transactions...")

        if self.customers is None:
            self.generate_customers()
        if self.products is None:
            self.generate_products()

        transactions = []
        start_date = datetime.now() - timedelta(days=365)

        for i in range(self.num_transactions):
            num_items = random.randint(1, 5)
            transaction_date = start_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            customer_id = random.choice(self.customers['customer_id'].values)

            # Generate line items for this transaction
            selected_products = random.sample(list(self.products['product_id'].values),
                                            min(num_items, len(self.products)))

            for product_id in selected_products:
                product_price = self.products[
                    self.products['product_id'] == product_id
                ]['price'].values[0]

                quantity = random.randint(1, 3)
                unit_price = product_price
                discount = round(random.choice([0, 0, 0, 5, 10, 15, 20]) / 100 * unit_price, 2)

                transaction = {
                    'transaction_id': f'TXN{str(i+1).zfill(8)}_{len(transactions)}',
                    'order_id': f'ORD{str(i+1).zfill(8)}',
                    'customer_id': customer_id,
                    'product_id': product_id,
                    'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'discount_amount': discount,
                    'total_amount': round((unit_price * quantity) - (discount * quantity), 2),
                    'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'wallet']),
                    'shipping_cost': round(random.uniform(0, 20), 2),
                    'tax_amount': round(random.uniform(0, 10), 2),
                    'order_status': random.choice(['pending', 'confirmed', 'shipped',
                                                  'delivered', 'delivered', 'delivered',
                                                  'cancelled', 'refunded']),
                    'shipping_address': fake.address().replace('\n', ', '),
                    'device_type': random.choice(['mobile', 'desktop', 'tablet']),
                    'session_id': f'SESSION{random.randint(1, 100000):08d}'
                }

                # Add some data quality issues intentionally
                if random.random() < 0.01:  # 1% missing values
                    transaction['payment_method'] = None
                if random.random() < 0.005:  # 0.5% invalid amounts
                    transaction['total_amount'] = -1 * abs(transaction['total_amount'])

                transactions.append(transaction)

        self.transactions = pd.DataFrame(transactions)
        return self.transactions

    def save_data(self, output_dir='data/raw'):
        """Save generated data to CSV files."""
        os.makedirs(output_dir, exist_ok=True)

        if self.customers is not None:
            customers_path = os.path.join(output_dir, 'customers.csv')
            self.customers.to_csv(customers_path, index=False)
            print(f"Saved customers data to {customers_path}")

        if self.products is not None:
            products_path = os.path.join(output_dir, 'products.csv')
            self.products.to_csv(products_path, index=False)
            print(f"Saved products data to {products_path}")

        if self.transactions is not None:
            transactions_path = os.path.join(output_dir, 'transactions.csv')
            self.transactions.to_csv(transactions_path, index=False)
            print(f"Saved transactions data to {transactions_path}")

    def generate_all(self, output_dir='data/raw'):
        """Generate all data and save to files."""
        self.generate_customers()
        self.generate_products()
        self.generate_transactions()
        self.save_data(output_dir)

        print("\n=== Data Generation Summary ===")
        print(f"Customers: {len(self.customers)}")
        print(f"Products: {len(self.products)}")
        print(f"Transactions: {len(self.transactions)}")


if __name__ == "__main__":
    generator = EcommerceDataGenerator(
        num_customers=10000,
        num_products=500,
        num_transactions=50000
    )
    generator.generate_all()

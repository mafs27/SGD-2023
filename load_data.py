import pandas as pd
import psycopg2
from dotenv import dotenv_values

def query(connection, statement, values=None):
    cur = connection.cursor()
    try:
        cur.execute(statement, values)
        #new_item_id = cur.fetchone()[0]
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        connection.rollback()
    finally:
        cur.close()


def db_connection():

    db = psycopg2.connect(
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432',
        database='pet_store_db'
    )

    return db


conn = db_connection()

drop_tables = """
    DROP TABLE IF EXISTS item CASCADE;
    DROP TABLE IF EXISTS client CASCADE;
    DROP TABLE IF EXISTS purchase CASCADE;
    DROP TABLE IF EXISTS shoppingcart CASCADE;
    DROP TABLE IF EXISTS cartitem CASCADE;
    DROP TABLE IF EXISTS purchaseitem CASCADE;
    DROP TABLE IF EXISTS category CASCADE;
"""

query(conn, drop_tables)

create_tables = """
    CREATE TABLE category (
        name VARCHAR(512) UNIQUE,
        PRIMARY KEY(name)
    );

    CREATE TABLE item (
        item_id SERIAL PRIMARY KEY,
        name VARCHAR(512) NOT NULL,
        category VARCHAR(512) REFERENCES category(name),
        price REAL,
        stock INTEGER,
        description VARCHAR(512),
        manufacturer VARCHAR(512) NOT NULL,
        weight REAL NOT NULL,
        image_url VARCHAR(512) NOT NULL,
        total_unit_sales INTEGER
    );

    CREATE TABLE client (
        client_id VARCHAR(512),
        name VARCHAR(512) NOT NULL,
        email VARCHAR(512) NOT NULL,
        last_purch_date DATE,
        last_item_bought VARCHAR(512),
        PRIMARY KEY(client_id)
    );

    CREATE TABLE purchase (
        order_id SERIAL PRIMARY KEY,
        total_price REAL NOT NULL,
        order_date TIMESTAMP NOT NULL,
        client_client_id VARCHAR(512) REFERENCES client(client_id)
    );

    CREATE TABLE shoppingcart (
        data DATE,
        tempo TIMESTAMP,
        client_client_id VARCHAR(512) REFERENCES client(client_id),
        PRIMARY KEY(client_client_id)
    );

    CREATE TABLE cartitem (
        quantity INTEGER,
        item_item_id INTEGER REFERENCES item(item_id),
        shoppingcart_client_client_id VARCHAR(512) REFERENCES shoppingcart(client_client_id),
        PRIMARY KEY(shoppingcart_client_client_id, item_item_id)
    );

    CREATE TABLE purchaseitem (
        quantity INTEGER,
        purchase_order_id INTEGER REFERENCES purchase(order_id),
        item_item_id INTEGER REFERENCES item(item_id),
        PRIMARY KEY(purchase_order_id, item_item_id)
    );
    
    
    ALTER TABLE item ADD CONSTRAINT item_fk1 FOREIGN KEY (category) REFERENCES category(name);
    ALTER TABLE purchase ADD CONSTRAINT purchase_fk1 FOREIGN KEY (client_client_id) REFERENCES client(client_id);
    ALTER TABLE shoppingcart ADD CONSTRAINT shoppingcart_fk1 FOREIGN KEY (client_client_id) REFERENCES client(client_id);
    ALTER TABLE cartitem ADD CONSTRAINT cartitem_fk1 FOREIGN KEY (item_item_id) REFERENCES item(item_id);
    ALTER TABLE cartitem ADD CONSTRAINT cartitem_fk2 FOREIGN KEY (shoppingcart_client_client_id) REFERENCES shoppingcart(client_client_id);
    ALTER TABLE purchaseitem ADD CONSTRAINT purchaseitem_fk1 FOREIGN KEY (purchase_order_id) REFERENCES purchase(order_id);
    ALTER TABLE purchaseitem ADD CONSTRAINT purchaseitem_fk2 FOREIGN KEY (item_item_id) REFERENCES item(item_id);
"""

query(conn, create_tables)


# CATEGORIES table ---------------------------------------------------------------------------------------------------
categories_data = ['Food', 'Toys', 'Accessories']
for row in categories_data:
    #print(f'Inserting category data: {row}')
    query(conn, "INSERT INTO category (name) VALUES (%s)", (row,))


# ITEM table ---------------------------------------------------------------------------------------------------
items_data = [
    (1246, 'Premium Dog Bed', 'Accessories', 49.99, 100, 'Luxury bed for dogs', 'ComfyPets', 3.0, 'https://example.com/item-dog-bed.jpg', 5),
    (1537, 'Cat Tunnel', 'Toys', 19.99, 200, 'Interactive tunnel for cats', 'PlayfulPets Inc.', 1.2, 'https://example.com/item-cat-tunnel.jpg', 8),
    (1348, 'Pet Grooming Gloves', 'Accessories', 12.49, 150, 'Gloves for grooming pets', 'GroomingPro', 0.5, 'https://example.com/item-grooming-gloves.jpg', 12),
    (1449, 'Organic Dog Treats', 'Food', 8.99, 300, 'Healthy treats for dogs', 'OrganicTreats', 0.8, 'https://example.com/item-dog-treats.jpg', 15),
    (2240, 'Feather Teaser Toy', 'Toys', 5.49, 180, 'Feather teaser toy for cats', 'FeatherPlay', 0.3, 'https://example.com/item-feather-toy.jpg', 10),
    (1321, 'Large Dog Crate', 'Accessories', 79.99, 150, 'Spacious crate for large dogs', 'PetHaven', 10.0, 'https://example.com/item-dog-crate.jpg', 3),
    (2242, 'Interactive Laser Toy', 'Toys', 14.99, 120, 'Automatic laser toy for cats', 'LaserPlay', 0.5, 'https://example.com/item-laser-toy.jpg', 7),
    (1423, 'Adjustable Cat Harness', 'Accessories', 18.49, 180, 'Harness for walking cats', 'WalkSafe', 0.8, 'https://example.com/item-cat-harness.jpg', 5),
    (2324, 'Large Bag of Bird Seed', 'Food', 11.99, 250, 'Nutritious seed mix for birds', 'FeatherFeast', 1.0, 'https://example.com/item-bird-seed.jpg', 12),
    (1425, 'Durable Dog Chew Toy', 'Toys', 9.79, 200, 'Long-lasting chew toy for dogs', 'ChewMaster', 0.7, 'https://example.com/item-chew-toy.jpg', 8),
    (1821, 'Dei Acc', 'Accessories', 92.79, 150, 'Ouf Ouf Miau Miau', 'DEiPet', 0.7,'https://example.com/item-dei-toy.jpg', 8),
]

for row in items_data:
    #print(f'Inserting item data: {row}')
    query(conn, '''
        INSERT INTO item (item_id, name, category, price, stock, description, manufacturer, weight, image_url, total_unit_sales)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', row)


# CLIENT table ---------------------------------------------------------------------------------------------------
clients_data = [
    ('client101', 'Alice Johnson', 'alice@example.com', '2023-09-10', 'Interactive Laser Toy'),
    ('client202', 'Bob Smith', 'bob@example.com', '2023-09-15', 'Durable Dog Chew Toy'),
    ('client303', 'Eva Davis', 'eva@example.com', '2023-09-08', 'Large Bag of Bird Seed'),
    ('client404', 'Charlie Brown', 'charlie@example.com', '2023-09-12', 'Feather Teaser Toy'),
    ('client505', 'Olivia White', 'olivia@example.com', '2023-09-20', 'Cat Tunnel'),
    ('client606', 'Tom Hanks', 'tomhanks@example.com', '2023-04-02', 'Interactive Laser Toy'),
    ('client707', 'Jennifer Lopez', 'jlo@example.com', '2023-04-08', 'Feather Teaser Toy'),
    ('client808', 'Angelina Jolie', 'angelinajolie@example.com', '2023-03-22', 'Durable Dog Chew Toy')
]

for row in clients_data:
    #print(f'Inserting client data: {row}')
    query(conn, '''
        INSERT INTO client (client_id, name, email, last_purch_date, last_item_bought)
        VALUES (%s, %s, %s, %s, %s)
    ''', row)


# PURCHASE table ---------------------------------------------------------------------------------------------------
purchase_data = [
    (1721, 55.99, '2023-09-02 12:45:00', 'client303'),
    (1722, 32.49, '2023-09-04 16:30:00', 'client505'),
    (2773, 78.75, '2023-09-06 09:15:00', 'client101'),
    (2324, 45.29, '2023-09-08 14:20:00', 'client404'),
    (1825, 23.99, '2023-09-10 18:10:00', 'client202'),
    (1236, 56.32, '2023-10-10 18:20:00', 'client202'),
    (1127, 12.27, '2023-10-12 20:23:00', 'client808'),
]

for row in purchase_data:
    #print(f'Inserting purchase data: {row}')
    query(conn, '''
        INSERT INTO purchase (order_id, total_price, order_date, client_client_id)
        VALUES (%s, %s, %s, %s)
    ''', row)

# SHOPPINGCART table ---------------------------------------------------------------------------------------------------

shoppingcart_data = [
    ('2023-09-02', '2023-09-02 12:45:00', 'client202'),
    ('2023-09-04', '2023-09-04 16:30:00', 'client101'),
    ('2023-09-06', '2023-09-06 09:15:00', 'client404'),
    ('2023-09-12', '2023-09-12 11:45:00', 'client303'),
    ('2023-10-04', '2023-10-04 16:30:00', 'client808'),
    ('2023-10-16', '2023-10-16 09:15:00', 'client606'),
    ('2023-10-18', '2023-10-18 11:45:00', 'client505')
]

for row in shoppingcart_data:
    #print(f'Inserting shoppingcart data: {row}')
    query(conn, '''
        INSERT INTO shoppingcart (data, tempo, client_client_id)
        VALUES (%s, %s, %s)
    ''', row)

# CARTITEM table ---------------------------------------------------------------------------------------------------

cartitem_data = [
    (1, 1537, 'client101'),
    (3, 1425, 'client303'),
    (1, 1449, 'client404'),
    (2, 1348, 'client404'),
    (2, 2242, 'client202'),
    (12, 1821, 'client808'),
    (1, 2240, 'client606'),
    (3, 1246, 'client505')
]

for row in cartitem_data:
    #print(f'Inserting cartitem data: {row}')
    query(conn, '''
        INSERT INTO cartitem (quantity, item_item_id, shoppingcart_client_client_id)
        VALUES (%s, %s, %s)
    ''', row)

# PURCHASEITEM table ---------------------------------------------------------------------------------------------------

purchaseitem_data = [
    (2, 2324, 1449),
    (1, 2773, 2240),
    (2, 1722, 2242),
    (2, 2324, 1246),
    (3, 1721, 1423),
    (6, 1825, 2324),
    (5, 1127, 1821),
    (33, 1236, 1425),
]

for row in purchaseitem_data:
    #print(f'Inserting purchaseitem data: {row}')
    query(conn, '''
        INSERT INTO purchaseitem (quantity, purchase_order_id, item_item_id)
        VALUES (%s, %s, %s)
    ''', row)

print("Done!")
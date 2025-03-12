import flask
import logging
import datetime
import psycopg2
from load_data import db_connection
from flask import render_template
from dotenv import dotenv_values

StatusCodes = {
    'success': 200,
    'api_error': 400,  # error: bad request (request error)
    'internal_error': 500,  # error: internal server error (API error)
    'not_found': 404,
}

app = flask.Flask(__name__)

''' ####################### Endpoints '''

# http://127.0.0.1:8080/
@app.route('/')
def landing_page():
    return '''
        <h1>REST API Landing Page</h1>
        <p>Bem-vindo à Pet Store! </p>
        <img src= "https://i.ibb.co/xGLQswK/minifoto.png">
        <p>SGD 2023/2024</p>
    '''

# 1. Create Item: http://localhost:8080/proj/api/items (POST)
@app.route('/proj/api/items', methods=['POST'], strict_slashes=True)
def create_item():
    logger.info('POST /proj/api/items')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    needed_parameters = ['name', 'category', 'price', 'stock', 'description', 'manufacturer', 'weight', 'image_url']
    if set(needed_parameters).union(set(payload.keys())) != set(payload.keys()):
        response = {'status': StatusCodes['api_error'],
                    'errors': 'Incorrect Parameters'}
        return flask.jsonify(response), response['status']

    if payload['price'] < 0 or payload['stock'] < 0 or payload['weight'] < 0:
        response = {'status': StatusCodes['api_error'],
                    'errors': 'Price, Stock and Weight must be greater than or equal to 0'}
        return flask.jsonify(response), response['status']

    cur.execute("SELECT name FROM category;")
    existing_categories = {row[0] for row in cur.fetchall()}

    if payload['category'] not in existing_categories:
        # The category doesn't exist
        input_ = input(f"The category '{payload['category']}' does not exist. Do you want to create it? (y/n): ")

        if input_.lower() == 'y':

            cur.execute("INSERT INTO category (name) VALUES (%s);", (payload['category'],))
            conn.commit()

        else:

            response = {'status': StatusCodes['api_error'],
                        'errors': f"The category '{payload['category']}' does not exist and will not be created."}
            conn.rollback()
            conn.close()
            return flask.jsonify(response), response['status']

    statement = """INSERT INTO item (name, category, price, stock, description, manufacturer, weight, image_url, total_unit_sales)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING item_id"""

    values = (payload['name'],
              payload['category'],
              payload['price'],
              payload['stock'],
              payload['description'],
              payload['manufacturer'],
              payload['weight'],
              payload['image_url'],
              0  # total_unit_sales = 0 for a new item
              )
    try:
        cur.execute(statement, values)
        new_item_id = cur.fetchone()[0]
        conn.commit()  # commit the transaction

        response_data = {'Item_ID': new_item_id,
                         'Name': payload['name'],
                         'Category': payload['category'],
                         'Price': payload['price'],
                         'Stock': payload['stock'],
                         'Description': payload['description'],
                         'Manufacturer': payload['manufacturer'],
                         'Weight': payload['weight'],
                         'Image_URL': payload['image_url'],
                         'Total_Unit_Sales': 0}

        response = {'status': StatusCodes['success'],
                    'message': 'Item created successfully.',
                    'data': response_data}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /items - error: {error}')
        logger.error("Failed SQL:", cur.mogrify(statement, values))
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}
        conn.rollback()  # an error occurred, rollback

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 2. Update Item: http://localhost:8080/proj/api/items/{item_id} (PUT)
@app.route('/proj/api/items/<item_id>', methods=['PUT'], strict_slashes=True)
def update_item(item_id):
    logger.info(f'PUT /proj/api/items/{item_id}')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    cur.execute("SELECT EXISTS (SELECT 1 FROM item WHERE item_id = %s)", (item_id,))
    item_exists = cur.fetchone()[0]

    if not item_exists:
        response = {'status': StatusCodes['not_found'],
                    'message': 'Item not found.'}
        return flask.jsonify(response), response['status']

    new_category = payload.get('category')
    cur.execute("SELECT name FROM category;")
    existing_categories = {row[0] for row in cur.fetchall()}

    if new_category not in existing_categories:

        input_ = input(f"The category '{new_category}' does not exist. Do you want to create it? (y/n): ")

        if input_.lower() == 'y':

            cur.execute("INSERT INTO category (name) VALUES (%s);", (new_category,))
            conn.commit()

        else:

            response = {'status': StatusCodes['api_error'],
                        'errors': f"The category '{new_category}' does not exist and will not be created. Update canceled."}
            conn.rollback()
            conn.close()

            return flask.jsonify(response), response['status']

    if not any(param in payload for param in ['name', 'category', 'price', 'stock', 'description', 'manufacturer', 'weight', 'image_url']):
        response = {'status': StatusCodes['api_error'],
                    'errors': 'No valid parameters provided for update.'}

        return flask.jsonify(response), response['status']

    if payload['price'] < 0 or payload['stock'] < 0 or payload['weight'] < 0:
        response = {'status': StatusCodes['api_error'],
                    'errors': 'Price, Stock and Weight must be greater than or equal to 0'}
        return flask.jsonify(response), response['status']

    try:
        update_columns, update_values = [], []
        for key, value in payload.items():
            update_columns.append(f'{key} = %s')
            update_values.append(value)

        if update_columns:
            update_statement = f'UPDATE item SET {", ".join(update_columns)} WHERE item_id = %s'
            update_values.append(item_id)

            cur.execute(update_statement, update_values)

            response_data = {
                'id': item_id,
                'name': payload.get('name'),
                'category': payload.get('category'),
                'price': payload.get('price'),
                'stock': payload.get('stock'),
                'description': payload.get('description'),
                'manufacturer': payload.get('manufacturer'),
                'weight': payload.get('weight'),
                'image_url': payload.get('image_url')
            }

            response = {'status': StatusCodes['success'],
                        'message': 'Item updated successfully.',
                        'data': response_data}
            conn.commit()
        else:
            response = {'status': StatusCodes['api_error'],
                        'results': 'No valid update parameters provided'}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'],
                    'results': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 3. Delete Item from Cart: http://localhost:8080/proj/api/carts/{client_id}/items/{item_id} (DELETE)
@app.route('/proj/api/carts/<client_id>/items/<item_id>', methods=['DELETE'], strict_slashes=True)
def delete_item_from_cart(client_id, item_id):
    logger.info(f'DELETE /proj/api/carts/{client_id}/items/{item_id}')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT EXISTS (SELECT 1 FROM shoppingcart WHERE client_client_id = %s)", (client_id,))
        client_exists = cur.fetchone()[0]

        cur.execute("SELECT EXISTS (SELECT 1 FROM item WHERE item_id = %s)", (item_id,))
        item_exists = cur.fetchone()[0]

        if client_exists and item_exists:
            cur.execute("SELECT EXISTS (SELECT 1 FROM cartitem WHERE shoppingcart_client_client_id = %s AND item_item_id = %s)",
                        (client_id, item_id))
            item_in_cart = cur.fetchone()[0]

            if item_in_cart:
                cur.execute("DELETE FROM cartitem WHERE shoppingcart_client_client_id = %s AND item_item_id = %s",
                            (client_id, item_id))

                response = {'status': StatusCodes['success'],
                            'message': 'Item deleted from cart.'}

                conn.commit()
            else:
                response = {'status': StatusCodes['api_error'],
                            'message': 'Item not found in the cart for the specified client.'}
        else:
            response = {'status': StatusCodes['not_found'],
                        'message': 'Client or Item not found.'}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 4. Add Item to Cart: http://localhost:8080/proj/api/cart/{client_id} (POST)
@app.route('/proj/api/cart/<client_id>', methods=['POST'], strict_slashes=True)
def add_item_to_cart(client_id):
    logger.info(f'POST /proj/api/cart/{client_id}')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT EXISTS (SELECT 1 FROM shoppingcart WHERE client_client_id = %s)", (client_id,))
        cart_exists = cur.fetchone()[0]

        if not cart_exists:
            response = {'status': StatusCodes['not_found'],
                        'message': 'Cart not found.'}
            return flask.jsonify(response), response['status']

        request_data = flask.request.get_json()

        if 'item_id' not in request_data or 'quantity' not in request_data:
            response = {'status': StatusCodes['api_error'],
                        'message': 'Request body must contain "item_id" and "quantity".'}
            return flask.jsonify(response), response['status']

        item_id = request_data['item_id']
        quantity = request_data['quantity']

        cur.execute("SELECT EXISTS (SELECT 1 FROM item WHERE item_id = %s)", (item_id,))
        item_exists = cur.fetchone()[0]

        if not item_exists:
            response = {'status': StatusCodes['not_found'],
                        'message': 'Item not found.'}
            return flask.jsonify(response), response['status']
        if quantity < 0:
            response = {'status': StatusCodes['api_error'],
                        'message': '"quantity" must be greater than 0.'}
            return flask.jsonify(response), response['status']

        cur.execute("INSERT INTO cartitem (quantity, item_item_id, shoppingcart_client_client_id) VALUES (%s, %s, %s)",
                    (quantity, item_id, client_id))

        response = {'status': StatusCodes['success'],
                    'message': 'Item added to the shopping cart.'}

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 5. Get Items List: http://localhost:8080/proj/api/items (GET)
# exemplos:
# 1a pagina 10 itens nela: http://localhost:8080/proj/api/items?page=1&pageSize=10
# ordenar por norme: http://localhost:8080/proj/api/items?sort=name
# ordenar por preço: http://localhost:8080/proj/api/items?sort=price
# ordenar por preço, 2a pagina 7 itens nela: http://localhost:8080/proj/api/items?sort=price&page=2&pageSize=7
@app.route('/proj/api/items', methods=['GET'], strict_slashes=True)
def get_items_list():
    logger.info('GET /proj/api/items')
    conn = db_connection()
    cur = conn.cursor()

    try:
        base_query = """SELECT item_id, name, category, price, stock, description, manufacturer, weight,
                            image_url, total_unit_sales 
                        FROM item"""

        page = flask.request.args.get('page', default=1, type=int)
        limit = flask.request.args.get('limit', default=10, type=int)
        category = flask.request.args.get('category')
        sort = flask.request.args.get('sort')

        if page <= 0 or limit <= 0:
            response = {'status': StatusCodes['api_error'],
                        'message': 'Page and page size parameters must be positive integers.'}
            return flask.jsonify(response), response['status']

        if category:
            cur.execute("SELECT EXISTS (SELECT 1 FROM category WHERE category_id = %s)", (category,))
            category_exists = cur.fetchone()[0]
            if not category_exists:
                response = {'status': StatusCodes['api_error'],
                            'message': 'The specified category does not exist.'}
                return flask.jsonify(response), response['status']

        if sort and sort not in ['name', 'price']:
            response = {'status': StatusCodes['api_error'],
                        'message': 'The specified sorting option is not valid. Use "name" or "price".'}
            return flask.jsonify(response), response['status']

        if category:
            base_query += f" WHERE category_category_id = '{category}'"

        if sort == 'name':
            base_query += " ORDER BY name"
        elif sort == 'price':
            base_query += " ORDER BY price"

        offset = (page - 1) * limit
        base_query += f" LIMIT {limit} OFFSET {offset}"

        cur.execute(base_query)
        rows = cur.fetchall()

        response_data = []
        for row in rows:
            item = {'Item_ID': row[0],
                    'Name': row[1],
                    'Category': row[2],
                    'Price': row[3],
                    'Stock': row[4],
                    'Description': row[5],
                    'Manufacturer': row[6],
                    'Weight': row[7],
                    'Image_URL': row[8],
                    'Total_Unit_Sales': row[9]}
            response_data.append(item)

        response = {'status': StatusCodes['success'],
                    'message': 'Items retrieved successfully.',
                    'data': response_data}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /proj/api/items - error: {error}')
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 6. Get Item Details: http://localhost:8080/proj/api/items/{id} (GET)
@app.route('/proj/api/items/<item_id>', methods=['GET'], strict_slashes=True)
def get_item_details(item_id):
    logger.info(f'GET /proj/api/items/{item_id}')
    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM item WHERE item_id = %s", (item_id,))
        rows = cur.fetchall()

        if len(rows) == 0:
            response = {'status': StatusCodes['not_found'],
                        'error': 'Item not found'}
        else:
            row = rows[0]
            response_data = {'Item_ID': row[0],
                             'Name': row[1],
                             'Category': row[2],
                             'Price': row[3],
                             'Stock': row[4],
                             'Description': row[5],
                             'Manufacturer': row[6],
                             'Weight': row[7],
                             'Image_URL': row[8]}

            response = {'status': StatusCodes['success'],
                        'message': 'Item details retrieved successfully.',
                        'data': response_data}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /proj/api/items/{item_id} - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'message': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 7. Search Items: http://localhost:8080/proj/api/items/search/{item_name} (GET)
@app.route('/proj/api/items/search/<search>', methods=['GET'])
def search_items(search):
    logger.info('GET /proj/api/items/search')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM item WHERE lower(name) LIKE lower(%s)", ('%' + search + '%',))
        rows = cur.fetchall()

        logger.debug('GET /proj/api/items/search - parse')

        if not rows:
            response = {'status': StatusCodes['not_found'],
                        'message': "No items found for the given search criteria."}
        else:
            response_data = []
            for row in rows:
                logger.debug(row)
                content = {'Item_ID': row[0],
                           'Name': row[1],
                           'Category': row[2],
                           'Price': row[3],
                           'Stock': row[4],
                           'Description': row[5],
                           'Manufacturer': row[6],
                           'Weight': row[7],
                           'Image_URL': row[8]}

                response_data.append(content)

            response = {'status': StatusCodes['success'],
                        'message': "Items retrieved successfully.",
                        'data': response_data}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /proj/api/items/search - error: {error}')
        response = {'status': StatusCodes['internal_error'],
                    'results': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# 8. Get Top 3 Sales per Category: http://localhost:8080/proj/api/stats/sales (GET)
@app.route('/proj/api/stats/sales', methods=['GET'], strict_slashes=True)
def get_top_sales_per_category():
    logger.info('GET /proj/api/stats/sales')
    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(""" SELECT category.name AS category_name, item.name AS item_name, 
                               SUM(purchaseitem.quantity) AS total_sales
                        FROM item
                        JOIN purchaseitem ON item.item_id = purchaseitem.item_item_id
                        JOIN purchase ON purchaseitem.purchase_order_id = purchase.order_id
                        JOIN category ON item.category = category.name
                        GROUP BY category_name, item_name
                        ORDER BY category_name, total_sales DESC""")

        rows = cur.fetchall()

        top_sales_per_category = {}
        current_category = None
        current_category_data = []

        for row in rows:
            category_name, item_name, total_sales = row
            if category_name != current_category:
                if current_category is not None:
                    top_sales_per_category[current_category] = current_category_data[:3]

                current_category = category_name
                current_category_data = []

            current_category_data.append({'item_name': item_name, 'total_sales': total_sales})

        if current_category is not None:
            top_sales_per_category[current_category] = current_category_data[:3]

        response = {'status': StatusCodes['success'],
                    'message': 'Top 3 sales per category retrieved successfully.',
                    'data': {'top_sales_per_category': top_sales_per_category}}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /proj/api/stats/sales - error: {error}')
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 9. Purchase Items: http://localhost:8080/proj/api/purchase (POST)
@app.route('/proj/api/purchase', methods=['POST'], strict_slashes=True)
def purchase_items():
    logger.info('POST /proj/api/purchase')
    payload = flask.request.get_json()

    if 'cart' not in payload or 'client_id' not in payload:
        response = {'status': StatusCodes['api_error'],
                    'message': 'Invalid request payload'}
        return flask.jsonify(response), response['status']

    conn = db_connection()
    cur = conn.cursor()

    try:
        conn.autocommit = False
        total_price = 0

        client_id = payload['client_id']

        cur.execute('SELECT 1 FROM shoppingcart WHERE client_client_id = %s', (client_id,))
        if cur.fetchone() is None:
            response = {'status': StatusCodes['not_found'],
                        'message': f'Shopping cart not found for client: {client_id}'}
            return flask.jsonify(response), response['status']

        for item in payload['cart']:
            item_id, quantity = item['item_id'], item['quantity']

            if quantity < 0:
                response = {'status': StatusCodes['api_error'],
                            'message': '"quantity" must be greater than 0.'}
                return flask.jsonify(response), response['status']

            cur.execute('SELECT stock, price FROM item WHERE item_id = %s', (item_id,))
            row = cur.fetchone()

            if row is None:
                response = {'status': StatusCodes['not_found'],
                            'message': f'Item not found: {item_id}'}
                return flask.jsonify(response), response['status']

            stock, price = row

            if quantity > stock:
                response = {'status': StatusCodes['api_error'],
                            'message': f'Insufficient stock for item {item_id}'}
                return flask.jsonify(response), response['status']

            new_stock = stock - quantity
            cur.execute('UPDATE item SET stock = %s WHERE item_id = %s', (new_stock, item_id))

            total_price += quantity * price

        cur.execute('''INSERT INTO purchase (total_price, order_date, client_client_id)
                       VALUES (%s, NOW(), %s)
                       RETURNING order_id''', (total_price, client_id))
        order_id = cur.fetchone()[0]

        response = {'status': StatusCodes['success'],
                    'message': 'Purchase successful',
                    'data': {'total_price': total_price, 'order_id': order_id}}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /proj/api/purchase - error: {error}')
        conn.rollback()  # An error occurred, rollback
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}

    finally:
        if conn is not None:
            #conn.autocommit = True
            conn.close()

    return flask.jsonify(response), response['status']


# 10. Get Clients with Filters: http://localhost:8080/proj/api/clients (GET)
@app.route('/proj/api/clients', methods=['GET'], strict_slashes=True)
def get_clients_with_filters():
    logger.info('GET /proj/api/clients')
    conn = db_connection()
    cur = conn.cursor()

    try:
        last_purchase_date = flask.request.args.get('last_purchase_date', type=str)
        item_bought = flask.request.args.get('item_bought', type=str)

        base_query = """SELECT client.client_id, client.name, client.email,
                                MAX(purchase.order_date) AS last_purchase_date,
                                MAX(item.name) AS last_item_bought
                         FROM client
                         LEFT JOIN purchase ON client.client_id = purchase.client_client_id
                         LEFT JOIN purchaseitem ON purchase.order_id = purchaseitem.purchase_order_id
                         LEFT JOIN item ON purchaseitem.item_item_id = item.item_id"""

        where_conditions, params = [], []

        if last_purchase_date:
            where_conditions.append("purchase.order_date::date = %s")
            params.append(datetime.strptime(last_purchase_date, "%Y-%m-%d").date())

        if item_bought:
            where_conditions.append("item.name = %s")
            params.append(item_bought)

        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)

        base_query += """ GROUP BY client.client_id, client.name, client.email
                          ORDER BY last_purchase_date DESC NULLS LAST"""

        cur.execute(base_query, tuple(params))
        rows = cur.fetchall()

        results = []
        for row in rows:
            client = {'id': row[0],
                      'name': row[1],
                      'email': row[2],
                      'last_purchase_date': row[3].isoformat() if row[3] else None,
                      'last_item_bought': row[4]}
            results.append(client)

        response = {'status': StatusCodes['success'],
                    'message': 'Clients retrieved successfully.',
                    'data': results}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /proj/api/clients - error: {error}')
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 11. Add Client: http://localhost:8080/proj/api/clients (POST)
@app.route('/proj/api/clients', methods=['POST'], strict_slashes=True)
def add_client():
    logger.info('POST /proj/api/clients')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    try:
        required_fields = ['name', 'email']
        if not set(required_fields).issubset(set(payload.keys())):
            response = {'status': StatusCodes['api_error'],
                        'message': 'Missing required fields in the request body.'}
            return flask.jsonify(response), response['status']

        client_name, client_email = payload['name'], payload['email']

        cur.execute('SELECT COUNT(*) FROM client')
        count = cur.fetchone()[0]
        client_id = f'client{count + 1}'

        cur.execute('''INSERT INTO client (client_id, name, email) VALUES (%s, %s, %s)
                       RETURNING client_id''', (client_id, client_name, client_email))
        new_client_id = cur.fetchone()[0]
        conn.commit()

        response_data = {'id': new_client_id,
                         'name': client_name,
                         'email': client_email}

        response = {'status': StatusCodes['success'],
                    'message': 'Client added successfully.',
                    'data': response_data}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /proj/api/clients - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'message': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response['status']


# 12. Get Client Orders: http://localhost:8080/proj/api/clients/{client_id}/orders (GET)
@app.route('/proj/api/clients/<client_id>/orders', methods=['GET'], strict_slashes=True)
def get_client_orders(client_id):
    logger.info(f'GET /proj/api/clients/{client_id}/orders')
    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT EXISTS (SELECT 1 FROM client WHERE client_id = %s)", (client_id,))
        client_exists = cur.fetchone()[0]

        if not client_exists:
            response = {'status': StatusCodes['not_found'],
                        'message': 'Client not found.'}
            return flask.jsonify(response), response['status']

        cur.execute("""SELECT purchase.order_id, purchase.total_price, purchase.order_date,
                              purchaseitem.quantity, item.item_id
                       FROM purchase
                       JOIN purchaseitem ON purchase.order_id = purchaseitem.purchase_order_id
                       JOIN item ON purchaseitem.item_item_id = item.item_id
                       WHERE purchase.client_client_id = %s """, (client_id,))

        rows = cur.fetchall()

        if not rows:
            response = {'status': StatusCodes['not_found'],
                        'message': 'Client has no orders.'}
        else:
            orders = {}
            for row in rows:
                order_id, total_price, order_date, item_id, quantity = row
                if order_id not in orders:
                    orders[order_id] = {'order_id': order_id, 'total_price': total_price, 'order_date': order_date,
                                        'items': []}
                orders[order_id]['items'].append({'item_id': item_id, 'quantity': quantity})

            response_data = {'status': StatusCodes['success'],
                             'message': 'Client orders retrieved successfully.',
                             'data': list(orders.values())}
            response = response_data

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /proj/api/clients/{client_id}/orders - error: {error}')
        response = {'status': StatusCodes['internal_error'],
                    'message': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response), response["status"]


if __name__ == '__main__':

    logging.basicConfig(filename='log_file.log')
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s]:  %(message)s', '%H:%M:%S')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    host = '127.0.0.1'
    port = 8080
    app.run(host=host, debug=True, threaded=True, port=port)
    logger.info(f'API v1.0 online: http://{host}:{port}')
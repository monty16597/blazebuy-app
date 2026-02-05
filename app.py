import os
import time
import math
import uuid
import boto3
import logging
import multiprocessing
from datetime import datetime
from boto3.dynamodb.conditions import Key
from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user

app = Flask(__name__)
app.secret_key = 'supersecretkey'

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger("blazebuy-app")
DYNAMODB_ENDPOINT = os.environ.get('DYNAMODB_ENDPOINT', None)

# --- DynamoDB Setup (Same as before) ---
dynamodb = boto3.resource(
    'dynamodb',
    endpoint_url=DYNAMODB_ENDPOINT,
    region_name='us-east-1'
)

USERS_TABLE = 'BlazeBuyUsers'
ORDERS_TABLE = 'BlazeBuyOrders'


def init_db():
    """Create DynamoDB tables if they don't exist."""
    try:
        existing_tables = [t.name for t in dynamodb.tables.all()]

        if USERS_TABLE not in existing_tables:
            dynamodb.create_table(
                TableName=USERS_TABLE,
                KeySchema=[{'AttributeName': 'username', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'username', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )

        if ORDERS_TABLE not in existing_tables:
            dynamodb.create_table(
                TableName=ORDERS_TABLE,
                KeySchema=[
                    {'AttributeName': 'username', 'KeyType': 'HASH'},
                    {'AttributeName': 'order_id', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'username', 'AttributeType': 'S'},
                    {'AttributeName': 'order_id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
    except Exception as e:
        raise Exception("ERROR initializing database: %s", e)


init_db()
users_table = dynamodb.Table(USERS_TABLE)
orders_table = dynamodb.Table(ORDERS_TABLE)

# --- Flask Login Setup (Same as before) ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


class User(UserMixin):
    def __init__(self, username, first_name, last_name):
        self.id = username
        self.first_name = first_name
        self.last_name = last_name


# --- Global Error Handlers ---

@app.errorhandler(Exception)
def handle_unexpected_error(e):
    """Catch-all for unhandled exceptions so the app doesn't crash."""
    logger.exception("ERROR: Unhandled exception: %s", e)
    # For API calls, return JSON; for others, render a simple error page
    if request.accept_mimetypes.best == 'application/json':
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500
    flash('An unexpected error occurred. Please try again later.', 'danger')
    return redirect(url_for('index'))


@login_manager.user_loader
def load_user(user_id):
    try:
        response = users_table.get_item(Key={'username': user_id})
        if 'Item' in response:
            u = response['Item']
            return User(u['username'], u['first_name'], u['last_name'])
    except Exception as e:
        raise Exception("ERROR loading user %s: %s", user_id, e)
    return None


# --- HEAVY LOAD LOGIC ---
def process_payment_heavy_load(duration, process_id):
    """
    Consumes 100% of a single CPU core for the specified duration.
    """
    logger.info("[Process %s] STARTING HIGH CPU LOAD (%ss)", process_id, duration)
    end_time = time.time() + duration

    # We use a tight arithmetic loop to burn CPU cycles
    while time.time() < end_time:
        # Calculate large factorials and powers to keep the FPU busy
        _ = math.factorial(100)
        _ = [math.sqrt(i) ** 2 for i in range(1000)]

    logger.info("[Process %s] FINISHED", process_id)


# --- Routes ---

@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('shop'))
    return redirect(url_for('login'))


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        try:
            username = request.form['username']
            password = request.form['password']
            fname = request.form['fname']
            lname = request.form['lname']

            response = users_table.get_item(Key={'username': username})
            if 'Item' in response:
                flash('Username already exists', 'danger')
                return redirect(url_for('signup'))

            pw_hash = generate_password_hash(password)
            users_table.put_item(Item={
                'username': username, 'password': pw_hash,
                'first_name': fname, 'last_name': lname
            })

            user = User(username, fname, lname)
            login_user(user)
            return redirect(url_for('shop'))
        except Exception as e:
            raise Exception("ERROR during signup: %s", e)

    return render_template('signup.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        try:
            username = request.form['usernaem']
            password = request.form['password']

            response = users_table.get_item(Key={'username': username})
            if 'Item' in response:
                user_data = response['Item']
                if check_password_hash(user_data['password'], password):
                    user = User(user_data['username'], user_data['first_name'], user_data['last_name'])
                    login_user(user)
                    return redirect(url_for('shop'))

            flash('Invalid Credentials', 'danger')
        except Exception as e:
            raise Exception("ERROR during login: %s", e)
    return render_template('login.html')


@app.route('/shop')
@login_required
def shop():
    items = [
        {'id': 1, 'name': 'Quantum GPU', 'price': 999, 'desc': 'Process infinite data.'},
        {'id': 2, 'name': 'Neural Chip', 'price': 500, 'desc': 'Direct brain interface.'},
        {'id': 3, 'name': 'Hologram Projector', 'price': 1200, 'desc': '4K 3D projection.'},
        {'id': 4, 'name': 'AI Assistant Core', 'price': 2500, 'desc': 'Self-aware assistant.'},
    ]
    return render_template('shop.html', items=items)


@app.route('/cart')
@login_required
def cart():
    return render_template('cart.html')


@app.route('/checkout', methods=['POST'])
@login_required
def checkout():
    try:
        data = request.get_json()
        cart_items = data.get('items', [])
        total_price = data.get('total', 0)

        if not cart_items:
            return jsonify({'status': 'error', 'message': 'Cart is empty'}), 400

        # --- 1. TRIGGER MASSIVE CPU LOAD ---
        # # Duration: 600 seconds (10 minutes)
        # duration = 600

        # # Determine how many CPUs exist in the environment
        # # In Docker, this usually returns the host's CPU count unless restricted
        # cpu_count = multiprocessing.cpu_count()

        # logger.info("Spawning %s processes to stress all cores for %s seconds...", cpu_count, duration)

        # # Spawn one process per core to ensure 100% utilization across the board
        # for i in range(cpu_count):
        #     p = multiprocessing.Process(target=process_payment_heavy_load, args=(duration, i))
        #     p.start()

        # --- 2. Save Order to DynamoDB ---
        order_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()

        orders_table.put_item(Item={
            'username': current_user.id,
            'order_id': order_id,
            'timestamp': timestamp,
            'items': cart_items,
            'total_price': str(total_price)
        })

        return jsonify({'status': 'success', 'order_id': order_id, 'load_info': f'CPU stress test disabled.'})
    except Exception as e:
        raise Exception("ERROR during checkout: %s", e)


@app.route('/orders')
@login_required
def orders():
    response = orders_table.query(
        KeyConditionExpression=Key('username').eq(current_user.id)
    )
    orders = response.get('Items', [])
    orders.sort(key=lambda x: x['timestamp'], reverse=True)
    return render_template('orders.html', orders=orders)


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route('/health')
def health():
    try:
        # Simple read to ensure DynamoDB connectivity
        dynamodb.tables.all()
    except Exception as e:
        logger.exception("Health check failed: %s", e)
        return 'FAIL', 500
    return 'OK', 200


@app.errorhandler(404)
def page_not_found(e):
    """Handle 404 errors."""
    logger.warning("404 error: %s", request.url)
    return render_template('404.html'), 404


if __name__ == '__main__':
    try:
        app.run(host='0.0.0.0', port=5001, debug=True)
    except Exception as e:
        # Log any top-level exception and keep process alive (e.g., in a loop)
        raise Exception('ERROR: Flask app.run crashed; restarting main loop, error: %s', e)

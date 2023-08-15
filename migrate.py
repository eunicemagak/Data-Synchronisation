import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve database credentials from environment variables
sender_codes_db_config = {
    'host': os.getenv('SENDER_CODES_HOST'),
    'port': int(os.getenv('SENDER_CODES_PORT', 3306)),
    'user': os.getenv('SENDER_CODES_USER'),
    'password': os.getenv('SENDER_CODES_PASSWORD'),
    'database': os.getenv('SENDER_CODES_DATABASE')
}

sender_id_db_config = {
    'host': os.getenv('SENDER_ID_HOST'),
    'port': int(os.getenv('SENDER_ID_PORT', 3306)),
    'user': os.getenv('SENDER_ID_USER'),
    'password': os.getenv('SENDER_ID_PASSWORD'),
    'database': os.getenv('SENDER_ID_DATABASE')
}


# Connect to the databases
sender_codes_conn = mysql.connector.connect(**sender_codes_db_config)
sender_id_conn = mysql.connector.connect(**sender_id_db_config)

# Create cursors for both databases
sender_codes_cursor = sender_codes_conn.cursor(dictionary=True)
sender_id_cursor = sender_id_conn.cursor()

# Select data from sender_codes that is not in sender_id
select_query = """
    SELECT
        id, sender_name, type
    FROM
        `gateway`.`sender_codes`
    WHERE
        sender_name NOT IN (SELECT value FROM `gateway-v2`.`sender_id`)
"""

sender_codes_cursor.execute(select_query)
data_to_insert = sender_codes_cursor.fetchall()

# Define common values
has_opt_out_mapping = {"transactional": 1, "bulk": 0}
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Insert selected data into sender_id
insert_query = """
    INSERT INTO
        sender_id (value, airtel_sender, category, has_opt_out, active, type, serviceId, placeholder, created_at)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for row in data_to_insert:
    id, sender_name, type = row['id'], row['sender_name'], row['type']
    category = "transactional" if type == "transactional" else "bulk"
    has_opt_out = has_opt_out_mapping[category]
    
    sender_id_cursor.execute(insert_query, (sender_name, sender_name, category, has_opt_out, 1, "SMS", None, None, current_time))

# Commit changes and close connections
sender_id_conn.commit()
sender_codes_conn.close()
sender_id_conn.close()

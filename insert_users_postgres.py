import psycopg2
import csv
import random
import os

# Constants
HOST_NAME = "your_host"
DB_NAME = "your_database"

TABLE_NAME = "your_table"
COLUMN_NAME_UID = "uid_column_name"
PRIMARY_KEY_ID = "id"

MAX_ATTEMPTS = 5
UNIQUE_ID_LENGTH = 10
FILE_NAME = "file_name.csv"

def generate_uid(cursor):
    attempts = 0
    while attempts < MAX_ATTEMPTS:
        uid = ''.join(str(random.randint(0, 9)) for _ in range(UNIQUE_ID_LENGTH))

        # Check if the UID already exists in the database
        query = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {COLUMN_NAME_UID} = %s;"
        cursor.execute(query, (uid,))
        count = cursor.fetchone()[0]

        if count == 0:
            return uid

        attempts += 1

    raise ValueError("Failed to generate a unique UID after multiple attempts")

def create_users(connection, cursor, num_users):
    for _ in range(num_users):
        uid = generate_uid(cursor)

        query = f"INSERT INTO {TABLE_NAME} ({COLUMN_NAME_UID}) VALUES (%s) RETURNING {PRIMARY_KEY_ID};"
        values = (uid,)

        cursor.execute(query, values)
        new_id = cursor.fetchone()[0]  # Fetch the generated id
        connection.commit()

        print(f"User created: ID={new_id}, UID={uid}")

def get_all_uuids(connection, cursor):
    query = f"SELECT {COLUMN_NAME_UID} FROM {TABLE_NAME};"
    cursor.execute(query)
    uuids = [row[0] for row in cursor.fetchall()]
    return uuids

def write_uuids_to_csv(uuids, filename):
    with open(filename, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        for uid in uuids:
            csv_writer.writerow([f"'{uid}"])  # Add a single quote before UID (This is to avoid Leading Zero Issue for CSV)

    print(f"{len(uuids)} UUIDs written to {filename}")

def main():
    try:
        db_config = {
            "host": HOST_NAME,
            "database": DB_NAME,
            "user": input("Enter the database username: "),
            "password": input("Enter the database password: ")
        }

        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        num_users = int(input("Enter the number of users to create: "))
        create_users(connection, cursor, num_users)

        uuids = get_all_uuids(connection, cursor)
        script_directory = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(script_directory, FILE_NAME)
        write_uuids_to_csv(uuids, filename)

    except Exception as e:
        print("Error:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()

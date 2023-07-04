#!/usr/bin/python3

import mysql.connector
from datetime import datetime

def check_primary_key_exists(connection, table_name, primary_key_value):
    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    # Prepare the SQL query to check if primary key exists in the table
    query = f"SELECT COUNT(*) FROM {table_name} WHERE job = %s"
    
    # Execute the query with the primary key value
    cursor.execute(query, (primary_key_value,))
    
    # Fetch the result
    result = cursor.fetchone()

    # Check if primary key exists (count > 0)
    primary_key_exists = result[0] > 0
    
    # Close the cursor
    cursor.close()
    
    return primary_key_exists


def insert_lines(file_path, table_name, log_file_path):
    # Connect to the MySQL database
    connection = mysql.connector.connect(
        host="localhost",
        user="User",
        password="Password",
        database="Database"
    )

    # Open the file in read mode
    with open(file_path, 'r') as file:
        # Read lines from the file
        lines = file.readlines()

    # Open the log file in append mode

    log_file = open(log_file_path, 'a')
    
    # Loop through each line
    for line in lines:
        # Strip leading/trailing whitespace and split by the "|" symbol
        values = line.strip().split("|")
        
        # Get the primary key value
        primary_key_value = values[0]
        
        # Check if primary key already exists in the table
        primary_key_exists = check_primary_key_exists(connection, table_name, primary_key_value)

        if primary_key_exists:
            log_message = f"{datetime.now()} - Primary key {primary_key_value} already exists in the table. Skipping insertion.\n"
            print(log_message)
            log_file.write(log_message)            
        else:
            # Connect to the MySQL database again for insertion
            connection_insert = mysql.connector.connect(
                host="localhost",
                user="User",
                password="Password",
                database="Database"
            )
            
            # Create a cursor object to interact with the database
            cursor_insert = connection_insert.cursor()

            # Prepare the SQL query to insert values into the table
            query_insert = f"INSERT INTO {table_name} (X, X, X, X) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

            # Execute the query for the current line of values
            cursor_insert.execute(query_insert, values)

            # Commit the changes to the database
            connection_insert.commit()

            log_message = f"{datetime.now()} - Line {line.strip()} inserted successfully!\n"
            print(log_message)
            log_file.write(log_message)

            #print(f"Line {line.strip()} inserted successfully!"

            # Close the cursor and connection
            cursor_insert.close()
            connection_insert.close()

    # Close the log file
    log_file.close()

    # Close the connection
    connection.close()


# Provide the path to your text file
file_path = 'file/path'

# Provide the name of the table in your MySQL database
table_name = 'table_name'

# Provide the path to the log file
log_file_path = 'log/path'

# Call the function to insert lines into MySQL
insert_lines(file_path, table_name, log_file_path)

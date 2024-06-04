import json
import random
import psycopg2
import requests

from confluent_kafka import SerializingProducer


BASE_URL = "https://randomuser.me/api/?nat=gb"
PARTIES = ["Conservative Party", "The Liberals", "Independent Lot"]

random.seed(121)


def create_tables(conn, cur):
    """
    This function creates the necessary tables in the PostgreSQL database for the voting system.

    Parameters:
    conn (psycopg2.extensions.connection): The connection object to the PostgreSQL database.
    cur (psycopg2.extensions.cursor): The cursor object to execute SQL commands.

    Returns:
    None

    Raises:
    psycopg2.Error: If any error occurs while executing SQL commands.
    """

    # candidates table
    cur.execute(
        """
         CREATE TABLE IF NOT EXISTS candidates (
          candidate_id VARCHAR(255) PRIMARY KEY, 
          candidate_name VARCHAR(255),
          party_affiliation VARCHAR(255),
          biography TEXT,
          campaign_platform TEXT,
          date_of_birth DATE,
          nationality VARCHAR(255),
          registration_number VARCHAR(255),
          registration_date TIMESTAMP,
          address_street VARCHAR(255),
          address_city VARCHAR(255),
          address_state VARCHAR(255),
          phone_number VARCHAR(255),
          email VARCHAR(255),
          picture TEXT    
        )
        """
    )

    # voters table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
          voter_id VARCHAR(255) PRIMARY KEY, 
          voter_name VARCHAR(255),
          date_of_birth DATE,
          registration_date TIMESTAMP,
          gender VARCHAR(255),
          nationality VARCHAR(255),
          registration_number VARCHAR(255),
          address_street VARCHAR(255),
          address_city VARCHAR(255),
          address_state VARCHAR(255),
          phone_number VARCHAR(255),
          email VARCHAR(255),
          picture TEXT
        )
        """
    )

    # votes table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
          voter_id VARCHAR(255) UNIQUE, 
          candidate_id VARCHAR(255),
          voting_time TIMESTAMP,
          vote INT DEFAULT 1,
          PRIMARY KEY (voter_id, candidate_id)
        )
        """
    )
    conn.commit()


def generate_candidates_data(candidate_number, total_parties):
    """
    This function generates candidate data for the voting system.

    Parameters:
    candidate_number (int): The unique identifier for the candidate.
    total_parties (int): The total number of parties in the election.

    Returns:
    dict: A dictionary containing the candidate's data. If there is an error fetching data, 
          it returns a string "Error fetching data!!".

    Raises:
    requests.exceptions.RequestException: If there is an error fetching data from the API.
    """

    # Make a GET request to the API with the specified gender
    response = requests.get(BASE_URL + '&gender=' +
                            ('female' if candidate_number % 2 == 1 else 'male'), timeout=60)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        user_data = response.json()['results'][0]

        # Create a dictionary with the candidate's data
        candidate_data = {
            'candidate_id': user_data['login']['md5'],
            'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'picture': user_data['picture']['large'],
            'party_affiliation': PARTIES[candidate_number % total_parties],
            'biography': 'About candidate',
            'campaign_platform': "Manifestos",
            'date_of_birth': user_data['dob']['date'],
            'nationality': user_data['nat'],
            'registration_number': user_data['login']['uuid'],
            'registration_date': user_data['registered']['date'],
            'address_street': f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
            'address_city': user_data['location']['city'],
            'address_state': user_data['location']['state'],
            'phone_number': user_data['phone'],
            'email': user_data['email'],
        }

        return candidate_data
    else:
        # Raise an exception if there was an error fetching data
        raise requests.exceptions.RequestException("Error fetching data!!")


def generate_voter_data():
    """
    This function generates voter data for the voting system.

    Parameters:
    None

    Returns:
    dict: A dictionary containing the voter's data.

    Raises:
    requests.exceptions.RequestException: If there is an error fetching data from the API.

    """
    response = requests.get(BASE_URL, timeout=60)  # Make a GET request to the API

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        user_data = response.json()['results'][0]

        # Create a dictionary with the voter's data
        voter_data = {
            'voter_id': user_data['login']['md5'],
            'voter_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'picture': user_data['picture']['large'],
            'gender': user_data['gender'],
            'date_of_birth': user_data['dob']['date'],
            'nationality': user_data['nat'],
            'registration_number': user_data['login']['uuid'],
            'registration_date': user_data['registered']['date'],
            'address_street': f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
            'address_city': user_data['location']['city'],
            'address_state': user_data['location']['state'],
            'phone_number': user_data['phone'],
            'email': user_data['email'],
        }

        return voter_data
    else:
        # Raise an exception if there was an error fetching data
        raise requests.exceptions.RequestException("Error fetching data!!")


def insert_candidates_data(conn, cur, candidates):
    cur.execute(
        """
          INSERT INTO candidates (
            candidate_name, 
            candidate_id,
            party_affiliation, 
            biography, 
            campaign_platform, 
            date_of_birth, 
            nationality, 
            registration_number, 
            registration_date, 
            address_street, 
            address_city, 
            address_state, 
            phone_number, 
            email, picture)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON CONFLICT(candidate_id) DO NOTHING
        """,
        (
            candidates['candidate_id'],
            candidates['candidate_name'],
            candidates['party_affiliation'],
            candidates['biography'],
            candidates['campaign_platform'],
            candidates['date_of_birth'],
            candidates['nationality'],
            candidates['registration_number'],
            candidates['registration_date'],
            candidates['address_street'],
            candidates['address_city'],
            candidates['address_state'],
            candidates['phone_number'],
            candidates['email'],
            candidates['picture']
        )
    )

    conn.commit()


def insert_voters_data(conn, cur, voter):
    cur.execute(
        """
          INSERT INTO voters
            (
                voter_id, 
                voter_name, 
                date_of_birth, 
                registration_date, 
                gender, 
                nationality, 
                registration_number, 
                address_street, 
                address_city, 
                address_state, 
                phone_number, 
                email, 
                picture
            )
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            voter['voter_id'],
            voter['voter_name'],
            voter['date_of_birth'],
            voter['registration_date'],
            voter['gender'],
            voter['nationality'],
            voter['registration_number'],
            voter['address_street'],
            voter['address_city'],
            voter['address_state'],
            voter['phone_number'],
            voter['email'],
            voter['picture']
        )
    )

    conn.commit()


def delivery_report(err, msg):
    """
    This function is a callback function for the Kafka producer to handle message delivery reports.

    Parameters:
    err (Exception): The error object if the message delivery failed. If the message was successfully delivered, this will be None.
    msg (confluent_kafka.Message): The message object that was produced.

    Returns:
    None

    Raises:
    None

    """
    if err is not None:
        print('Message delivery failed')
    else:
        print(
            f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


if __name__ == "__main__":
   
    try:

        # Create a connection to the PostgreSQL database
        conn = psycopg2.connect(user="postgres", dbname="voting", password="postgres",
            host="localhost", port="5433")
        cur = conn.cursor()
        
         # Create a Kafka producer
        producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})


        # Create the tables
        create_tables(conn, cur)

        # Check if there are values in the candidates table
        # if not generate and insert data into it
        cur.execute(""" SELECT * FROM candidates; """)
        candidates_data = cur.fetchall()

        if len(candidates_data) == 0:
            for i in range(3):
                candidates_data = generate_candidates_data(i, 3)
                insert_candidates_data(conn, cur, candidates_data)

        # Generate and insert data into the voters table
        for i in range(100):
            voter_data = generate_voter_data()
            insert_voters_data(conn, cur, voter_data)

            # Produce a message to the voters topic
            producer.produce(
                "voters_topic",
                key=voter_data['voter_id'],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )
            print(f"Produced voter {i}, data: {voter_data}")
            producer.flush()

    except Exception as e:
        print(e)

import time
from datetime import datetime
import random
import simplejson as json
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from main import delivery_report

# configuring the consumer and producer
config = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(
    config | {
        'group.id': 'voting-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
)

producer = SerializingProducer(config)


if __name__ == "__main__":

    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="voting",
        host="localhost",
        port="5433",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    cur.execute(
        """
    SELECT row_to_json(col)
    FROM (
      SELECT * FROM candidates
    ) AS col;
    """
    )

    # Check if we have candidates in the database
    candidates = [candidate[0] for candidate in cur.fetchall()]
    if len(candidates) == 0:
        raise Exception("No candidates found in the database")
    else:
        print("There are candidates to vote for! Voting can Proceed")

    #
    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    'vote': 1
                }

                try:
                    print("User {} is voting for {}".format(
                        vote['voter_id'], vote['candidate_id']))

                    cur.execute(
                        """
                          INSERT INTO votes (
                            voter_id,
                            candidate_id,
                            voting_time)
                          VALUES (%s, %s, %s)
                        """,
                        (
                            vote['voter_id'],
                            vote['candidate_id'],
                            vote['voting_time']
                        )
                    )
                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)

                except Exception as e:
                    print(f"Error: {e}")
                    continue
            time.sleep(3)
    except KafkaException as e:
        print(e)

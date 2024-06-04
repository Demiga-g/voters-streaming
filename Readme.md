
# Step By Step Guide on How the Project was Set

You have kafka, spark, and docker installed



## Creating the Virtual Environment

Deactivate `base` conda environment (Assuming that you are using Anaconda and your terminal default to the `base` environment).

```bash
conda deactivate
```

Determined the python version and location

```bash
python3 -V
which python3
```

Updated pip

```bash
python3 -m pip install --upgrade
```

Installed `ensurepip`. 

* Use the second command if there are any dependency issues but first ensure you've installed `aptitude`.

```bash
sudo apt install python3.10-venv
sudo aptitude install python3.10-venv
```

Created and activated a virtual environment

* Third command is for deactivation

```bash
python3 -m venv stream_votes
source stream_votes/bin/activate
deactivate
```

Downloading the required packages

```bash
pip install -r requirements.txt
```

* Incase of slow internet speed disconnection, run with the last digit set to your preference.

```bash
pip config set global.timeout 300
```

* To fix issues with installation of `psycopg2`, run either of the following command and then re-install the packages
  
  * Run the second one if the first one doesn't work to either downgrade or upgrade the dependencies

```bash
sudo apt-get install libpq-dev
sudo aptitude install libpq-dev
```

* If the previous commands don't work change `psycopg2` to `psycopg2-binary` in the `requirements.txt` file.

* Just incase you want to remove the environment:

```bash
sudo rm -r stream_votes
```

## Starting Docker Containers

Ensure there is no running containers first before running the command below.

```bash
docker compose up -d
```


# Files

## `main.py`
 
Handles how and what data is entered into the database.

Here we have the `candidates`, `voters`, and `votes` table. 

* Note that a voter can only vote once but hence the unique nature in the `votes` table.

Data for candidates and voters is obtained from the [Random User Generator API](https://randomuser.me/) 


Check for the kafka topic and its content

```bash
kafka-topics --list --bootstrap-sever broker:29092
kafka-console-consumer --topic voters_topic bootstrap-server broker:29092
```


Connecting to postgres through CLI

```bash
pgcli -h localhost \
  -p 5433 -u postgres \
  -d votingqu
```

Pyspark version is 3.4.2 go to this [website](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.13)

Download the postgres jdbc driver [here](https://jdbc.postgresql.org/download/)


Created a checkpoint folder to store already processed data from the streams

```bash
mkdir checkpoints checkpoints/checkpoint1 checkpoints/checkpoint2
```

Check topic list

```bash
kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092
```

Create topic

```bash
kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic aggregated_votes_per_candidate
```

```bash
kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic aggregated_turnout_per_location
```

Check progress of in console

```bash
kafka-console-consumer.sh \
  --topic aggregated_votes_per_candidate \
  --bootstrap-server localhost:9092
```

Running things in streamlit

```bash
streamlit run streamlit-app.py
```
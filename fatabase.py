from neo4j import GraphDatabase
from faker import Faker
import random
import datetime
from faker.providers import job


faker = Faker()
faker.add_provider(job)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234567890"))


# use gplus_combined.txt to create the graph use a buffer because file is big
def read_file_in_chunks(file_path, chunk_size=1000):
    with open(file_path, 'r') as f:
        while True:
            chunk = f.readlines(chunk_size)
            if not chunk:
                break
            for line in chunk:
                yield line.strip()
data = {}
relationships = ["FRIEND", "FOLLOWER"]
profession = list(set([faker.job() for _ in range(10)]))
gender = ["Male", "Female", "Other"]
with driver.session() as session:
    for line in read_file_in_chunks('gplus_combined.txt'):
        user1, user2 = line.split()
        if data.get(user1) is None:
            data[user1] = (faker.name(),random.choice(gender), random.choice(profession))
        if data.get(user2) is None:
            data[user2] = (faker.name(),random.choice(gender), random.choice(profession))
        session.run(f"MERGE (u1:{data[user1][1]} " + "{name: $user1, profession: $pro1}) "
                    f"MERGE (u2:{data[user2][1]} " + "{name: $user2, profession: $pro2}) "
                    f"MERGE (u1)-[:{random.choice(relationships)}]-(u2)",
                    user1=data[user1][0], pro1=data[user1][2],user2=data[user2][0], pro2=data[user2][2])

# add a random node to the graph

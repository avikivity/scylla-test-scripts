#!/usr/bin/python3

import argparse
import subprocess
import math
import random
import time
from cassandra.cluster import Cluster

parser = argparse.ArgumentParser(description='Background reclaim tester')
parser.add_argument('mode', choices=['populate', 'run'])
parser.add_argument('--node')

args = parser.parse_args()

n_blob = 10
n_small = 50_000_000
size_blob = 10_000_000
runtime = 3600

# remove newlines and extra spaces from input
def unnl(s):
    import re
    return re.sub(r'\s+', ' ', s)

if args.mode == 'populate':
    cluster = Cluster([args.node])
    session = cluster.connect()
    session.execute('''
        create keyspace if not exists blobspace
        with replication = { 'class' : 'SimpleStrategy',
                             'replication_factor': 1 }
    ''')
    session.execute('use blobspace')
    session.execute('''
        create table if not exists tab (
            pk int,
            ck int,
            v blob,
            primary key (pk, ck)
        )
    ''')
    ps1 = session.prepare('insert into tab (pk, ck, v) values (?, ?, ?)')

    fragment = b'x' * 128_000
    for pk in range(n_blob):
        for ck in range(math.ceil(size_blob // len(fragment))):
            session.execute(ps1, (pk, ck, fragment))

    subprocess.run(
        unnl(f'''
            cassandra-stress write n={n_small} no-warmup -node {args.node}
            -rate threads=200 -schema keyspace=smallspace
            '''),
        shell=True, check=True
        )
    print("Don't forget to run nodetool flush and restart the server")
else:
    p1 = subprocess.Popen(
        unnl(f'''
            cassandra-stress mixed duration={runtime}s no-warmup -node {args.node}
            -rate threads=300 fixed=20000/s -schema keyspace=smallspace
            -pop dist="GAUSSIAN(1..{n_small},3)"
            | tee small.txt
        '''),
        shell=True)
    start = time.monotonic()

    cluster = Cluster([args.node])
    session = cluster.connect()
    session.execute('use blobspace')
    ps1 = session.prepare('select ck, v from tab where pk = ?')

    while time.monotonic() < start + runtime:
        pk = random.randint(0, n_blob - 1)
        records = session.execute(ps1, (pk,))
        for r in records:
            pass
        time.sleep(1)
    p1.wait()

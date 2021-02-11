#!/usr/bin/python3

import argparse
import subprocess


parser = argparse.ArgumentParser(description='Background reclaim tester')
parser.add_argument('mode', choices=['populate', 'run'])
parser.add_argument('--node')

args = parser.parse_args()

n_blob = 10
n_small = 100_000
size_blob = 10_000_000
runtime = 3600

# remove newlines and extra spaces from input
def unnl(s):
    import re
    return re.sub(r'\s+', ' ', s)

if args.mode == 'populate':
    subprocess.run(
        unnl(
            f'''
            cassandra-stress write n={n_blob} no-warmup -node {args.node}
            -rate threads=1 -schema keyspace=blobspace
            -col names=blob size="FIXED({size_blob})"
            '''),
        shell=True, check=True
    )
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
    p2 = subprocess.Popen(
        unnl(f'''
            cassandra-stress mixed duration={runtime}s no-warmup -node {args.node}
            -rate threads=1 fixed=1/s -schema keyspace=blobspace
            -col names=blob size="FIXED({size_blob})"
            -pop dist="seq(1..{n_blob})"
            | tee blob.txt
        '''),
        shell=True)
    p1.wait()
    p2.wait()

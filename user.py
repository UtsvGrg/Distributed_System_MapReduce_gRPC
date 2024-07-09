import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc
import os
import random
import subprocess
import time 

MIN_C = 0
MAX_C = 10

def worker_creator(command):
    subprocess.Popen(['cmd', '/c', 'start', 'cmd', '/k', command])
    time.sleep(0.5)

def run():

    print("Welcome to Kmeans by MapReduce")

    num_m = int(input("Number of Mappers: "))
    num_r = int(input("Number of Reducers: "))
    num_k = int(input("Number of Centroids: "))
    num_iter = int(input("Number of Iterations: "))
    
    # Initialized Random Centroid Points
    # f = open(r'Reducers\centroids.txt', 'w') 
    # for i in range(num_k):
    #     x = random.uniform(MIN_C,MAX_C)
    #     y = random.uniform(MIN_C,MAX_C)
    #     f.write(f"{x},{y}\n")
    # f.close()

    # Started master process
    command = "python master.py"  
    worker_creator(command)

    # And worker processes
    for i in range(num_m):
        command = f"python worker.py --type mapper --ip 800{i}"
        worker_creator(command)

    for i in range(num_r):
        command = f"python worker.py --type reducer --ip 900{i}"
        worker_creator(command)

    with grpc.insecure_channel("localhost:7001") as channel:
        stub = mapreduce_pb2_grpc.MasterStub(channel)
        response = stub.input_spliter(mapreduce_pb2.Text(data=f"{num_m},{num_r},{num_k},{num_iter}",metadata=''))
        print(response.data)
        response = stub.work_done(mapreduce_pb2.Text(data=f"{num_m},{num_r},{num_k},{num_iter}",metadata=''))
        print(response.data)



if __name__ == "__main__":
    run()

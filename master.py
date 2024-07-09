import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc
from concurrent import futures
import os
import time

    
class Master(mapreduce_pb2_grpc.MasterServicer):
    def input_spliter(self, request, context):
        m,r,k,it = map(int, request.data.split(','))
        point_list = []
        f = open('Data/Input/points.txt', 'r')

        for line in f:
            point_list.append(line)
        f.close()    
        
        file_list = []
        for i in range(m):
            os.makedirs(f"Mappers/M{i}", exist_ok=True)
            file_list.append(open(f"split_{i}", 'w'))
            
        for i, j in enumerate(point_list):
            file_list[(i%m)].write(j)  
        return mapreduce_pb2.Text(data=f"Input Split Created Successfully")
    
    def work_done(self, request, context):
        m,r,k,ite = map(int, request.data.split(','))

        for it in range(ite):

            for i in range(m):
                with grpc.insecure_channel(f"localhost:800{i}") as channel:
                    stub = mapreduce_pb2_grpc.WorkerStub(channel)
                    response = stub.do_map(mapreduce_pb2.Text(data=f"{i},{r}",metadata=''))
                    print(response.data)
            print("All mappers have returned for iter:", it)

            for i in range(r):
                with grpc.insecure_channel(f"localhost:900{i}") as channel:
                    stub = mapreduce_pb2_grpc.WorkerStub(channel)
                    response = stub.do_reduce(mapreduce_pb2.Text(data=f"{i},{m}",metadata=''))
                    print(response.data)
            print("All reducers have returned for iter:", it)

            list_centroid = []
            
            f = open('Reducers/centroids.txt', 'r')
            for line in f:
                list_centroid.append(line.strip())  
                
            f.close()
            
            for i in range(r):
                f = open(f'Reducers/R{i}', 'r')
                for line in f:
                    temp = line.strip().split(',')
                    list_centroid[int(temp[0])] = f"{temp[1]},{temp[2]}"
                f.close()
                    
            print(list_centroid)
            f = open('Reducers/centroids.txt', 'w')
            for line in list_centroid:
                f.write(line+'\n')
            f.close()

        return mapreduce_pb2.Text(data=f"MapReduce Complete: Centroids Generated")
        

    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MasterServicer_to_server(Master(), server)
    server.add_insecure_port("[::]:7001")
    server.start()
    print("Master server started listening")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
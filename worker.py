import argparse
import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc
from concurrent import futures

INFINTE_VAL = 9999999

class Worker(mapreduce_pb2_grpc.WorkerServicer):

    def do_map(self, request, context):

        num, num_r = map(int, request.data.split(','))
        list_centroid = []
        
        f = open('Reducers/centroids.txt', 'r')
        for line in f:
            list_centroid.append(line.strip())
        f.close()
    
        split_file = f'split_{num}'
        f = open(split_file, 'r')
        intermediate_output = []    

        for line in f:
            
            min_d = INFINTE_VAL
            out_c = None
            point = line.strip().split(',')
            x = eval(point[0])
            y = eval(point[1])
            
            for i, centroid in enumerate(list_centroid):
                point = centroid.split(',')
                c_x = eval(point[0])
                c_y = eval(point[1])
                temp_d = (x-c_x)**2 + (y-c_y)**2

                if temp_d<min_d:
                    min_d = temp_d
                    out_c = i
                    
            intermediate_output.append(f'{out_c},{line.strip()}\n')
        f.close()
                        
        file_list = []
        for i in range(num_r):
            file_list.append(open(f"Mappers/M{num}/partition_{i}", 'w'))

        for i, j in enumerate(intermediate_output):
            key = eval(j.split(',')[0])
            file_list[(key%num_r)].write(j)

        return mapreduce_pb2.Text(data=f"Map SUCCESS")


    def do_reduce(self, request, context):
        num, num_m = map(int, request.data.split(','))

        all_val_list = []

        for i in range(num_m):
            f = open(f"Mappers/M{i}/partition_{num}")
            for line in f:
                all_val_list.append(line.strip())

        keys_dict = {}
        for i in all_val_list:
            temp = i.split(',')
            if temp[0] in keys_dict:
                keys_dict[temp[0]].append(temp)
            else:
                keys_dict[temp[0]] = [temp]
            
        centroid_dict = {}
        
        for keys in keys_dict:
            c_x = 0
            c_y = 0
            temp = keys_dict[keys]
            for i in temp:
                x = eval(i[1])
                y = eval(i[2])
                c_x += x
                c_y += y
            centroid_dict[keys] = f"{c_x/len(temp)},{c_y/len(temp)}"
        
        f = open(f'Reducers/R{num}', 'w')
        for key in centroid_dict:
            f.write(f"{key},{centroid_dict[key]}")
            f.write('\n')

        return mapreduce_pb2.Text(data=f"Reduce SUCCESS")
        

def mapper(ipaddr):
    print(f"mapper started at ip: {ipaddr}")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    mapreduce_pb2_grpc.add_WorkerServicer_to_server(Worker(), server)
    server.add_insecure_port(f"[::]:{ipaddr}")
    server.start()
    print(f"Mapper server started listening at {ipaddr}")
    server.wait_for_termination()


def reducer(ipaddr):
    print(f"reducer started at ip: {ipaddr}")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    mapreduce_pb2_grpc.add_WorkerServicer_to_server(Worker(), server)
    server.add_insecure_port(f"[::]:{ipaddr}")
    server.start()
    print(f"Reducer server started listening at {ipaddr}")
    server.wait_for_termination()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--type', type=str)
    parser.add_argument('--ip', type=str)
    args = parser.parse_args()

    if args.type == 'mapper':
        mapper(args.ip)
    else:
        reducer(args.ip)
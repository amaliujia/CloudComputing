import numpy as np
import os
import sys

import time
import pyspark

def to_csv(x):
    l = x[1].tolist()[0]    
    ret = []
    for i in l:
        ret.append(str(i))
    return ','.join(ret)    

def get_parameters():
    params = dict()
    params['block'] = 8 
    params['num_iter'] = 30 
    params['eta'] = 0.015
    params['eta_decay'] = 0.99
    #params['input_file'] = "/user/deepbic/ratings_1M.csv"
    return params

def map_line(line):
    tokens = line.split(",")
    return int(tokens[0]), int(tokens[1]), float(tokens[2])

# x is (row_block_id, (i, j, rating))
def filter_blocks(x, offset, col_dim):
    col_dim_id = x[1][1] / col_dim
    return (x[0] + offset) % num_workers == col_dim_id

# x is (i, factor vector)
def assign_block_W(x, row_dim):
    i, _ = x
    return i / row_dim

# y is (j, factor vector)
def assign_block_H(x, col_dim):
    j, _ = x
    return j / col_dim

# x is (i, j, rating)
def assign_block(x, row_dim):
    return x[0] / row_dim

def dsgd(x):
    tmp = x.next()
    iter_list = tmp[1]
    Z = iter_list[0]
    W = iter_list[1]
    H = iter_list[2]
    W_dict = {}
    H_dict = {}
    # i is (i, factor vector)
    for i in W:
        W_dict[i[0]] = i[1] 
        # j is (j, factor vector)
    for i in H:
        H_dict[i[0]] = i[1]
    count = 0
    for entry in Z:
        (i, j, rating) = entry
        if i not in W_dict:
            W_dict[i] = np.random.rand(1, rank).astype(np.float32)
        if j not in H_dict:
            H_dict[j] = np.random.rand(1, rank).astype(np.float32)
        diff = rating - np.dot(W_dict[i], H_dict[j].T)
        # W
        W_gradient = -2 * diff * H_dict[j]
        #W_dict[i] -= eta_bc.value * W_gradient
        # H
        H_gradient = -2 * diff * W_dict[i]
        H_dict[j] -= eta_bc.value * H_gradient
        W_dict[i] -= eta_bc.value * W_gradient
    return (tuple(['W',W_dict.items()]), tuple(['H',H_dict.items()]))

def evaluate(block_data, W, H):
    W_dict = {}
    H_dict = {}
    for ele in W:
        id, (i, vec) = ele 
        W_dict[i] = vec
    for ele in H:
        id, (i, vec) = ele 
        H_dict[i] = vec
    count = 0 
    error = 0 
    for entry in block_data:
        i, j, rating = entry
        count += 1
        error += (rating - np.dot(W_dict[i], H_dict[j].T)) ** 2 
    return error[0][0], np.sqrt(error / count)[0][0]

input_z = sys.argv[1] #input
rank = int(sys.argv[2]) # rank
output_w = sys.argv[3] # W
output_h = sys.argv[4] # H

params = get_parameters()
blocks = params['block']
num_workers = blocks

sc = pyspark.SparkContext(conf=pyspark.SparkConf())
rank_b = sc.broadcast(rank)
log = ""
t = time.clock()
block_data = sc.textFile(input_z).map(map_line)

user_num = block_data.max(lambda x : x[0])
movie_num = block_data.max(lambda x : x[1])

log += "compute dimension of matrix"
log += str(time.clock() - t) 
log += "\n"

user_num = user_num[0] + 1
movie_num = movie_num[1] + 1
    
# block sizes
blocks_row_dim = int((user_num - 1 + params['block']) / params['block'])
blocks_col_dim = int((movie_num - 1 + params['block']) / params['block'])
eta_bc = sc.broadcast(params['eta'])

W = []
for i in range(0, user_num):
    W.append((i, np.random.rand(1, rank).astype(np.float32)))
H = []
for i in range(0, movie_num):
    H.append((i, np.random.rand(1, rank).astype(np.float32)))

W_rdd = sc.parallelize(W).keyBy(lambda x : assign_block_W(x, blocks_row_dim)).partitionBy(num_workers) 
H_rdd = sc.parallelize(H).keyBy(lambda x : assign_block_H(x, blocks_col_dim)).partitionBy(num_workers)

t = time.clock()
log += "Init factor matrix rdd"
log += str(time.clock() - t) 
log += "\n"

key_data = block_data.keyBy(lambda x : assign_block(x, blocks_row_dim)).partitionBy(num_workers).cache()
log += "iteration" +  " seconds" + " squared_error" +  " RMSE\n"
t1 = time.clock() 
for j in range(0, params['num_iter']):
    for i in range(0, blocks):
        key_data_filter = key_data.filter(lambda x : filter_blocks(x, i, blocks_col_dim))
        merged_rdd = key_data_filter.groupWith(W_rdd, H_rdd)
        update_rdd = merged_rdd.mapPartitions(lambda x : dsgd(x)).reduceByKey(lambda x,y: x + y)
        W_new_rdd = update_rdd.filter(lambda x: x[0]=='W').flatMap(lambda x: x[1])
        H_new_rdd = update_rdd.filter(lambda x: x[0]=='H').flatMap(lambda x: x[1])
        W_rdd = W_new_rdd.keyBy(lambda x : assign_block_W(x, blocks_row_dim)).partitionBy(num_workers) 
        H_rdd = H_new_rdd.keyBy(lambda x : assign_block_H(x, blocks_col_dim)).partitionBy(num_workers) 
    W_new = W_rdd.collect()
    H_new = H_rdd.collect()
    block_data_py = block_data.collect()
    error, RSME = evaluate(block_data_py, W_new, H_new)  
    t2 = time.clock()
    log = log + str(j) + " " + str(t2 - t1) + " " + str(error) + " " + str(RSME) + "\n" 
    cur_eta = eta_bc.value 
    eta_bc.unpersist()
    cur_eta *= 0.99
    eta_bc = sc.broadcast(cur_eta) 
W_csv_rdd = W_rdd.map(lambda x : x[1]).sortBy(lambda x: x[0]).map(to_csv)
print W_csv_rdd.take(15)
#W_csv_rdd.coalesce(1).saveAsTextFile(output_w) 
#csv_rdd = H_rdd.map(lambda x : x[1]).sortBy(lambda x: x[0]).map(to_csv)
#H_csv_rdd.coalesce(1).saveAsTextFile(output_h) 

print log

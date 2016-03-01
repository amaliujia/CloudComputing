import os
import sys
import numpy as np

from numpy.random import rand
import time
import pyspark

def map_line(line):
    tokens = line.split(",")
    return int(tokens[0]), int(tokens[1]), float(tokens[2])

def get_parameters():
    params = dict()
    params['block'] = 2 
    params['rank'] = 16 
    params['num_iter'] = 3
    params['eta'] = 0.01
    params['eta_decay'] = 0.99
    params['num_worker'] = 8
    params['input_file'] = "/user/deepbic/ratings_1M.csv"
    params['output_file'] = "/user/deepbic/out1.txt"
    return params

def assign_block(i,j):
    block_row_id = (i - 1) / blocks_row_dim_global.value
    block_col_id = (j - 1) / blocks_col_dim_global.value
    return (block_row_id, block_col_id)

def dsgd_on_block(entry):
    #print ""
    i, j, rating = entry
    W = W_bc.value
    H = H_bc.value
    diff = rating - np.dot(W[i-1], H[j-1])
    W_gradient = -2 * diff * H[j-1]
    H_gradient = -2 * diff * W[i-1]
    #print "diff " + str(diff)
    #print W_gradient
    #print H_gradient
    #print ""
    return i, j, W_gradient, H_gradient


def dsgd_on_block_b(block):
    i, j, rating = block
    W = W_bc.value
    H = H_bc.value
    diff = rating - np.dot(W[i-1], H[j-1])
    W_gradient = -2 * diff * H[j-1]
    H_gradient = -2 * diff * (W[i-1] - eta_global.value * W_gradient)
    return i, j, W_gradient, H_gradient


def compute_loss(entry):
    i, j, rating = entry 
    #WH = HW_bc.value
    W = W_bc.value
    H = H_bc.value
    #return (rating - WH[i-1][j-1]) ** 2
    return (rating - np.dot(W[i-1], H[j-1]))**2

if __name__ == '__main__':
    sc = pyspark.SparkContext(conf=pyspark.SparkConf())

    # get parameters
    params = get_parameters()
    blocks = params['block']
    
    log = ""
   
    block_data = sc.textFile("/user/deepbic/ratings_1M.csv").map(map_line).cache()

    # sort user_id and movie id in desc order.
    user_id = block_data.map(lambda x : x[0]).distinct().sortBy(lambda x : x, False)
    movie_id = block_data.map(lambda x : x[1]).distinct().sortBy(lambda x : x, False)

    # get the largest user_id and max_movide_id, in order to divided matrix
    # Z to blocks.
    user_num = user_id.first()
    movie_num = movie_id.first()

    # initialize H and W matrix
    W = rand(user_num, params['rank'])
    H = rand(movie_num, params['rank'])
    # reset type as 4 bytes float to save memory
    W = W.astype(np.float32, copy=False)
    H = H.astype(np.float32, copy=False)

    # block sizes
    blocks_row_dim = (user_num - 1 + params['block']) / params['block']
    blocks_col_dim = (movie_num - 1 + params['block']) / params['block']

    blocks_global = sc.broadcast(blocks)
    blocks_row_dim_global = sc.broadcast(blocks_row_dim)
    blocks_col_dim_global = sc.broadcast(blocks_col_dim)
    eta_global = sc.broadcast(params['eta'])
    eta_decay_global = sc.broadcast(params['eta_decay'])
    worker_global = sc.broadcast(params['num_worker'])

    #block_data = data.map(lambda r : r[0], r[1], r[2]).sortByKey().cache()
    #block_data = data.sortByKey().cache()

    log = log + "iteration" +  " seconds" + " squared_error" +  " RMSE\n"
    t1 = time.clock()
    step_size = eta_global.value
    for i in range(0, params['num_iter']):
        W_bc = sc.broadcast(W)
        H_bc = sc.broadcast(H)

        gradients = block_data.map(dsgd_on_block).collect()

        W_bc.unpersist()
        H_bc.unpersist()

        for i, j, W_gradient, H_gradient in gradients:
            W[i-1] -= step_size * W_gradient
            H[j-1] -= step_size * H_gradient

        step_size *= eta_decay_global.value
        #HW_bc = sc.broadcast(np.dot(W, H.transpose()))
        
	W_bc = sc.broadcast(W)
        H_bc = sc.broadcast(H)
       
        loss_data = block_data.map(compute_loss).cache()

        n = loss_data.count()
        error = loss_data.reduce(lambda a, b : a + b)
        #HW_bc.unpersist()
        loss_data.unpersist()
        t2 = time.clock()
        
        W_bc.unpersist()
        H_bc.unpersist()
        log = log + str(i) + " " + str(t2 - t1) + " " + str(error) + " " + str(np.sqrt(error / n)) + "\n"

    blocks_global.unpersist()
    blocks_row_dim_global.unpersist()
    blocks_col_dim_global.unpersist()
    eta_global.unpersist()
    eta_decay_global.unpersist()
    worker_global.unpersist()

    print log 

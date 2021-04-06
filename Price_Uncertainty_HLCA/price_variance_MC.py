import numpy as np
import time
import pandas as pd




def do_price_MC(M_io, Lp, Cu, price_data, Nruns = 10):
    """Performs a Monte Carlo based on price variations.
    Input:
    M_io           The Characteristation multiplier matrix, tehcnically C.dot(S).dot(L_io)
    Lp             Lenontief inverse of the LCA A-matrix
    Cu             Unscaled Cut off matrix
    price_data     numpy array containing relevan price sample data or ones for the processes w.o. external prices
    Nruns          Number of MC runs to perform
    """
    print("Starting run at {}".format(time.ctime()))
    t0 = time.time()
    results = np.zeros((M_io.shape[0],Lp.shape[1], Nruns), dtype='float32')
    print("Results shape: ", results.shape)
    x = max(Nruns//50, 1)
    for i in range(Nruns):
        if i%x == 0:
            print("Run {}".format(i+1))
        # Next line is optional but not necessary, if not used it will just always
        # regenerate the same distribution but the samples are independent anyways.
        # price_vector = generate_price_vector(price_data)
        price_vector = price_data[:,i]
        Cu_sample = Cu.multiply(price_vector)
        results[:,:,i] =  M_io.dot(Cu_sample).dot(Lp)
    print("Finished run at {}".format(time.ctime()))
    dt = time.time() - t0
    print("Finished {} runs in {} minutes and {} seconds".format(Nruns, dt//60, dt%60))
    return results


def generate_price_vector(price_data_array):
    """
    Randomly samples columns for each row in the given array.
    As the prices for each proces/product can vary independently.
    Input:
        Price array[processes, samples]
    Output:
        Updated Price vector to be multiplied with the unscaled Cu matrix
    """
    shape = price_data_array.shape
    return price_data_array[np.arange(shape[0]), np.random.randint(shape[1],size=shape[0])]





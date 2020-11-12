import numpy as np
import dask.array as da
import h5py
import cProfile


def multiply_1024():
    file = h5py.File('1024.hdf5', 'a')
    for i in range(1024):
        A = np.random.rand(1024, 1024)
        B = np.random.rand(1024, 1024)
        file.create_dataset('/'+str(i), (1024, 1024), data=A@B)




def multiply_16384():
    file = h5py.File('16384.hdf5', 'a')
    for i in range(16):
        A = np.random.rand(16384, 16384)
        B = np.random.rand(16384, 16384)
        file.create_dataset('/'+str(i), (16384, 16384), data=A@B)




def multiply_266144():
    file = h5py.File('266144.hdf5', 'a')
    da.random.random((262144, 262144)).to_hdf5('262144.hdf5', '/a')
    da.random.random((262144, 262144)).to_hdf5('262144.hdf5', '/b')
    out = file.create_dataset('/out', (262144, 262144))
    A = file['/a']
    B = file['/b']


    a = da.from_array(A, chunks=A.chunks)
    b = da.from_array(B, chunks=B.chunks)
    c = a@b
    c.store(out)
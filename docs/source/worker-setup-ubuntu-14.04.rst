Setting Up a Worker on an Ubuntu 14.04 LTS system
====

-----

**Note:** This setup procedure uses ``apt-get`` for all packages. Do **NOT** use Python's ``pip`` utility 
to install any of the Python libraries mentioned in this procedure.

-----

#.  Start with a fresh install of 14.04 LTS Server (64-bit).

#.  Install setuptools, numpy (1.8.2), scipy (0.13.3), cython (0.20.1), mpi4py, and pymc using apt-get:

    ``apt-get install python-setuptools python-numpy python-scipy cython python-mpi4py python-pymc``

#.  Install version 331 of the NVIDIA driver:

    ``apt-get install nvidia-331``

#.  Reboot the server, and check the driver is working using:

    ``nvidia-smi``

#.  Install version 5.5 of the NVIDIA CUDA toolkit:

    ``apt-get install nvidia-cuda-toolkit``

#.  Run the nvidia compiler w/ the version option to make sure CUDA is working:

    ``nvcc --version``

#.  Install pycuda:

    ``apt-get install python-pycuda``

#.  Install the development files for libarmadillo & libboost (1.54):

    ``apt-get install libarmadillo-dev libboost-dev``

#.  Install git to clone the flow packages.

    ``apt-get install git``

#.  Clone the following repositories:

    ::

        git clone https://github.com/andrewcron/cy_armadillo.git
        git clone https://github.com/andrewcron/cyrand.git
        git clone https://github.com/dukestats/gpustats.git
        git clone https://github.com/andrewcron/dpmix.git
        git clone https://github.com/whitews/FlowIO.git
        git clone https://github.com/whitews/FlowUtils.git
        git clone https://github.com/whitews/FlowStats.git
        git clone https://github.com/whitews/ReFlowRESTClient.git

#.  Install the cloned libraries using ``python setup.py install`` in the following order:

    * cyrand
    * cyarma (cy_armadillo)
    * gpustats
    * dpmix
    * FlowIO
    * FlowUtils
    * FlowStats
    * ReFlowRESTClient

#.  Clone the ReFlowWorker git repository, but don't install it as a library.

#.  Create a new worker on the ReFlow server and get the new worker's token from the Django admin site.

#.  On the worker's client machine (Ubuntu) add the worker config file as:

    ``/etc/reflow_worker.conf``

#.  Edit the file and add the JSON content (edit with proper values):

    ::

        {
            "host": "<reflow_server_ip_address",
            "name": "<worker_name>",
            "token": "<worker_token>"
        }


#.  As root, from the ``ReFlowWorker/reflowworker`` directory, start the worker:

    ``python worker.py start``

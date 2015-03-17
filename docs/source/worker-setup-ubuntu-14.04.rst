Setting Up a Worker on an Ubuntu 14.04 LTS system
====

#.  Start with a fresh install of 14.04 LTS Server (64-bit).

#.  Install version 331 of the NVIDIA driver & the associated dev files:

    ``apt-get install nvidia-331 nvidia-331-dev``

#.  Reboot the server, and check the driver is working using:

    ``nvidia-smi``

#.  Install version 5.5 of the NVIDIA CUDA toolkit:

    ``apt-get install nvidia-cuda-toolkit``

#.  Run the nvidia compiler w/ the version option to make sure CUDA is working:

    ``nvcc --version``

#.  Install numpy (1.8.2), scipy (0.13.3), and cython (0.20.1) using apt-get:

    ``apt-get install python-numpy python-scipy cython``

#.  Install the development files for libarmadillo & libboost (1.54):

    ``apt-get install libarmadillo-dev libboost-dev``

#.  Install git to clone the flow packages.

    ``apt-get install git``

#.  Clone the following repositories:

    ::
        mkdir git
        cd git
        git clone --recursive http://git.tiker.net/trees/pycuda.git
        git clone https://github.com/andrewcron/cy_armadillo.git
        git clone https://github.com/andrewcron/cyrand.git
        git clone https://github.com/dukestats/gpustats.git
        git clone https://github.com/andrewcron/dpmix.git
        git clone https://github.com/whitews/FlowIO.git
        git clone https://github.com/whitews/FlowUtils.git
        git clone https://github.com/whitews/FlowStats.git
        git clone https://github.com/whitews/ReFlowRESTClient.git

#.  Install python-pip:

    ``apt-get install python-pip``

#.  Install pycuda first:

    ::
        cd pycuda
        python configure.py
        python setup.py install

#.  Next, install cyrand and cy_armadillo (cyarm) using setup.py:

    ``python setup.py install``

#. Install the rest of the libraries using setup.py in the following order

   * gpustats
   * dpmix
   * FlowIO
   * FlowUtils
   * FlowStats
   * ReFlowRESTClient

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

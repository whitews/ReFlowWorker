Setting Up a Worker on an Ubuntu 12.04 LTS system
====

#.  Clone the gpustats, dpmix, FlowIO, FlowUtils, FlowStats, and ReFlowRESTClient repos:

    ::

        git clone https://github.com/dukestats/gpustats.git
        git clone https://github.com/andrewcron/dpmix.git
        git clone https://github.com/whitews/FlowIO.git
        git clone https://github.com/whitews/FlowUtils.git
        git clone https://github.com/whitews/FlowStats.git
        git clone https://github.com/whitews/ReFlowRESTClient.git

#.  Make sure you have sudo privileges to install Python packages and install
    all the above. Change to each of the directories and install using setup.py:

    ``python setup.py install``

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


#.  As root, from the ``ReFlowRESTClient/reflowrestclient/processing`` directory, start the worker:

    ``python worker.py start``
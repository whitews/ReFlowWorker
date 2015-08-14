import os
from cStringIO import StringIO
import hashlib

from reflowrestclient import utils
import numpy as np
import flowio
import flowutils

from processing_error import ProcessingError

# NOTE: Don't attempt to log or catch exceptions here, ProcessRequest will
#       handle any exceptions thrown. Exceptions will be raised in cases
#       where processing should not continue.


class Sample(object):
    """
    Represents the FCS sample downloaded from a ReFlow server.
    Used by a Worker to manage downloaded samples related to a
    ReFlow ProcessRequest
    """
    def __init__(self, process_request, sample_dict, compensation):
        """
        host: the ReFlow host from which the sample originated
        sample_dict: the ReFlow 'data' dictionary

        Raises KeyError if sample_dict is incomplete
        """
        self.host = process_request.host
        self.process_request = process_request
        self.sample_id = sample_dict['id']
        self.compensation = compensation

        # there are 3 files used for analysis, we store their paths:
        #   fcs_path:
        #       downloaded FCS file
        #   preprocessed_path:
        #       NumPy array of preprocessed events including all channels.
        #       Pre-processed generally means compensated, transformed, &
        #       sub-sampled
        #   normalized_path:
        #       NumPy array of preprocessed events with only the columns
        #       corresponding to the parameters requested for analysis. Also,
        #       the column order has been "normalized" meaning the columns have
        #       been rearranged so all columns are the same across all samples
        #       in the process request.
        self.fcs_path = None
        self.preprocessed_path = None
        self.normalized_path = None

        self.event_count = None  # total event count

        # Save sub-sampled indices for the clustering output
        self.subsample_indices = None

        self.acquisition_date = sample_dict['acquisition_date']
        self.original_filename = sample_dict['original_filename']
        self.sha1 = sample_dict['sha1']

        self.exclude = sample_dict['exclude']

        self.site_id = sample_dict['site']
        self.site_name = sample_dict['site_name']

        self.cytometer_id = sample_dict['cytometer']

        self.specimen_id = sample_dict['specimen']
        self.specimen_name = sample_dict['specimen_name']

        self.stimulation_id = sample_dict['stimulation']
        self.stimulation_name = sample_dict['stimulation_name']

        self.storage = sample_dict['storage']
        self.pretreatment = sample_dict['pretreatment']

        self.visit_id = sample_dict['visit']
        self.visit_name = sample_dict['visit_name']

        self.subject_id = sample_dict['subject']
        self.subject_code = sample_dict['subject_code']

        self.site_panel_id = sample_dict['site_panel']

    def _validate_sample_hash(self, file_path):
        sample_file = open(file_path)
        sha1_hash = hashlib.sha1(sample_file.read())
        sample_file.close()

        if sha1_hash.hexdigest() == self.sha1:
            return True

        return False

    def download_fcs(self, token, download_dir):
        """
        Updates self.fcs_path with location of downloaded FCS file.
        ReFlow Worker sample downloads are kept in CACHE_DIR
        organized by host, then sample id
        """
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        fcs_path = download_dir + str(self.sample_id) + '.fcs'

        # Validate sample's identity via SHA1 hash
        if os.path.exists(fcs_path):
            is_valid = self._validate_sample_hash(fcs_path)
        else:
            is_valid = False

        if not is_valid:
            # Either the file wasn't cached or it failed SHA1 validation.
            # If the file was invalid, try downloading again and re-run
            # validation check
            utils.download_sample(
                self.host,
                token,
                sample_pk=self.sample_id,
                data_format='fcs',
                directory=download_dir,
                method=self.process_request.method
            )

            if not self._validate_sample_hash(fcs_path):
                raise ValueError(
                    "Sample PK %s failed to validate using SHA1"
                )

        self.fcs_path = fcs_path

        # open fcs file to save event count
        flow_obj = flowio.FlowData(self.fcs_path)
        self.event_count = flow_obj.event_count

    def get_all_events(self):
        """
        Returns NumPy array if all events in FCS file
        """
        # open fcs file & convert events to NumPy array
        flow_obj = flowio.FlowData(self.fcs_path)
        numpy_data = np.reshape(
            flow_obj.events,
            (-1, flow_obj.channel_count)
        )

        return numpy_data

    def generate_subsample(self, subsample_count, random_seed):
        """
        Sub-samples FCS sample

        Returns NumPy array if sub-sampling succeeds
        Also updates self.subsample_indices
        """

        # Before sub-sampling we need to filter out events with
        # negative scatter values. To do that we need the parameter
        # annotations
        params = self.process_request.panels[self.site_panel_id]['parameters']
        scatter_indices = []
        for p in params:
            if p['parameter_type'] in ['FSC', 'SSC']:
                scatter_indices.append(p['fcs_number'] - 1)

        numpy_data = self.get_all_events()

        is_neg = numpy_data[:, scatter_indices] < 0
        is_neg = np.where(is_neg.any(True))[0]

        if self.event_count - len(is_neg) < subsample_count:
            raise ProcessingError(
                "Sample %s has fewer events than the subsample count" %
                self.sample_id
            )

        # generate random indices for subsample
        # using a new RandomState with given seed
        shuffled_indices = np.arange(self.event_count)
        shuffled_indices = np.delete(shuffled_indices, is_neg)
        rng = np.random.RandomState()
        rng.seed(random_seed)
        rng.shuffle(shuffled_indices)

        # save indices
        self.subsample_indices = shuffled_indices[:subsample_count]

        # sub-sample FCS events using given indices
        subsample = numpy_data[self.subsample_indices]

        return subsample

    def compensate_events(self, events):
        """
        Gets compensation matrix and applies it to the given events

        Returns NumPy array of compensated events if successful
        """
        # self.compensate has headers for the channel numbers, but
        # flowutils compensate() takes the plain matrix and indices as
        # separate arguments
        # (also note channel #'s vs indices)
        indices = self.compensation[0, :] - 1  # headers are channel #'s
        indices = [int(i) for i in indices]
        comp_matrix = self.compensation[1:, :]  # just the matrix
        comp_data = flowutils.compensate.compensate(
            events,
            comp_matrix,
            indices
        )

        return comp_data

    def apply_logicle_transform(self, data, logicle_t, logicle_w):
        """
        Applies logicle transform to given data

        Returns NumPy array containing transformed data
        """
        # don't transform scatter, time, or null channels
        panel = self.process_request.panels[self.site_panel_id]
        indices = list()
        for param in panel['parameters']:
            if param['parameter_type'] in ['FSC', 'SSC', 'TIM', 'NUL']:
                continue
            else:
                indices.append(param['fcs_number'] - 1)

        x_data = flowutils.transforms.logicle(
            data,
            indices,
            t=logicle_t,
            w=logicle_w
        )

        return x_data

    def apply_asinh_transform(self, data, pre_scale=0.003):
        """
        Applies inverse hyperbolic sine transform on given data

        By default, the compensated data will be transformed and the default
        pre-scale factor is 0.003

        Returns NumPy array containing transformed data
        """
        # don't transform scatter, time, or null channels
        panel = self.process_request.panels[self.site_panel_id]
        indices = list()
        for param in panel['parameters']:
            if param['parameter_type'] in ['FSC', 'SSC', 'TIM', 'NUL']:
                continue
            else:
                indices.append(param['fcs_number'] - 1)

        x_data = flowutils.transforms.asinh(
            data,
            indices,
            pre_scale=pre_scale
        )

        return x_data

    def create_preprocessed(self, data, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

        self.preprocessed_path = "%s/pre_%s.npy" % (
            directory,
            str(self.sample_id)
        )

        np.save(self.preprocessed_path, data)

    def create_normalized(self, data, directory, channel_map):
        if not os.path.exists(directory):
            os.makedirs(directory)

        norm_data = data[:, channel_map]

        self.normalized_path = "%s/norm_%s.npy" % (
            directory,
            str(self.sample_id)
        )

        np.save(self.normalized_path, norm_data)


class Cluster(object):
    """
    All processing pipelines must return a list of Cluster instances
    that the parent ProcessRequest will POST to the ReFlow server
    """
    def __init__(self, cluster_index):
        self.index = cluster_index

        # will store the primary key from the ReFlow server after
        # successful POST
        self.reflow_pk = None

        # a list of fresh SampleCluster instances
        # each SampleCluster "assigns" a different Sample PK to this cluster
        self.sample_clusters = list()

    def add_sample_cluster(self, sample_cluster):
        self.sample_clusters.append(sample_cluster)

    def post(self, host, token, method, process_request_id):
        response = utils.post_cluster(
            host,
            token,
            process_request_id,
            self.index,
            method=method
        )

        if response['status'] == 201:
            self.reflow_pk = response['data']['id']
        else:
            raise ValueError(
                "POST failed: cluster index %s" % str(self.index)
            )


class SampleCluster(object):
    """
    A SampleCluster ties a collection of sample events to a particular cluster.
    Each sample can have an independent location for the parent cluster.
    These locations are stored in SampleClusterParameter instances.
    """
    def __init__(self, sample_id, parameters, events, components):
        self.sample_id = sample_id
        self.parameters = parameters
        self.events = events
        self.components = components

    def post(self, host, token, method, cluster_id):
        param_dict = dict()

        for p in self.parameters:
            param_dict[p.channel] = p.location

        component_list = list()

        for comp in self.components:
            comp_dict = dict()

            # convert covariance to string
            covariance = StringIO()
            np.savetxt(covariance, comp.covariance, fmt="%.6f", delimiter=',')

            # convert component parameter locations from class instance to dict
            comp_param_dict = dict()
            for cp in comp.parameters:
                comp_param_dict[cp.channel] = cp.location

            # assemble component dictionary
            comp_dict['index'] = comp.index
            comp_dict['weight'] = comp.weight
            comp_dict['covariance'] = covariance.getvalue()
            comp_dict['parameters'] = comp_param_dict

            # add component to list
            component_list.append(comp_dict)

        response = utils.post_sample_cluster(
            host,
            token,
            cluster_id,
            self.sample_id,
            param_dict,
            self.events,
            component_list,
            method=method
        )

        if response['status'] != 201:
            raise ValueError(
                "POST failed: received %s status" % str(
                    response['status']
                )
            )


class SampleClusterParameter(object):
    """
    Holds the cluster location for a particular Sample channel
    """
    def __init__(self, channel_number, location):
        self.channel = channel_number
        self.location = location


class SampleClusterComponent(object):
    """
    Since a SampleCluster can be considered a mode comprised of one or more
    components. Each component is a gaussian distribution with its own
    location, weight, and covariance. The components are mainly used
    for re-classification of events in 2nd stage processing
    """
    def __init__(self, index, weight, covariance, parameters):
        # index might be a numpy int that doesn't always play nice with JSON
        self.index = int(index)
        self.weight = weight
        self.covariance = covariance
        self.parameters = parameters


class SampleClusterComponentParameter(object):
    """
    Holds the component location for a particular Sample channel
    """
    def __init__(self, channel_number, location):
        self.channel = channel_number
        self.location = location

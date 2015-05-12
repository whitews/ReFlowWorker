import os
from cStringIO import StringIO

from reflowrestclient import utils
import numpy as np
import flowio
import flowutils


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

        self.fcs_path = None  # path to downloaded FCS file
        self.event_count = None  # total event count
        self.subsample_path = None  # path to subsampled Numpy array

        # need to save sub-sampled indices for the clustering output
        # if sample is using full FCS data, the indices aren't needed
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

    def download_fcs(self, token, download_dir):
        """
        ReFlow Worker sample downloads are kept in BASE_DIR
        organized by host, then sample id

        Returns True if download succeeded or file is already present
        Also updates self.fcs_path
        """
        if not self.host or not self.sample_id:
            return False

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        fcs_path = download_dir + str(self.sample_id) + '.fcs'

        # TODO: if FCS file exists validate its identity using sha1 hash

        if not os.path.exists(fcs_path):
            try:
                utils.download_sample(
                    self.host,
                    token,
                    sample_pk=self.sample_id,
                    data_format='fcs',
                    directory=download_dir,
                    method=self.process_request.method
                )
            except Exception, e:
                print e
                return False

        self.fcs_path = fcs_path

        # open fcs file to save event count
        flow_obj = flowio.FlowData(self.fcs_path)
        self.event_count = flow_obj.event_count

        return True

    def generate_subsample(self, subsample_count):
        """
        Sub-samples FCS sample

        Returns NumPy array if sub-sampling succeeds
        Also updates self.subsample_indices
        """
        if not self.sample_id:
            return None

        if not (self.fcs_path and os.path.exists(self.fcs_path)):
            return None

        # open fcs file
        flow_obj = flowio.FlowData(self.fcs_path)

        # generate random indices for subsample & save indices
        shuffled_indices = np.arange(self.event_count)
        np.random.shuffle(shuffled_indices)
        self.subsample_indices = shuffled_indices[:subsample_count]

        # sub-sample FCS events using given indices
        # create new 1st column containing the event indices of the
        # sub-sampled events
        numpy_data = np.reshape(
            flow_obj.events,
            (-1, flow_obj.channel_count)
        )
        indexed_subsample = numpy_data[self.subsample_indices]

        return indexed_subsample

    def compensate_events(self, events):
        """
        Gets compensation matrix and applies it to the given events

        Returns NumPy array of compensated events if successful
        """
        if not self.sample_id:
            return None

        if not len(self.compensation) > 0:
            return None

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
        if not self.sample_id:
            return False

        # don't transform scatter, time, or null channels
        panel = self.process_request.panels[self.site_panel_id]
        if 'parameters' not in panel:
            return False
        indices = list()
        for param in panel['parameters']:
            if 'parameter_value_type' not in param:
                return False
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

        if not self.sample_id:
            return False

        # don't transform scatter, time, or null channels
        panel = self.process_request.panels[self.site_panel_id]
        if 'parameters' not in panel:
            return False
        indices = list()
        for param in panel['parameters']:
            if 'parameter_value_type' not in param:
                return False
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

    def create_normalized(self, data, channel_map):
        return data[:, channel_map]


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

        if 'status' not in response:
            return False
        if response['status'] == 201:
            self.reflow_pk = response['data']['id']
            return True

        return False


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
            np.savetxt(covariance, comp.covariance, fmt="%d", delimiter=',')

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

        if 'status' not in response:
            return False
        if response['status'] == 201:
            return True

        return False


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
        self.index = index
        self.weight = weight
        self.covariance = covariance
        self.parameters = parameters

    def post(self, host, token, method, sample_cluster_id):
        param_dict = dict()

        for p in self.parameters:
            param_dict[p.channel] = p.location

        response = utils.post_sample_cluster_component(
            host,
            token,
            sample_cluster_id,
            self.weight,
            self.covariance,
            param_dict,
            method=method
        )

        if 'status' not in response:
            return False
        if response['status'] == 201:
            return True

        return False


class SampleClusterComponentParameter(object):
    """
    Holds the component location for a particular Sample channel
    """
    def __init__(self, channel_number, location):
        self.channel = channel_number
        self.location = location
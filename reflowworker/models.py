import os
import re

from reflowrestclient import utils
import numpy
import flowutils

BASE_DIR = '/var/tmp/ReFlow-data/'


class ProcessRequest(object):
    """
    A process request from a ReFlow server.
    Contains the list of Samples and a list of process input key/values
    """

    REQUIRED_CATEGORIES = ['transformation', 'clustering']

    def __init__(self, host, token, pr_dict, method):
        self.host = host
        self.token = token
        self.method = method  # 'http://' or 'https://'
        self.process_request_id = pr_dict['id']
        self.parent_stage = pr_dict['parent_stage']
        self.sample_collection_id = pr_dict['sample_collection']
        self.subsample_count = pr_dict['subsample_count']
        self.directory = "%s%s/process_requests/%s" % (
            BASE_DIR,
            self.host,
            self.process_request_id)
        self.inputs = pr_dict['inputs']
        self.use_fcs = False
        self.samples = list()
        self.panels = dict()
        self.transformation = None
        self.transformation_options = {}
        self.clustering = None
        self.clustering_options = {}

        # the results of the processing pipeline will be stored as
        # a list of Cluster instances
        self.clusters = list()

        # the param_list will be the normalized order of parameters
        self.param_list = list()

        # panel_maps will hold the re-ordering of each site panel parameter
        # keys will be site panel PK, and values will be a list of indices...
        self.panel_maps = dict()

        self.results_directory = self.directory + "/results"

        if not os.path.exists(self.results_directory):
            os.makedirs(self.results_directory)

        # lookup the sample collection
        response = utils.get_sample_collection(
            self.host,
            self.token,
            sample_collection_pk=self.sample_collection_id,
            method=self.method
        )
        if 'data' not in response:
            return

        for member in response['data']['members']:
            compensation = self.convert_matrix(member['compensation'])
            sample = Sample(self, member['sample'], compensation)

            self.samples.append(sample)
            if sample.site_panel_id not in self.panels:
                panel_response = utils.get_site_panel(
                    self.host,
                    self.token,
                    sample.site_panel_id,
                    method=self.method
                )
                if 'data' not in response:
                    continue
                self.panels[sample.site_panel_id] = panel_response['data']

    @staticmethod
    def convert_matrix(compensation_string):
        """
        Converts the comma delimited text string returned within
        ReFlow's SampleCollection to a numpy array that the Sample
        constructor needs
        """
        lines = compensation_string.splitlines(False)
        headers = re.split(',', lines[0])
        headers = [int(h) for h in headers]

        # create numpy array and add headers
        np_array = numpy.array(headers)

        # now add the matrix data
        for line in lines[1:]:
            line_values = re.split(',', line)
            for i, value in enumerate(line_values):
                line_values[i] = float(line_values[i])
            np_array = numpy.vstack([np_array, line_values])

        return np_array

    def validate_inputs(self):
        # iterate through the inputs to validate:
        #     - all the required categories are present
        #     - there are no mixed implementations
        #     - the input values are the correct type

        for pr_input in self.inputs:
            if pr_input['category_name'] == 'transformation':
                if not self.transformation:
                    self.transformation = pr_input['implementation_name']
                elif self.transformation != pr_input['implementation_name']:
                    # mixed implementations aren't allowed
                    return False
                self.transformation_options[pr_input['input_name']] = pr_input['value']
            elif pr_input['category_name'] == 'clustering':
                if not self.clustering:
                    self.clustering = pr_input['implementation_name']
                elif self.clustering != pr_input['implementation_name']:
                    # mixed implementations aren't allowed
                    return False
                self.clustering_options[pr_input['input_name']] = pr_input['value']

                # TODO: validate value against value_type

        if not self.clustering:
            # clustering category is required, but missing
            return False

        if not self.transformation:
            self.transformation = 'asinh'  # default xform is asinh

        return True

    def download_samples(self):
        if self.use_fcs:
            for s in self.samples:
                s.download_fcs(self.token)
        else:
            for s in self.samples:
                s.download_subsample(self.token)

    def compensate_samples(self):
        directory = self.directory + '/preprocessed/comp'
        if self.use_fcs:
            for s in self.samples:
                # TODO: implement compensate_fcs
                s.compensate_fcs(self.token, directory)
        else:
            for s in self.samples:
                s.compensate_subsample(directory)

    def apply_logicle_transform(self):
        directory = self.directory + '/preprocessed/transformed'
        for s in self.samples:
            s.apply_logicle_transform(
                directory,
                int(self.transformation_options['t']),
                float(self.transformation_options['w']))

    def apply_asinh_transform(self):
        """
        Inverse hyperbolic sine transformation with a pre-scale factor
        for optimum visualization (and similarity to the logicle transform)
        """
        directory = self.directory + '/preprocessed/transformed'
        for s in self.samples:
            s.apply_asinh_transform(directory)

    def normalize_transformed_samples(self):
        directory = self.directory + '/preprocessed/normalized'

        request_params = []
        for pr_input in self.inputs:
            if pr_input['category_name'] == 'filtering':
                if pr_input['implementation_name'] == 'parameters':
                    if pr_input['input_name'] == 'parameter':
                        request_params.append(pr_input['value'])

        # get distinct list of parameters common to all panels in this PR
        param_set = set()
        for panel in self.panels:
            for param in self.panels[panel]['parameters']:
                param_type = param['parameter_type']

                if param_type in ['TIM', 'NUL']:
                    continue

                value_type = param['parameter_value_type']

                param_str = "_".join([param_type, value_type])

                markers = list()
                for marker in param['markers']:
                    markers.append(marker['name'])

                if len(markers) > 0:
                    markers.sort()
                    markers = "_".join(markers)
                    param_str = "_".join([param_str, markers])

                if param['fluorochrome']:
                    fluoro = param['fluorochrome']['fluorochrome_abbreviation']
                    param_str = "_".join([param_str, fluoro])

                # limit the set to the requested params filter
                if param_str in request_params:
                    param_set.add(param_str)

                param['full_name'] = param_str

        # the param_list will be the normalized order of parameters
        self.param_list = list(param_set)
        self.param_list.sort()

        # panel_maps will hold the re-ordering of each site panel parameter
        # keys will be site panel PK, and values will be a list of indices...
        self.panel_maps = dict()

        for panel in self.panels:
            self.panel_maps[panel] = list()
            # first, iterate through the param_list so the order is correct
            for p in self.param_list:
                for param in self.panels[panel]['parameters']:
                    if 'full_name' in param:
                        if param['full_name'] == p:
                            self.panel_maps[panel].append(
                                param['fcs_number'] - 1)

        for s in self.samples:
            s.create_normalized(directory, self.panel_maps[s.site_panel_id])

    def set_clusters(self, clusters):
        self.clusters = clusters

    def post_clusters(self):
        """
        POST all clusters and sample clusters (with event classifications)
        to the ReFlow server. This should only be called after local
        processing has finished.
        """
        # first post the Cluster instances to get their ReFlow PKs
        for c in self.clusters:
            # POST only if it has no PK
            if not c.reflow_pk:
                # after trying to POST, if no PK we return False
                if not c.post(
                        self.host,
                        self.token,
                        self.method,
                        self.process_request_id
                ):
                    return False

            # now save all the sample clusters
            for sc in c.sample_clusters:
                if not sc.post(self.host, self.token, self.method, c.reflow_pk):
                    return False

        return True


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
        self.subsample_path = None  # path to subsampled Numpy array
        self.compensated_path = None  # path to comp'd data (numpy)
        self.transformed_path = None  # path to transformed data (numpy)
        self.normalized_path = None  # path to normalized data (numpy)

        # need to save sub-sampled indices for the clustering output
        # if sample is using full FCS data, the indices aren't needed
        self.is_subsampled = False  # TODO: no longer needed, everything is subsampled even if it's all the events
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

    def download_subsample(self, token):
        """
        ReFlow Worker sample downloads are kept in BASE_DIR
        organized by host, then sample id

        Returns True if download succeeded or file is already present
        Also updates self.subsample_path
        """
        if not self.host or not self.sample_id:
            return False

        download_dir = BASE_DIR + str(self.host) + '/'
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        subsample_path = download_dir + str(self.sample_id) + '.npy'

        if not os.path.exists(subsample_path):
            try:
                utils.download_sample(
                    self.host,
                    token,
                    sample_pk=self.sample_id,
                    data_format='npy',
                    directory=download_dir,
                    method=self.process_request.method
                )
            except Exception, e:
                print e
                return False

        self.subsample_path = subsample_path
        self.is_subsampled = True

        # get the sub-sampled indices
        data = numpy.load(self.subsample_path)

        # note the 1st data column are event indices
        self.event_indices = data[:, 0]

        return True

    def download_fcs(self, token):
        """
        ReFlow Worker sample downloads are kept in BASE_DIR
        organized by host, then sample id

        Returns True if download succeeded or file is already present
        Also updates self.subsample_path
        """
        if not self.host or not self.sample_id:
            return False

        download_dir = BASE_DIR + str(self.host) + '/'
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        fcs_path = download_dir + str(self.sample_id) + '.fcs'

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
        return True

    def compensate_subsample(self, directory):
        """
        Gets compensation matrix and applies it to the subsample, saving
        compensated data in given directory

        Returns False if the subsample has not been downloaded or
        if the compensation fails or if the directory given doesn't exist
        """
        if not self.host or not self.sample_id:
            return False

        if not (self.subsample_path and os.path.exists(self.subsample_path)):
            return False

        if not os.path.exists(directory):
            os.makedirs(directory)

        if not len(self.compensation) > 0:
            return False

        # self.compensate has headers for the channel numbers, but
        # flowutils compensate() takes the plain matrix and indices as
        # separate arguments
        # (also note channel #'s vs indices)
        data = numpy.load(self.subsample_path)

        # note the 1st data column are event indices, we'll throw these away
        data = data[:, 1:]
        indices = self.compensation[0, :] - 1  # headers are channel #'s
        indices = [int(i) for i in indices]
        comp_matrix = self.compensation[1:, :]  # just the matrix
        comp_data = flowutils.compensate.compensate(
            data,
            comp_matrix,
            indices
        )

        data[:, indices] = comp_data

        # we name these as compd_<id>.npy to differentiate between the
        # comp matrix files which use comp_<id>.npy even though they are
        # in a different directory...in case anyone ever moves stuff around
        # things won't clobber each other
        compensated_path = "%s/compd_%s.npy" % (
            directory,
            str(self.sample_id)
        )

        numpy.save(compensated_path, data)

        self.compensated_path = compensated_path

        return True

    def apply_logicle_transform(
            self,
            directory,
            logicle_t,
            logicle_w,
            use_comp=True):
        """
        Transforms sample data

        By default, the compensated data will be transformed and the default
        transform is 'logicle'

        Returns False if the transformation fails or if the directory given
        cannot be created
        """
        if not self.host or not self.sample_id:
            return False

        if use_comp:
            if not (
                    self.compensated_path
                    and
                    os.path.exists(self.compensated_path)):
                return False
            else:
                data = numpy.load(self.compensated_path)
        else:
            if not (
                    self.subsample_path
                    and
                    os.path.exists(self.subsample_path)):
                return False
            else:
                data = numpy.load(self.subsample_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

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

        transformed_path = "%s/logicle_%s.npy" % (
            directory,
            str(self.sample_id)
        )
        numpy.save(transformed_path, x_data)

        self.transformed_path = transformed_path

        return True

    def apply_asinh_transform(self, directory, pre_scale=0.01, use_comp=True):
        """
        Transforms sample data

        By default, the compensated data will be transformed and the default
        pre-scale factor is 1/100

        Returns False if the transformation fails or if the directory given
        cannot be created
        """

        if not self.host or not self.sample_id:
            return False

        if use_comp:
            if not (self.compensated_path and os.path.exists(self.compensated_path)):
                return False
            else:
                data = numpy.load(self.compensated_path)
        else:
            if not (self.subsample_path and os.path.exists(self.subsample_path)):
                return False
            else:
                data = numpy.load(self.subsample_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

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

        transformed_path = "%s/asinh_%s.npy" % (
            directory,
            str(self.sample_id)
        )
        numpy.save(transformed_path, x_data)

        self.transformed_path = transformed_path

        return True

    def create_normalized(self, directory, channel_map):
        if not os.path.exists(directory):
            os.makedirs(directory)

        if self.transformed_path:
            data = numpy.load(self.transformed_path)
        elif self.compensated_path:
            data = numpy.load(self.compensated_path)
        else:
            data = numpy.load(self.subsample_path)

        norm_data = data[:, channel_map]

        normalized_path = "%s/norm_%s.npy" % (
            directory,
            str(self.sample_id)
        )

        numpy.save(normalized_path, norm_data)

        self.normalized_path = normalized_path


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
    def __init__(self, sample_id, parameters, event_indices):
        self.sample_id = sample_id
        self.parameters = parameters
        self.event_indices = [int(x) for x in event_indices]

    def post(self, host, token, method, cluster_id):
        param_dict = dict()

        for p in self.parameters:
            param_dict[p.channel] = p.location

        response = utils.post_sample_cluster(
            host,
            token,
            cluster_id,
            self.sample_id,
            param_dict,
            self.event_indices,
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
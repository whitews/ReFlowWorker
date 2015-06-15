from settings import CACHE_DIR
from sample_models import Sample
from clustering_processes import hdp

# NOTE: We import logger here for logging info and for more granular
#       errors useful for troubleshooting. All exceptions should be caught
#       and reported to the ReFlow server by the WorkerProcess.
from logger import logger
from processing_error import ProcessingError

import re
import numpy as np
from reflowrestclient import utils


class ProcessRequest(object):
    """
    A process request from a ReFlow server.
    Stores the list of Sample instances and  process input key/values, along
    with panel mapping information. Nearly all the data munging and
    processing logic occurs within ProcessRequest, except for the
    implementation of the clustering algorithm itself.

    NOTE: ProcessRequest tries to log useful information for monitoring and
          troubleshooting. Any errors are logged as-is, but a more useful
          ProcessingError is re-raised so the WorkerProcess can report them
          back to the ReFlow server.
    """
    def __init__(self, host, token, pr_dict, method):
        self.host = host
        self.token = token
        self.method = method  # 'http://' or 'https://'
        self.process_request_id = pr_dict['id']

        # 2nd stage processing stuff
        self.parent_stage = pr_dict['parent_stage']
        # stores the PK of the parent clusters to use for enrichment
        self.parent_clusters = []
        for c in pr_dict['stage2_clusters']:
            self.parent_clusters.append(c['cluster'])

        self.random_seed = None
        self.sample_collection_id = pr_dict['sample_collection']
        self.subsample_count = pr_dict['subsample_count']
        self.directory = "%s%s/process_requests/%s" % (
            CACHE_DIR,
            self.host,
            self.process_request_id)
        self.inputs = pr_dict['inputs']
        self.samples = list()
        self.panels = dict()
        self.transformation = None
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

        # lookup the sample collection
        response = utils.get_sample_collection(
            self.host,
            self.token,
            sample_collection_pk=self.sample_collection_id,
            method=self.method
        )

        logger.info(
            "(PR: %s) GET SampleCollection %s succeeded",
            str(self.process_request_id),
            str(self.sample_collection_id)
        )

        # populate self.samples w/Sample instances w/compensation
        for member in response['data']['members']:
            try:
                compensation = self.convert_matrix(member['compensation'])
            except Exception as e:
                logger.error(str(e), exc_info=True)
                raise ProcessingError("Error parsing compensation")
            try:
                sample = Sample(self, member['sample'], compensation)
            except Exception as e:
                logger.error(str(e), exc_info=True)
                raise ProcessingError("Error retrieving samples")

            self.samples.append(sample)
            if sample.site_panel_id not in self.panels:
                try:
                    panel_response = utils.get_site_panel(
                        self.host,
                        self.token,
                        sample.site_panel_id,
                        method=self.method
                    )
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    raise ProcessingError("Error retrieving sample annotation")
                self.panels[sample.site_panel_id] = panel_response['data']

        logger.info(
            "(PR: %s) All Sample instances were created successfully",
            str(self.process_request_id)
        )

    @staticmethod
    def convert_matrix(compensation_string):
        """
        Converts the comma delimited text string returned from
        ReFlow to a numpy array that the Sample
        constructor needs
        """
        lines = compensation_string.splitlines(False)
        headers = re.split(',', lines[0])
        headers = [int(h) for h in headers]

        # create numpy array and add headers
        np_array = np.array(headers)

        # now add the matrix data
        for line in lines[1:]:
            line_values = re.split(',', line)
            for i, value in enumerate(line_values):
                line_values[i] = float(line_values[i])
            np_array = np.vstack([np_array, line_values])

        return np_array

    def _validate_inputs(self):
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

        self.random_seed = int(self.clustering_options['random_seed'])

        if not self.transformation:
            self.transformation = 'asinh'  # default transform is asinh

        # Finally, we need to generate the panel maps used for "normalizing"
        # the samples
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
                                param['fcs_number'] - 1
                            )

        return True

    def _download_samples(self):
        download_dir = CACHE_DIR + str(self.host) + '/'
        for s in self.samples:
            s.download_fcs(self.token, download_dir)

    def _pre_process(self):
        if self.parent_stage is None:
            # we've got a simple 1st stage PR
            for s in self.samples:
                # Sub-sample events
                subsample = s.generate_subsample(self.subsample_count)

                # Compensate the sub-sampled events
                comped_sub = s.compensate_events(subsample)

                # Apply specified transform to compensated events
                if self.transformation == 'logicle':
                    xform = s.apply_logicle_transform(comped_sub)
                elif self.transformation == 'asinh':
                    xform = s.apply_asinh_transform(comped_sub)

                # save xform as pre-processed data
                s.create_preprocessed(xform, self.directory + '/preprocessed')

                # next is normalization of common sample parameters
                s.create_normalized(
                    xform,
                    self.directory + '/normalized',
                    self.panel_maps[s.site_panel_id]
                )
        else:
            # we've got a 2nd stage PR, so much more pre-processing to do...
            # Since only a few of the 1st stage clusters were selected
            # for analysis, thus we use the original model from stage 1 to
            # classify all events in order to enrich a new sub-sample with
            # just events from those selected clusters

            for s in self.samples:
                # get all events
                data = s.get_all_events()

                # compensate all events
                data = s.compensate_events(data)

                # transform all events
                if self.transformation == 'logicle':
                    data = s.apply_logicle_transform(data)
                elif self.transformation == 'asinh':
                    data = s.apply_asinh_transform(data)

                # Retrieve this sample's components from parent stage
                components = utils.get_sample_cluster_components(
                    self.host,
                    self.token,
                    process_request_pk=self.parent_stage,
                    sample_pk=s.sample_id,
                    method=self.method
                )

                # create the DPCluster instances & save a map of the
                # components that belong to the specified clusters from stage 1
                # NOTE: we import this here to avoid a PyCUDA issue when
                # starting up the daemonize procedure
                from flowstats.dp_cluster import DPCluster, DPMixture
                dp_clusters = []
                enrich_components = []
                for comp_idx, comp in enumerate(components['data']):
                    # determine if this comp was a member of a user-specified
                    # cluster to include for analysis
                    if comp['cluster'] in self.parent_clusters:
                        enrich_components.append(comp_idx)

                    # use the channel order from the covariance matrix to
                    # avoid re-arranging the covariance matrix
                    # all the covariance matrices in all components will have
                    # been saved in the same order from the 1st stage
                    covariance = []
                    for l in comp['covariance_matrix'].splitlines():
                        covariance.append(
                            [float(n) for n in l.split(',')]
                        )
                    indices = covariance.pop(0)
                    indices = [int(i) for i in indices]
                    covariance = np.array(covariance)

                    locations = []
                    for i in indices:
                        for c in comp['parameters']:
                            if (c['channel'] - 1) == i:
                                locations.append(c['location'])

                    dp_clusters.append(
                        DPCluster(
                            comp['weight'],
                            locations,
                            covariance
                        )
                    )

                dp_mixture = DPMixture(dp_clusters)
                classifications = dp_mixture.classify(data[:, indices])
                enrich_indices = []
                for ec in enrich_components:
                    # note: where returns a tuple where first item is a
                    # numpy array containing the indices...probably does this
                    # for compatibility with numpy fancy indexing
                    enrich_indices.extend(
                        np.where(classifications == ec)[0]
                    )

                # shuffle the enriched indices and draw our subsample
                # saving the chosen indices for the sample
                np.random.shuffle(enrich_indices)
                s.subsample_indices = enrich_indices[:self.subsample_count]

                # save subsample as pre-processed data
                s.create_preprocessed(
                    data[s.subsample_indices], self.directory + '/preprocessed'
                )

                # next is normalization of common sample parameters
                s.create_normalized(
                    data[s.subsample_indices],
                    self.directory + '/normalized',
                    self.panel_maps[s.site_panel_id]
                )
        return True

    def analyze(self, device):
        # First, validate the inputs
        if not self._validate_inputs():
            raise ProcessingError("Invalid process request inputs")

        # Seed the RNG
        np.random.seed(self.random_seed)

        # Download the samples
        try:
            self._download_samples()
        except Exception as e:
            logger.error(str(e), exc_info=True)
            raise ProcessingError("Downloading samples failed")

        # Pre-process data...takes care of various combinations of tasks
        # Afterward, all samples' subsampled data files will be available &
        # ready for analysis
        if not self._pre_process():
            raise ProcessingError("Error occurred during pre-processing")

        # next is clustering
        if self.clustering == 'hdp':
            try:
                self.clusters = hdp(self, device)
            except Exception as e:
                logger.error(str(e), exc_info=True)
                raise ProcessingError("HDP clustering failed")
        else:
            # only HDP is implemented at this time
            raise ProcessingError("Unsupported clustering type")

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
                try:
                    c.post(
                        self.host,
                        self.token,
                        self.method,
                        self.process_request_id
                    )
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    raise ProcessingError("Cluster POST failed")

            # now save all the sample clusters
            for sc in c.sample_clusters:
                try:
                    sc.post(
                        self.host,
                        self.token,
                        self.method,
                        c.reflow_pk
                    )
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    raise ProcessingError("SampleCluster POST failed")

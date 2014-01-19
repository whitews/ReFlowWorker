import os

from reflowrestclient import utils
import numpy
import flowutils

BASE_DIR = '/var/tmp/ReFlow-data/'


class ProcessRequest(object):
    """
    A process request from a ReFlow server.
    Contains the list of Samples and a list of process input key/values
    """
    def __init__(self, host, token, pr_dict):
        self.host = host
        self.token = token
        self.process_request_id = pr_dict['id']
        self.directory = "%s%s/process_requests/%s" % (
            BASE_DIR,
            self.host,
            self.process_request_id)
        self.process_id = pr_dict['process']
        self.inputs = pr_dict['inputs']
        self.use_fcs = False
        self.samples = list()
        self.panels = dict()
        self.required_inputs = None  # will be populated with dict in worker

        # the param_list will be the normalized order of parameters
        self.param_list = list()

        # panel_maps will hold the re-ordering of each site panel parameter
        # keys will be site panel PK, and values will be a list of indices...
        self.panel_maps = dict()

        self.results_directory = self.directory + "/results"
        self.__setup()

    def __setup(self):
        """
        Essentially populates the list of samples based on the inputs.
        The following keys will be used to filter the samples:
            'project_panel'
            'site' ***
            'site_panel' ***
            'subject' ***
            'visit' ***
            'specimen' ***
            'stimulation' ***
            'storage' ***
            'cytometer' ***
            'acquisition_date' ***
        Note that these keys(***) can appear more than once, i.e. more than one
        site panel.
        """

        if not os.path.exists(self.results_directory):
            os.makedirs(self.results_directory)

        project_panel = None
        sites = list()
        site_panels = list()
        subjects = list()
        visits = list()
        specimens = list()
        storages = list()
        pretreatments = list()
        stimulations = list()
        cytometers = list()
        acquisition_dates = list()

        for i in self.inputs:
            if i['key'] == 'project_panel' and not project_panel:
                project_panel = i['value']
            elif i['key'] == 'use_fcs':
                if i['value'] == 'True':
                    self.use_fcs = True
            elif i['key'] == 'site':
                sites.append(i['value'])
            elif i['key'] == 'site_panel':
                site_panels.append(i['value'])
            elif i['key'] == 'subject':
                subjects.append(i['value'])
            elif i['key'] == 'visit':
                visits.append(i['value'])
            elif i['key'] == 'specimen':
                specimens.append(i['value'])
            elif i['key'] == 'storage':
                storages.append(i['value'])
            elif i['key'] == 'pretreatment':
                pretreatments.append(i['value'])
            elif i['key'] == 'stimulation':
                stimulations.append(i['value'])
            elif i['key'] == 'cytometer':
                cytometers.append(i['value'])
            elif i['key'] == 'acquisition_date':
                acquisition_dates.append(i['value'])

        # start with the project panel and get all those samples
        response = utils.get_samples(
            self.host,
            self.token,
            project_panel_pk=project_panel)
        if not 'data' in response:
            return

        for sample_dict in response['data']:
            sample = Sample(self.host, self, sample_dict)
            
            # keep in mind the key/value inputs are strings
            if len(sites) > 0:
                if not str(sample.site_id) in sites:
                    continue
            if len(site_panels) > 0:
                if not str(sample.site_panel_id) in site_panels:
                    continue
            if len(subjects) > 0:
                if not str(sample.subject_id) in subjects:
                    continue
            if len(visits) > 0:
                if not str(sample.visit_id) in visits:
                    continue
            if len(specimens) > 0:
                if not str(sample.specimen_id) in specimens:
                    continue
            if len(storages) > 0:
                if not str(sample.storage) in storages:
                    continue
            if len(pretreatments) > 0:
                if not str(sample.pretreatment) in pretreatments:
                    continue
            if len(stimulations) > 0:
                if not str(sample.stimulation_id) in stimulations:
                    continue
            if len(cytometers) > 0:
                if not str(sample.cytometer_id) in cytometers:
                    continue
            if len(acquisition_dates) > 0:
                if not str(sample.acquisition_date) in acquisition_dates:
                    continue

            self.samples.append(sample)
            if not sample.site_panel_id in self.panels:
                panel_response = utils.get_site_panel(
                    self.host,
                    self.token,
                    sample.site_panel_id)
                if not 'data' in response:
                    continue
                self.panels[sample.site_panel_id] = panel_response['data']

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
                s.conpensate_fcs(self.token, directory)
        else:
            for s in self.samples:
                s.compensate_subsample(self.token, directory)

    def apply_logicle_transform(self, logicle_t, logicle_w):
        directory = self.directory + '/preprocessed/transformed'
        for s in self.samples:
            s.apply_logicle_transform(directory, logicle_t, logicle_w)

    def normalize_transformed_samples(self):
        directory = self.directory + '/preprocessed/normalized'

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


class Sample(object):
    """
    Represents the FCS sample downloaded from a ReFlow server.
    Used by a Worker to manage downloaded samples related to a
    ReFlow ProcessRequest
    """
    def __init__(self, host, process_request, sample_dict):
        """
        host: the ReFlow host from which the sample originated
        sample_dict: the ReFlow 'data' dictionary

        Raises KeyError if sample_dict is incomplete
        """
        self.host = host
        self.process_request = process_request
        self.sample_id = sample_dict['id']

        self.fcs_path = None  # path to downloaded FCS file
        self.subsample_path = None  # path to downloaded Numpy array
        self.compensated_path = None  # path to comp'd data (numpy)
        self.transformed_path = None  # path to transformed data (numpy)
        self.normalized_path = None  # path to normalized data (numpy)

        self.acquisition_date = sample_dict['acquisition_date']
        self.original_filename = sample_dict['original_filename']
        self.sha1 = sample_dict['sha1']

        self.compensation_id = sample_dict['compensation']

        # Comp matrix of self.compensation_id, and if null may come from
        # the FCS $SPILL element
        self.compensation = None

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

        self.project_panel_id = sample_dict['project_panel']
        self.project_panel_name = sample_dict['panel_name']

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
                    directory=download_dir)
            except Exception, e:
                print e
                return False

        self.subsample_path = subsample_path
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
                    directory=download_dir)
            except Exception, e:
                print e
                return False

        self.fcs_path = fcs_path
        return True

    def _parse_fcs_spill(self, token):
        """
        Finds if a Sample has an FCS $SPILL value.
        If found, returns the Numpy array with the sample's channel
        numbers as the headers
        """
        spill_resp = utils.get_sample_metadata(
            self.host,
            token,
            sample_pk=self.sample_id,
            key='spill')

        spill_text = None
        if len(spill_resp['data']) > 0:
            if 'value' in spill_resp['data'][0]:
                spill_text = spill_resp['data'][0]['value']
        if not spill_text:
            return False

        spill = spill_text.split(',')

        n_markers = int(spill.pop(0))  # marker count
        markers = spill[:n_markers]
        matrix = spill[n_markers:]

        if not len(matrix) == n_markers**2:
            print "Wrong number of items in spill matrix"
            return False

        np_matrix = numpy.array([float(i) for i in matrix])
        np_matrix = np_matrix.reshape(n_markers, n_markers)

        # the spill text header uses PnN values to identify
        # the channels, so get the sample's site panel which
        # contains the FCS PnN info
        sp_resp = utils.get_site_panel(
            self.host,
            token,
            self.site_panel_id)
        if not len(sp_resp['data']) > 0:
            print "No site panel was found."
            return False

        # match the spill channels with the site panel, and
        # save channel number in new header list
        new_headers = markers
        for param in sp_resp['data']['parameters']:
            if param['fcs_text'] in markers:
                i = markers.index(param['fcs_text'])
                new_headers[i] = param['fcs_number']

        new_headers = numpy.array(new_headers)
        np_matrix = numpy.insert(np_matrix, 0, new_headers, 0)

        return np_matrix

    def _download_compensation(self, token):
        # Compensation matrices are saved as Numpy arrays in a
        # 'comp' sub-directory, and use the naming convention:
        # 'comp_<id>.npy'
        # A comp with a primary key of 42 will by 'comp_42.py'
        # Note, this is not the sample's primary key
        if not self.host or not self.sample_id or not self.compensation_id:
            return False

        if not self.subsample_path and os.path.exists(self.subsample_path):
            return False

        download_dir = BASE_DIR + str(self.host) + '/comp/'

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # always re-download compensation, it may have changed on the server
        try:
            utils.download_compensation(
                self.host,
                token,
                compensation_pk=self.compensation_id,
                data_format='npy',
                directory=download_dir)
        except Exception, e:
            print e
            return False

        if os.path.exists(download_dir + 'comp_%s.npy' % self.compensation_id):
            return True

        return False

    def _populate_compensation(self, token):
        """
        Determines the compensation matrix and populates
        self.compensation if found.

        Note: Compensation matrix is chosen in the following order:
            1st choice: self.compensation which comes from the Sample's
                        directly related compensation relationship on the
                        ReFlow host (self.host)
            2nd choice: The first matching compensation matrix on the
                        ReFlow host (self.host) which matches both the
                        Sample's site panel and the Sample's acquisition date
            3rd choice: The spillover matrix within the original FCS file

        Returns True if successful
        """

        if not self.host or not self.sample_id:
            return False

        if not self.compensation_id:
            # try to find a matching compensation
            comps = utils.get_compensations(
                self.host,
                token,
                site_panel_pk=self.site_panel_id,
                acquisition_date=self.acquisition_date
            )
            if len(comps['data']) > 0:
                # found a match, set the comp for this sample
                self.compensation_id = comps['data'][0]['id']

        # if there's still no compensation id, try the FCS file's spill
        if not self.compensation_id:
            self.compensation = self._parse_fcs_spill(token)
        else:
            # we have a compensation_id, download it and save
            if self._download_compensation(token):
                try:
                    self.compensation = numpy.load(
                        BASE_DIR + '%s/comp/comp_%s.npy'
                        % (
                            self.host,
                            self.compensation_id
                        )
                    )
                except Exception, e:
                    print e
                    return False
            else:
                return False

        if len(self.compensation) > 0:
            return True

        # No compensation was found
        return False

    def compensate_subsample(self, token, directory):
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

        self._populate_compensation(token)

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
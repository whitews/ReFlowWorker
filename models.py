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
        self.process_id = pr_dict['process']
        self.inputs = pr_dict['inputs']
        self.samples = list()
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

        project_panel = None
        sites = list()
        site_panels = list()
        subjects = list()
        visits = list()
        specimens = list()
        storages = list()
        stimulations = list()
        cytometers = list()
        acquisition_dates = list()

        for i in self.inputs:
            if i['key'] == 'project_panel' and not project_panel:
                project_panel = i['value']
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
            sample = Sample(self.host, self.process_request_id, sample_dict)
            self.samples.append(sample)

    def download_samples(self):
        for s in self.samples:
            s.download_subsample(self.token)


class Sample(object):
    """
    Represents the FCS sample downloaded from a ReFlow server.
    Used by a Worker to manage downloaded samples related to a
    ReFlow ProcessRequest
    """
    def __init__(self, host, process_request_id, sample_dict):
        """
        host: the ReFlow host from which the sample originated
        sample_dict: the ReFlow 'data' dictionary

        Raises KeyError if sample_dict is incomplete
        """
        self.host = host
        self.process_request_id = process_request_id
        self.sample_id = sample_dict['id']

        self.fcs_path = None  # path to downloaded FCS file
        self.fcs_comp_path = None  # path to fcs compensated data (numpy)
        self.subsample_path = None  # path to downloaded Numpy array
        self.subsample_comp_path = None  # path sub-sampled comp'd data (numpy)

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

        self.specimen_id = sample_dict['specimen']
        self.specimen_name = sample_dict['specimen_name']

        self.stimulation_id = sample_dict['stimulation']
        self.stimulation_name = sample_dict['stimulation_name']

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

        if len(spill_resp['data']) > 0:
            if hasattr('spill', spill_resp['data'][0]):
                spill_text = spill_resp['data'][0]['spill']
                spill = spill_text.split(',')

                n_markers = int(spill.pop(0))  # marker count
                markers = spill[:n_markers]
                matrix = spill[n_markers:]

                if not len(matrix) == n_markers**2:
                    print "Wrong number of items in spill matrix"
                    return False

                np_matrix = numpy.array(matrix)
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
                # save in new header list
                for param in sp_resp['data'][0]['parameters']:
                    print param

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
        comp_path = download_dir + 'comp_%s.npy' % self.compensation_id

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # avoid re-downloading the comp matrix if already present
        if os.path.exists(comp_path):
            self.compensation_path = comp_path
            return True

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
                        BASE_DIR + '/comp/comp_%s.npy' % self.compensation_id)
                except Exception, e:
                    print e
                    return False
            else:
                return False

        if self.compensation:
            return True

        # No compensation was found
        return False

    def compensate_subsample(self, token):
        """
        Gets compensation matrix and applies it to the subsample.

        Returns False if the subsample has not been downloaded or
        if the compensation fails
        """
        if not self.host or not self.sample_id:
            return False

        if not (self.subsample_path and os.path.exists(self.subsample_path)):
            return False

        self._populate_compensation(token)

        if not self.compensation:
            return False

        # self.compensate has headers for the channel numbers, but
        # flowutils compensate() takes the plain matrix and indices as
        # separate arguments (also note channel #'s vs indices)
        data = numpy.load(self.subsample_path)
        comp_data = flowutils.compensate.compensate(
            data,
            self.compensation[1:][:],
            self.compensation[0][:] - 1
        )
        numpy.save(
            BASE_DIR +
            comp_data)

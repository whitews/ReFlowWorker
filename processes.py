import numpy as np
import datetime
from flowstats import cluster
import json

PROCESS_LIST = {
    1: 'Test',
    2: 'HDP'
}


def test(process_request):
    process_request.compensate_samples()
    process_request.apply_logicle_transform(logicle_t=262144, logicle_w=0.5)
    process_request.normalize_transformed_samples()

    data_sets = list()
    sample_id_map = list()
    sample_metadata = dict()
    for s in process_request.samples[:2]:
        norm_data = np.load(s.normalized_path)
        data_sets.append(norm_data)
        sample_id_map.append(s.sample_id)
        sample_metadata[s.sample_id] = dict()
        sample_metadata[s.sample_id]['filename'] = s.original_filename
        sample_metadata[s.sample_id]['sha1'] = s.sha1
        sample_metadata[s.sample_id]['site_panel'] = s.site_panel_id
        if s.compensation is not None:
            if isinstance(s.compensation, np.ndarray):
                sample_metadata[s.sample_id]['compensation'] = \
                    s.compensation.tolist()
            elif isinstance(s.compensation, list):
                sample_metadata[s.sample_id]['compensation'] = s.compensation

    n_data_sets = len(data_sets)
    n_clusters = 32
    n_iterations = 2
    burn_in = 50
    seed = 123

    model = cluster.HDPMixtureModel(n_clusters, n_iterations, burn_in)

    time_0 = datetime.datetime.now()
    print time_0

    results = model.fit(
        data_sets,
        True,
        seed=seed,
        munkres_id=True,
        verbose=True
    )

    time_1 = datetime.datetime.now()

    delta_time = time_1 - time_0
    print delta_time.total_seconds()

    metadata = dict()
    metadata['input_parameters'] = dict()
    metadata['fcs_parameters'] = process_request.param_list
    metadata['panel_maps'] = process_request.panel_maps
    metadata['samples'] = sample_metadata

    archive_dict = dict()
    archive_dict['metadata'] = metadata
    archive_dict['results'] = dict()

    # pis are split by data set, then iteration
    if n_data_sets > 1:
        pis = np.array_split(results.pis, n_data_sets)
        for i, p in enumerate(pis):
            pis[i] = np.array_split(pis[i][0], n_iterations)
    elif n_data_sets == 1:
        pis = list()
        pis.append(np.array_split(results.pis, n_iterations))

    archive_dict['results']['samples'] = list()
    for i, pi in enumerate(pis):
        archive_dict['results']['samples'].append(dict())
        archive_dict['results']['samples'][i]['sample_id'] = sample_id_map[i]
        archive_dict['results']['samples'][i]['pis'] = [j.tolist() for j in pi]

    # mus and sigmas are split by iteration
    mus = np.array_split(results.mus, n_iterations)
    sigmas = np.array_split(results.sigmas, n_iterations)

    archive_dict['results']['mus'] = list()
    for i, mu in enumerate(mus):
        archive_dict['results']['mus'].append([j.tolist() for j in mu])

    archive_dict['results']['sigmas'] = list()
    for i, sigma in enumerate(sigmas):
        archive_dict['results']['sigmas'].append([j.tolist() for j in sigma])

    # Save JSON results
    file_path = process_request.results_directory + "/archived_results.json"
    output_file = open(file_path, 'wb')
    json.dump(archive_dict, output_file)
    output_file.close()

    # Get averaged results
    if n_data_sets > 1:
        results_avg = results.average()

        average_dict = dict()
        average_dict['metadata'] = metadata
        average_dict['results'] = dict()

        # averaged pis are split by data set
        pis = np.array_split(results_avg.pis, n_data_sets)

        average_dict['results']['samples'] = list()
        for i, pi in enumerate(pis):
            average_dict['results']['samples'].append(dict())
            average_dict['results']['samples'][i]['sample_id'] = sample_id_map[i]
            average_dict['results']['samples'][i]['pis'] = pi.tolist()

        average_dict['results']['mus'] = results_avg.mus.tolist()
        average_dict['results']['sigmas'] = results_avg.sigmas.tolist()

        # Save JSON averaged results
        file_path = process_request.results_directory + "/averaged_results.json"
        output_file = open(file_path, 'wb')
        json.dump(average_dict, output_file)
        output_file.close()

    return True


def hdp(process_request):
    # samples have already been downloaded, so start by applying compensation
    process_request.compensate_samples()

    # next, apply logicle transformation

    return False


dispatch_process = {
    1: test,
    2: hdp
}

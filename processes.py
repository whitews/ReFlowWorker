import numpy as np
import cPickle
import datetime
from flowstats import cluster

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
    for s in process_request.samples[:2]:
        norm_data = np.load(s.normalized_path)
        data_sets.append(norm_data)
        sample_id_map.append(s.sample_id)

    n_data_sets = len(data_sets)
    n_clusters = 48
    n_iterations = 2
    burn_in = 100
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

    archive_dict = dict()
    archive_dict['inputs'] = dict()
    # TODO: add input here

    # pis are split by data set, then iteration
    pis = np.array_split(results.pis, n_data_sets)
    for i, p in enumerate(pis):
        pis[i] = np.array_split(pis[i][0], n_iterations)

    archive_dict['samples'] = list()
    for i, pi in enumerate(pis):
        archive_dict['samples'].append(dict())
        archive_dict['samples'][i]['sample_id'] = sample_id_map[i]
        archive_dict['samples'][i]['pis'] = [j.tolist() for j in pi]

    # mus and sigmas are split by iteration
    mus = np.array_split(results.mus, n_iterations)
    sigmas = np.array_split(results.sigmas, n_iterations)

    archive_dict['mus'] = list()
    for i, mu in enumerate(mus):
        archive_dict['mus'].append([j.tolist() for j in mu])

    archive_dict['sigmas'] = list()
    for i, sigma in enumerate(sigmas):
        archive_dict['sigmas'].append([j.tolist() for j in sigma])

    # Get averaged results
    results_averaged = results.average()

    # Save our sample ID index
    file_path = process_request.results_directory + "/sample_id_map.txt"
    the_file = open(file_path, 'w')
    for sample_id in sample_id_map:
        the_file.write("%s\n" % sample_id)
    the_file.close()

    # Save pickled results
    file_path = process_request.results_directory + "/results.pkl"
    the_file = open(file_path, 'wb')
    cPickle.dump(results, the_file)
    the_file.close()

    # Save pickled averaged results
    file_path = process_request.results_directory + "/averaged_results.pkl"
    the_file = open(file_path, 'wb')
    cPickle.dump(results_averaged, the_file)
    the_file.close()

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

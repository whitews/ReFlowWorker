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
    for s in process_request.samples:
        norm_data = np.load(s.normalized_path)
        data_sets.append(norm_data)
        sample_id_map.append(s.sample_id)

    n_clusters = 48
    n_iterations = 2
    burn_in = 100

    model = cluster.HDPMixtureModel(n_clusters, n_iterations, burn_in)

    time_0 = datetime.datetime.now()
    print time_0

    results = model.fit(
        data_sets,
        True,
        seed=123,
        munkres_id=True,
        verbose=True
    )

    time_1 = datetime.datetime.now()

    delta_time = time_1 - time_0
    print delta_time.total_seconds()

    # Get averaged results
    results_averaged = results.average()

    # Save our sample ID index
    file_path = process_request.results_directory + "/sample_id_map.txt"
    with open(file_path) as the_file:
        for sample_id in sample_id_map:
            the_file.write("%s\n" % sample_id)

    # Save pickled results
    file_path = process_request.results_directory + "/results.pkl"
    the_file = open(file_path, 'wb')
    cPickle.dump(results, the_file)

    # Save pickled averaged results
    file_path = process_request.results_directory + "/averaged_results.csv"
    the_file = open(file_path, 'wb')
    cPickle.dump(results_averaged, the_file)

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

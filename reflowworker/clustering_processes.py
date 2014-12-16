from models import Cluster, SampleCluster, SampleClusterParameter

import numpy as np
# NOTE: we don't import cluster here because it causes an
# issue with PyCUDA and our daemonize procedure, see
# the hdp function for where this is actually imported
# from flowstats import cluster
import json


def hdp(process_request):
    iteration_count = int(process_request.clustering_options['iteration_count'])
    cluster_count = int(process_request.clustering_options['cluster_count'])

    burn_in = int(process_request.clustering_options['burnin'])
    random_seed = int(process_request.clustering_options['random_seed'])

    data_sets = list()
    sample_id_map = list()
    sample_metadata = dict()
    for s in process_request.samples:
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
    if not n_data_sets:
        # nothing for us to do, return True
        return True

    # NOTE: we import this here to avoid a PyCUDA issue when starting up
    # the daemonize procedure
    from flowstats import cluster

    model = cluster.HDPMixtureModel(
        cluster_count,
        iteration_count,
        burn_in)

    results = model.fit(
        data_sets,
        True,
        seed=random_seed,
        munkres_id=True,
        verbose=True
    )

    metadata = dict()

    # TODO: re-org this to the Worker
    metadata['input_parameters'] = process_request.clustering_options
    metadata['fcs_parameters'] = process_request.param_list
    metadata['panel_maps'] = process_request.panel_maps
    metadata['samples'] = sample_metadata
    # TODO: add asinh prescale (if used) to results metadata

    archive_dict = dict()
    archive_dict['metadata'] = metadata
    archive_dict['results'] = dict()

    # pis are split by data set, then iteration
    if n_data_sets > 1:
        pis = np.array_split(results.pis, n_data_sets)
        for i, p in enumerate(pis):
            pis[i] = np.array_split(pis[i][0], iteration_count)
    elif n_data_sets == 1:
        pis = list()
        pis.append(np.array_split(results.pis, iteration_count))
    else:
        pis = None

    archive_dict['results']['samples'] = list()
    for i, pi in enumerate(pis):
        archive_dict['results']['samples'].append(dict())
        archive_dict['results']['samples'][i]['sample_id'] = sample_id_map[i]
        archive_dict['results']['samples'][i]['pis'] = [j.tolist() for j in pi]

    # mus and sigmas are split by iteration
    mus = np.array_split(results.mus, iteration_count)
    sigmas = np.array_split(results.sigmas, iteration_count)

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
            average_dict['results']['samples'][i]['sample_id'] = \
                sample_id_map[i]
            average_dict['results']['samples'][i]['pis'] = pi.tolist()

        average_dict['results']['mus'] = results_avg.mus.tolist()
        average_dict['results']['sigmas'] = results_avg.sigmas.tolist()

        # Save JSON averaged results
        file_path = process_request.results_directory + "/averaged_results.json"
        output_file = open(file_path, 'wb')
        json.dump(average_dict, output_file, indent=2)
        output_file.close()
    else:
        # this shouldn't really happen, it would mean that HDP was requested
        # for one sample
        results_avg = [results]

    # now run make_modal to merge insignificant modes and create a common
    # "parent" mode for each cluster across all samples...else a common cluster
    # between 2 samples may get a different cluster index
    # Note, the cluster to mode mapping is in the "cmap" property
    modal_mixture = results_avg.make_modal()

    # initialize our list of Cluster instances
    clusters = list()
    for i in range(len(modal_mixture.cmap)):
        clusters.append(Cluster(i))

    # update our list of clusters
    # first iterate over our data sets to get the classified events
    for i, sample in enumerate(process_request.samples):
        classifications = results_avg[i].classify(data_sets[i])

        # group event indices by the modal mixture mode for this sample
        event_map = dict()
        for j, event_class in enumerate(classifications):
            modal_event_class = None
            for m_class in modal_mixture.cmap:
                if event_class in modal_mixture.cmap[m_class]:
                    modal_event_class = m_class
                    break

            if modal_event_class is None:
                continue

            if modal_event_class in event_map:
                # append this mode index to this sample cluster
                event_map[modal_event_class].append(sample.event_indices[j])
            else:
                # create a new map for this sample cluster
                event_map[modal_event_class] = [sample.event_indices[j]]

        # now we have all the events for this sample classified and organized
        # by cluster, so we can start creating the SampleCluster instances
        for event_class in event_map:
            # save SampleClusterParameter instances for this SampleCluster
            parameters = list()
            for k, channel in enumerate(process_request.panel_maps[sample.site_panel_id]):
                parameters.append(
                    SampleClusterParameter(
                        channel_number=channel + 1,  # channel is an index
                        location=modal_mixture.modes[event_class][k]
                    )
                )

            clusters[event_class].add_sample_cluster(
                SampleCluster(
                    sample_id=sample.sample_id,
                    parameters=parameters,
                    event_indices=event_map[event_class]
                )
            )

    return clusters
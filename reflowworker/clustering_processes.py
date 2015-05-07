from models import Cluster, SampleCluster, SampleClusterParameter, \
    SampleClusterComponent, SampleClusterComponentParameter

import numpy as np
# NOTE: we don't import cluster here because it causes an
# issue with PyCUDA and our daemonize procedure, see
# the hdp function for where this is actually imported
# from flowstats import cluster


def hdp(process_request):
    iteration_count = int(process_request.clustering_options['iteration_count'])
    cluster_count = int(process_request.clustering_options['cluster_count'])

    burn_in = int(process_request.clustering_options['burnin'])
    random_seed = int(process_request.clustering_options['random_seed'])

    data_sets = list()
    for s in process_request.samples:
        norm_data = np.load(s.normalized_path)
        data_sets.append(norm_data)

    n_data_sets = len(data_sets)
    if not n_data_sets:
        # nothing for us to do, return True
        return True

    # NOTE: we import this here to avoid a PyCUDA issue when starting up
    # the daemonize procedure
    from flowstats import cluster

    if n_data_sets > 1:
        model = cluster.HDPMixtureModel(
            cluster_count,
            iteration_count,
            burn_in
        )
        results = model.fit(
            data_sets,
            True,
            seed=random_seed,
            munkres_id=True,
            verbose=True
        )
    else:
        model = cluster.DPMixtureModel(
            cluster_count,
            iteration_count,
            burn_in,
            model='dp'
        )
        results = model.fit(
            data_sets[0],
            True,
            seed=random_seed,
            munkres_id=True,
            verbose=True
        )

    # Run make_modal on averaged results to merge insignificant modes
    # and create a common "parent" mode for each cluster across all
    # samples...else a common cluster between 2 samples may get a different
    # cluster index
    results_avg = results.average()
    modal_mixture = results_avg.make_modal()

    # Create list of Cluster instances from the modes
    # Note, the cluster to mode mapping is in the "cmap" property
    clusters = list()
    for i in range(len(modal_mixture.cmap)):
        clusters.append(Cluster(i))

    # Create the SampleCluster & SampleClusterComponent instances belonging
    # to each Cluster instance. To do this, we iterate over our data sets
    # to classify the data events
    for i, sample in enumerate(process_request.samples):
        if n_data_sets > 1:
            classifications = modal_mixture[i].classify(data_sets[i])
        else:
            classifications = modal_mixture.classify(data_sets[i])

        # Grab events from transformed data set b/c the normalized data
        # doesn't have all columns.
        # transformed data already has the original index in 1st column
        x_data = np.load(sample.transformed_path)

        # Group event indices by the modal mixture mode for this sample.
        # event_map holds the event data plus the event's index (as 1st column)
        # for each mode. the first row for each map contains the header row
        event_map = dict()
        for j, event_class in enumerate(classifications):
            if event_class not in event_map:
                # create a new map for this sample cluster w/ header row
                header_row = ['event_index']
                header_row.extend(
                    range(1, x_data.shape[1] + 1)
                )
                event_map[event_class] = [header_row]  # a list of lists

            # grab event from transformed data, and add to map
            event_row = list(x_data[j])
            event_row.insert(0, sample.event_indices[j])
            event_map[event_class].append(event_row)

        # now we have all the events for this sample classified and organized
        # by cluster, so we can start creating the SampleCluster instances
        for event_class in event_map:
            # save SampleClusterParameter instances for this SampleCluster
            sc_parameters = list()
            for k, channel in enumerate(
                    process_request.panel_maps[sample.site_panel_id]):
                sc_parameters.append(
                    SampleClusterParameter(
                        channel_number=channel + 1,  # channel is an index
                        location=modal_mixture.modes[event_class][k]
                    )
                )

            # Create SampleClusterComponents for each component of this
            # mode for this sample
            components = list()
            for l, comp in enumerate(modal_mixture.cmap[event_class]):
                # get the comp parameters first
                sc_comp_parameters = list()
                for k, channel in enumerate(
                        process_request.panel_maps[sample.site_panel_id]):
                    sc_comp_parameters.append(
                        SampleClusterComponentParameter(
                            channel_number=channel + 1,
                            location=modal_mixture.mus[l][k]
                        )
                    )

                # Create new component with comp_params, sigmas, and pis
                # But, first we need to identify the channels in the
                # covariance matrix
                covariance = np.insert(
                    modal_mixture.sigmas[comp],
                    0,
                    process_request.panel_maps[sample.site_panel_id],
                    axis=0
                )
                components.append(
                    SampleClusterComponent(
                        index=comp,
                        covariance=covariance,
                        weight=modal_mixture.pis[i][comp],
                        parameters=sc_comp_parameters
                    )
                )

            clusters[event_class].add_sample_cluster(
                SampleCluster(
                    sample_id=sample.sample_id,
                    parameters=sc_parameters,
                    events=event_map[event_class],
                    components=components
                )
            )

    return clusters
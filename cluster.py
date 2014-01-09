from flowstats.cluster import HDPMixtureModel
import numpy as np

n_clusters = 256
n_iterations = 100
burn_in = 1000

delta = 0.1
data_size = 10000
bias_sample_size = 1000

seed = 123

cost_type = 'mean'
dalign_type = 'diagonal'
method = 'ralg'

# these will eventually come from PR inputs
sample_dict = dict(A='C6902C9H', C='B6901Q8B', E='C6904VL4')
stim_dict = {
    '01': 'BFA',
    '02': 'BFA',
    '03': 'BFA',
    '05': 'CMVpp65',
    '06': 'CMVpp65',
    '07': 'CMVpp65',
    '09': 'CEF',
    '10': 'CEF',
    '11': 'CEF'
}

dims = [
    (4, 3),
    (0, 3),
    (5, 6),
    (4, 7),
    (5, 6),
    (4, 7)
]

parameter_names = [
    'FSC-A',
    'FSC-H',
    'FSC-W',
    'SSC-A',
    'CD3-APC-A',
    'CD4-FITC-A',
    'CD8-PerCP-Cy55-A',
    'IFN-IL2-PE-A'
]

targets = {
    0: ['singlets'],
    1: ['lymphs'],
    2: ['cd4'],
    3: ['cd8'],
    5: ['cd4cyto'],
    6: ['cd8cyto']
}

strategies = {
    'lymphs': (0, 1),
    'singlets': (0,),
    'cd4': (0, 1, 2, 5),
    'cd8': (0, 1, 3, 4),
    'cd4cyto': (0, 1, 2, 5, 6),
    'cd8cyto': (0, 3, 4, 7)
}

parents = {
    'singlets': 'all',
    'lymphs': 'singlets',
    'cd4': 'lymphs',
    'cd8': 'lymphs',
    'cd4cyto': 'cd4',
    'cd8cyto': 'cd8'
}

gates = [
    [1000.0*np.array([[75, 75, 250, 250], [90, 50, 220, 260]])],
    [1000.0*np.array([[55, 55, 95, 95], [90, 10, 10, 90]])],
    [1000.0*np.array([[35, 35, 80, 80], [70, 35, 35, 70]])],
    [1000.0*np.array([[35, 35, 80, 80], [70, 25, 25, 70]])],
    [
        1000.0*np.array([[0, 0, 30, 30, 60, 60], [70, 25, 25, 35, 35, 70]]),
        1000.0*np.array([[30, 30, 60, 60], [35, 0, 0, 35]])
    ],
    [1000.0*np.array([[25, 25, 70, 70], [100, 50, 50, 100]])],
    [1000.0*np.array([[25, 25, 70, 70], [100, 45, 45, 100]])]
]

hlines = {
    0: [50000, 30000],
    1: [10000, 20000],
    2: [47000, 18000],
    3: [48000, 15000],
    4: [46000, 14000],
    5: [55000, 13000],
    6: [54000, 12000]
}

scatter_indices = [
    i for i, p in enumerate(parameter_names) if
    ('FSC' in p or 'SSC' in p)
]


def run_cluster(data_sets, device):
    model = HDPMixtureModel(n_clusters, n_iterations, burn_in)

    results = model.fit(
        data_sets,
        device,
        seed=seed,
        munkres_id=True)

    results_averaged = results.average()
    results_averaged_modal = results_averaged.make_modal()
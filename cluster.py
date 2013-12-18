from fcm.statistics import HDPMixtureModel
import os
import numpy as np
import re
import cPickle

nclusts_align = 32
niter_align = 100
nburnin_align = 1000

nclusts = 256
niter = 100
nburnin = 1000
verbose = 10
model0 = 'DP'
model1 = 'HDP'
model2 = 'HDP'
delta = 0.1
f = 1
e = 1.0/(f*f)
data_size = 50000
seed = 123
cost_type = 'mean'
dalign_type = 'diagonal'
method = 'ralg'

# bias_sample_size = data_size/2 # 1000
bias_sample_size = 1000
pos_sample = 'E05'
neg_sample = 'A01'

panel = '4C'
basedir = '/tmp/reflow_worker/'

pat = re.compile(r'^cd[4|8].+')

# clustering tag
tag = '%s_%s_k%d_n%d_b%d_s%d_ns%d_e%g_f%g_d%g' % (
    model2,
    panel,
    nclusts_align,
    niter_align,
    nburnin_align,
    seed,
    data_size,
    e,
    f,
    delta)

# alignment tag
atag = '%s_%s_%s_%s_%s_ka%d_na%d_ba%d_k%d_n%d_b%d_s%d_ns%d_e%g_f%g_d%g' % (
    model2,
    panel,
    cost_type,
    method,
    dalign_type,
    nclusts_align,
    niter_align,
    nburnin_align,
    nclusts,
    niter,
    nburnin,
    seed,
    data_size,
    e,
    f,
    delta)

parents_dict = {
    '4C': {
        'singlets': 'all',
        'lymphs': 'singlets',
        'cd4': 'lymphs',
        'cd8': 'lymphs',
        'cd4cyto': 'cd4',
        'cd8cyto': 'cd8'
    },
    '7C': {
        'singlets': 'all',
        'lymphs': 'singlets',
        'cd4': 'lymphs',
        'cd8': 'lymphs',
        'cd4ifn': 'cd4',
        'cd4il2': 'cd4',
        'cd4tnf': 'cd4',
        'cd8ifn': 'cd8',
        'cd8il2': 'cd8',
        'cd8tnf': 'cd8',
    }
}

strategies_dict = {
    '4C': {
        'lymphs': (0, 1),
        'singlets': (0,),
        'cd4': (0, 1, 2, 5),
        'cd8': (0, 1, 3, 4),
        'cd4cyto': (0, 1, 2, 5, 6),
        'cd8cyto': (0, 3, 4, 7)
    },
    '7C': {
        'lymphs': (0, 1),
        'singlets': (0,),
        'cd4': (0, 1, 2, 3, 6),
        'cd8': (0, 1, 2, 4, 5),
        'cd4ifn': (0, 1, 2, 3, 6, 7),
        'cd4il2': (0, 1, 2, 3, 6, 8),
        'cd4tnf': (0, 1, 2, 3, 6, 9),
        'cd8ifn': (0, 1, 2, 4, 5, 7),
        'cd8il2': (0, 1, 2, 4, 5, 8),
        'cd8tnf': (0, 1, 2, 4, 5, 9)
    }
}

targets_dict = {
    '4C': {
        0: ['singlets'],
        1: ['lymphs'],
        2: ['cd4'],
        3: ['cd8'],
        5: ['cd4cyto'],
        6: ['cd8cyto']
    },
    '7C': {
        1: ['lymphs'],
        0: ['singlets'],
        3: ['cd4'],
        4: ['cd8'],
        6: ['cd4ifn', 'cd8ifn'],
        7: ['cd4il2', 'cd8il2'],
        8: ['cd4tnf', 'cd8tnf']
    }
}

parameter_dict = {
    '4C': [
        'FSC-A',
        'FSC-H',
        'FSC-W',
        'SSC-A',
        'CD3-APC-A',
        'CD4-FITC-A',
        'CD8-PerCP-Cy55-A',
        'IFN-IL2-PE-A'
    ],
    '7C': [
        'FSC-A',
        'FSC-H',
        'FSC-W',
        'SSC-A',
        'CD3-V450-A',
        'CD4-PE-Cy7-A',
        'CD8-PerCP-Cy55-A',
        'IFNg-PE-A',
        'IL2-APC-A',
        'TNFa-FITC-A',
        'aAmine-A'
    ]
}

dims_dict = {
    '4C': [
        (4, 3),
        (0, 3),
        (5, 6),
        (4, 7),
        (5, 6),
        (4, 7)
    ],
    '7C': [
        (0, 1),
        (3, 2),
        (0, 9),
        (3, 4),
        (3, 5),
        (4, 5),
        (3, 6),
        (3, 7),
        (3, 8)
    ]
}

gate_dict = {
    '4C': [
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
    ],

    '7C': [
        [1000.0*np.array([[75, 75, 250, 250], [90, 50, 220, 260]])],
        [1000.0*np.array([[55, 55, 95, 95], [90, 10, 10, 90]])],
        [1000.0*np.array([[75, 75, 250, 250], [40, 10, 10, 40]])],
        [1000.0*np.array([[35, 35, 90, 90], [90, 35, 35, 90]])],
        [1000.0*np.array([[35, 35, 90, 90], [90, 35, 35, 90]])],
        [
            1000.0*np.array([[0, 0, 35, 35, 90, 90], [90, 35, 35, 55, 55, 90]]),
            1000.0*np.array([[35, 35, 90, 90], [55, 0, 0, 55]])
        ],
        [1000.0*np.array([[35, 35, 90, 90], [95, 55, 55, 95]])],
        [1000.0*np.array([[35, 35, 90, 90], [95, 55, 55, 95]])],
        [1000.0*np.array([[35, 35, 90, 90], [95, 55, 55, 95]])],
    ]
}

hline_dict = {
    '4C':  {
        0: [50000, 30000],
        1: [10000, 20000],
        2: [47000, 18000],
        3: [48000, 15000],
        4: [46000, 14000],
        5: [55000, 13000],
        6: [54000, 12000]
    },
    '7C': {
        0: [45000, ],
        1: [110000, ],
        2: [60000, 30000],
        3: [64000, 15000],
        4: [72000, 35000],
        5: [70000, 35000],
        6: [85000, 26000],
        7: [65000, 18000],
        8: [80000, 37000],
    }
}

# sample_dict = dict(A='K6902C85', C = 'F6901PRY', E = 'G6904VJT')  # 4C
sample_dict = dict(A='C6902C9H', C='B6901Q8B', E='C6904VL4')  # 7C

stim_dict = {
    '01': 'BFA', '02': 'BFA', '03': 'BFA',
    '05': 'CMVpp65', '06': 'CMVpp65', '07': 'CMVpp65',
    '09': 'CEF', '10': 'CEF', '11': 'CEF'
}

dims = dims_dict[panel]
parameter_names = parameter_dict[panel]
targets = targets_dict[panel]
strategies = strategies_dict[panel]
parents = parents_dict[panel]
gates = gate_dict[panel]
hlines = hline_dict[panel]

scatter_idxs = [i for i, p in enumerate(parameter_names) if
                ('FSC' in p or 'SSC' in p)]

site_names = np.load('%s_site_names.npy' % panel)
site_names = [s for s in site_names if not s in ['012', '034', '036']]


def pickle(obj, filename):
    cPickle.dump(obj, open(filename, 'wb'), -1)


def run_cluster(site_name, device):

    wd = os.path.join(basedir, site_name)
    if not os.path.exists(os.path.join(wd, '%s_cs.pic' % tag)):
        yss = np.load(os.path.join(wd, 'xss_%d.npy' % data_size))

        # cluster_data does not like 3D numpy arrays
        yss = [ys for ys in yss]

        results = cluster_data(
            #yss,
            #nclusts=nclusts_align,
            #niter=niter_align,
            #nburnin=nburnin_align,
            #model=model0,
            e=e,
            f=f,
            verbose=verbose,
            device=device,
            seed=seed,
            ident=True)

        m = HDPMixtureModel(nclusts_align, niter_align, nburnin_align)

        for i, j in kwargs.items():
            if i != 'verbose':
                m.__setattr__(i, j)

        results = m.fit(yss, verbose=False)

        rs = results.average()
        cs = rs.make_modal()

        pickle(results, os.path.join(wd, '%s.pic' % tag))
        pickle(rs, os.path.join(wd, '%s_rs.pic' % tag))
        pickle(cs, os.path.join(wd, '%s_cs.pic' % tag))
        print "%s clustering with %d components is done" % (
            site_name, nclusts_align)


PROCESS_LIST = {
    1: 'Test',
    2: 'HDP'
}


def test(process_request):
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

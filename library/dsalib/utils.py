import os
import sys

from smv import SmvApp

# Ideally, user should be able to use sparkSession in
# the run method, as long as this file is imported. However
# sparkSession may not be aviable at the importing time
# There is a workaround:
# https://stackoverflow.com/questions/1462986/lazy-module-variables-can-it-be-done
# May apply that if the need is more obvious. For now, just
# provide the funcation below
def pandasToSpark(pdf):
    sparkSession = SmvApp.getInstance().sparkSession
    return sparkSession.createDataFrame(pdf)

class RedirectStdStreams(object):
    """
        From StackOverflow:
        https://stackoverflow.com/questions/6796492/temporarily-redirect-stdout-stderr
        Created because h2o logs a bunch of noisy stuff including pointing the user to their webUI
        every time an H2oContext is 'got or created' which gets ridiculous when running stuff in
        notebook.

        Thus, we have this context manager to suppress output temporarily to wrap the get or create
    """
    def __init__(self, stdout=None, stderr=None):
        self._stdout = stdout or sys.stdout
        self._stderr = stderr or sys.stderr

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush(); self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

devnull = open(os.devnull, 'w')

def getH2oContext():
    """
        Init the hc (H2OContext) using the current sparkSession.
        Using this instead of h2o.init()
    """
    sparkSession = SmvApp.getInstance().sparkSession
    import pysparkling
    hc = pysparkling.H2OContext.getOrCreate(sparkSession)
    return hc

def h2oToSpark(h2oDf, name = ''):
    with RedirectStdStreams(stdout=devnull, stderr=devnull):
        hc = getH2oContext()
        spark_frame = hc.as_spark_frame(h2oDf)
    return spark_frame

def sparkToH2o(sparkDf):
    with RedirectStdStreams(stdout=devnull, stderr=devnull):
        hc = getH2oContext()
        h2o_frame = hc.as_h2o_frame(sparkDf)
    return h2o_frame

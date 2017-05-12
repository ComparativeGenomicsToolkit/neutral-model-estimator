from subprocess import check_call
import luigi

from neutral_model_estimator.filter import SubsampleBed, ApplySingleCopyFilter
from neutral_model_estimator.ancestral_repeats import GenerateAncestralRepeatsBed

class GenerateNeutralModel(luigi.Task):
    """Wrapper task that ties everything together."""
    genome = luigi.Parameter()
    hal_file = luigi.Parameter()
    sample_proportion = luigi.FloatParameter(default=1.0)
    no_single_copy = luigi.BoolParameter()
    rm_species = luigi.Parameter()

    def requires(self):
        job = self.clone(GenerateAncestralRepeatsBed)
        yield job
        bed = job.output().path
        if not self.no_single_copy:
            job = self.clone(ApplySingleCopyFilter, prev_task=job)
            yield job
            bed = job.output().path
        yield self.clone(HalPhyloPTrain, prev_task=job)

class HalPhyloPTrain(luigi.Task):
    """Runs halPhyloPTrain.py to do the actual training."""
    prev_task = luigi.TaskParameter()
    num_procs = luigi.IntParameter(default=2)
    hal_file = luigi.Parameter()
    genome = luigi.Parameter()

    def requires(self):
        return self.prev_task

    def output(self):
        return luigi.LocalTarget('%s-%s.mod' % (self.hal_file, self.genome)), luigi.LocalTarget('%s-%s.err' % (self.hal_file, self.genome))

    def run(self):
        bed_file = self.input().path
        check_call(["halPhyloPTrain.py", "--numProc", str(self.num_procs), "--no4d", self.hal_file,
                    self.genome, bed_file, self.output()[0].path, "--error", self.output()[1].path])

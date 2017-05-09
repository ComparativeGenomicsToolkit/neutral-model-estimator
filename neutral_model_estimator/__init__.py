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

    def requires(self):
        job = self.clone(GenerateAncestralRepeatsBed)
        yield job
        bed = job.output().path
        if not self.no_single_copy:
            job = self.clone(ApplySingleCopyFilter, bed_file=bed)
            yield job
            bed = job.output().path
        job = self.clone(SubsampleBed, bed_file=bed)
        yield job
        bed = job.output().path
        yield self.clone(HalPhyloPTrain, bed_file=bed)

class HalPhyloPTrain(luigi.Task):
    """Runs halPhyloPTrain.py to do the actual training."""
    num_procs = luigi.IntParameter(default=2)
    bed_file = luigi.Parameter()
    hal_file = luigi.Parameter()
    genome = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('%s.mod' % self.bed_file), luigi.LocalTarget('%s.err' % self.bed_file)

    def run(self):
        check_call(["halPhyloPTrain.py", "--numProc", str(self.num_procs), "--no4d", self.hal_file,
                    self.genome, self.bed_file, self.output()[0].path, "--error", self.output()[1].path])

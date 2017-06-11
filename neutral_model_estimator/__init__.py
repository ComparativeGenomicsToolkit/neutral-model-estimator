from subprocess import check_call
import os
import luigi

# This has to go above the following imports to avoid an import cycle
class NMETask(luigi.Task):
    """Base class wrapping common parameters for tasks in the pipeline."""
    genome = luigi.Parameter()
    hal_file = luigi.Parameter()
    work_dir = luigi.Parameter()
    # Toil options
    batchSystem = luigi.Parameter(default='singleMachine')
    parasolCommand = luigi.Parameter(default='parasol')

    def target_in_work_dir(self, filename):
        return luigi.LocalTarget(os.path.join(self.work_dir, filename))

from neutral_model_estimator.filter import SubsampleBed, ApplySingleCopyFilter
from neutral_model_estimator.ancestral_repeats import GenerateAncestralRepeatsBed

class GenerateNeutralModel(NMETask):
    """Wrapper task that ties everything together."""
    sample_proportion = luigi.FloatParameter(default=1.0)
    no_single_copy = luigi.BoolParameter()
    rm_species = luigi.Parameter()

    def requires(self):
        job = self.clone(GenerateAncestralRepeatsBed)
        yield job
        if not self.no_single_copy:
            job = self.clone(ApplySingleCopyFilter, prev_task=job)
            yield job
        yield self.clone(HalPhyloPTrain, prev_task=job, sample_proportion=self.sample_proportion)

class HalPhyloPTrain(NMETask):
    """Runs halPhyloPTrain.py to do the actual training."""
    prev_task = luigi.TaskParameter()
    num_procs = luigi.IntParameter(default=2)
    sample_proportion = luigi.FloatParameter(default=1.0)

    def requires(self):
        return self.clone(SubsampleBed, prev_task=self.prev_task)

    def output(self):
        hal_name = os.path.basename(self.hal_file)
        return self.target_in_work_dir('%s-%s.mod' % (hal_name, self.genome))

    def run(self):
        bed_file = self.input().path
        check_call(["halPhyloPTrain.py", "--numProc", str(self.num_procs), "--no4d", self.hal_file,
                    self.genome, bed_file, self.output().path])

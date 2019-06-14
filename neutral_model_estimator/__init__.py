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

from neutral_model_estimator.filter import SubsampleBed, ExtractSingleCopyRegions
from neutral_model_estimator.ancestral_repeats import GenerateAncestralRepeatsBed
from neutral_model_estimator.rescale import RescaleNeutralModel

class Extract4dSites(NMETask):
    cds_bed = luigi.Parameter()

    def output(self):
        return self.target_in_work_dir('4d.bed')

    def run(self):
        with self.output().temporary_path() as output_path:
            check_call("hal4dExtract --conserved %s %s %s %s" % (self.hal_file, self.genome, self.cds_bed, output_path), shell=True)

class GenerateNeutralModel(NMETask):
    """Wrapper task that ties everything together."""
    num_bases = luigi.IntParameter(default=1000000000)
    no_single_copy = luigi.BoolParameter()
    neutral_data = luigi.ChoiceParameter(choices=['4d', 'ancestral_repeats'])
    rescale_chroms = luigi.DictParameter()

    def requires(self):
        if self.neutral_data == '4d':
            job = self.clone(Extract4dSites)
        else:
            job = self.clone(GenerateAncestralRepeatsBed)
        yield job
        job = self.clone(SubsampleBed, prev_task=job)
        yield job
        if not self.no_single_copy:
            job = self.clone(ExtractSingleCopyRegions, prev_task=job)
            yield job
        job = self.clone(HalPhyloPTrain, prev_task=job, num_bases=self.num_bases)
        yield job
        for set_name, chrom_set in self.rescale_chroms.items():
            yield self.clone(RescaleNeutralModel, prev_task=job, chroms=chrom_set, set_name=set_name)

class HalPhyloPTrain(NMETask):
    """Runs halPhyloPTrain.py to do the actual training."""
    prev_task = luigi.TaskParameter()
    num_procs = luigi.IntParameter(default=2)
    num_bases = luigi.IntParameter(default=1000000000)
    model_type = luigi.ChoiceParameter(choices=('SSREV', 'REV'), default='SSREV')

    def requires(self):
        return self.prev_task

    def output(self):
        hal_name = os.path.basename(self.hal_file)
        return self.target_in_work_dir('%s-%s.mod' % (hal_name, self.genome))

    def run(self):
        bed_file = self.input().path
        check_call(["halPhyloPTrain.py", "--numProc", str(self.num_procs), "--no4d", self.hal_file,
                    self.genome, bed_file, '--substMod', self.model_type, self.output().path])

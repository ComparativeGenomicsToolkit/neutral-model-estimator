from neutral_model_estimator.filter import SubsampleBed, ApplySingleCopyFilter
from neutral_model_estimator.ancestral_repeats import GenerateAncestralRepeatsBed
import luigi

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
            bed = job.output().path
            yield job
        yield self.clone(SubsampleBed, bed_file=bed)

from subprocess import check_call

import luigi

from ancestral_repeats import GenerateAncestralRepeatsBed

class ExtractSingleCopyRegions(luigi.Task):
    """Get a BED of single-copy regions from a hal file."""
    hal_file = luigi.Parameter()
    genome = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('singleCopyRegions-%s.bed' % self.genome)

    def run(self):
        with self.output().open('w') as f:
            check_call(["halSingleCopyRegionsExtract", self.hal_file, self.genome], stdout=f)

class ApplySingleCopyFilter(luigi.Task):
    """Restrict the bed file to be only within single-copy regions."""
    genome = luigi.Parameter()
    hal_file = luigi.Parameter()
    required_overlap = luigi.FloatParameter(default=0.5)

    def requires(self):
        return (ExtractSingleCopyRegions(hal_file=self.hal_file, genome=self.genome),
                GenerateAncestralRepeatsBed())

    def output(self):
        return luigi.LocalTarget('filtered-%s.bed' % self.genome)

    def run(self):
        single_copy_regions = self.input()[0]
        bed_file = self.input()[1]
        with self.output().open('w') as f:
            check_call(["bedtools", "intersect", "-a", bed_file.path,
                        "-b", single_copy_regions.path, "-f", str(self.required_overlap)],
                       stdout=f)

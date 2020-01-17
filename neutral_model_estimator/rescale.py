import os
import shutil
from subprocess import check_call, check_output
from tempfile import NamedTemporaryFile

import luigi

from neutral_model_estimator import NMETask

class RescaleNeutralModel(NMETask):
    num_sampled_bases = luigi.IntParameter(default=3000)
    rescale_genome = luigi.Parameter()
    chroms = luigi.ListParameter()
    prev_task = luigi.TaskParameter()
    set_name = luigi.Parameter()

    def requires(self):
        return self.prev_task

    def output(self):
        return self.target_in_work_dir('%s-%s-%s.mod' % (os.path.basename(self.hal_file), self.rescale_genome, self.set_name))

    def run(self):
        # Sample bases
        sampled_bases = NamedTemporaryFile()
        with NamedTemporaryFile() as f:
            bed_lines = check_output(['halStats', '--bedSequences', self.rescale_genome, self.hal_file]).split('\n')
            for line in bed_lines:
                if len(line.strip()) == 0:
                    continue
                fields = line.split()
                if fields[0] in self.chroms:
                    f.write('%s\t%s\n' % (fields[0], fields[2]))
            f.flush()
            check_call(['bedtools', 'random', '-n', str(self.num_sampled_bases), '-l', '1', '-g', f.name], stdout=sampled_bases)

        # Extract a MAF from the sampled bases
        maf_file = NamedTemporaryFile()
        check_call(['hal2maf', '--onlyOrthologs', '--noAncestors', '--refGenome', self.rescale_genome, '--refTargets', sampled_bases.name, self.hal_file, maf_file.name])
        sampled_bases.close()

        # Convert to SS
        ss_file = NamedTemporaryFile()
        check_call(['msa_view', '-z', '--out-format', 'SS', maf_file.name], stdout=ss_file)
        maf_file.close()

        # Scale the model using phyloFit
        mod_file = NamedTemporaryFile()
        check_call(['phyloFit', '--init-model', self.input().path, '--scale-only', '-o', mod_file.name, ss_file.name])
        ss_file.close()
        shutil.move(mod_file.name + '.mod', self.output().path)

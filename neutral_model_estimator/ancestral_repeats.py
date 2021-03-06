#!/usr/bin/env python
import os
import re
import subprocess
from subprocess import check_call, Popen
from glob import glob

import luigi
from toil.common import Toil
from toil.job import Job

from neutral_model_estimator import NMETask

# Toil jobs

def concatenate_job(job, input_ids):
    """Bring together the various output files from each chunk."""
    output = os.path.join(job.fileStore.getLocalTempDir(), 'rm.out')
    input_paths = map(job.fileStore.readGlobalFile, input_ids)
    with open(output, 'w') as outfile:
        for input_path in input_paths:
            with open(input_path) as infile:
                for line in infile:
                    outfile.write(line)
    return job.fileStore.writeGlobalFile(output)

def repeat_masking_job(job, input_fasta, species):
    """Do the actual RepeatMasking for each chunk."""
    temp_dir = job.fileStore.getLocalTempDir()
    os.chdir(temp_dir)
    local_fasta = os.path.join(temp_dir, 'input.fa')
    job.fileStore.readGlobalFile(input_fasta, local_fasta, cache=False)
    check_call("chmod a+rw %s" % local_fasta, shell=True)
    check_call("RepeatMasker -species '{species}' {input}".format(species=species, input=local_fasta), shell=True)
    output_path = local_fasta + '.out'
    masked_out = job.fileStore.writeGlobalFile(output_path)
    return masked_out

def split_fasta(input_fasta, split_size, work_dir):
    """Split a fasta file into approximately 'split_size'-sized pieces by sequence.

    Won't play nicely with chromosome-sized sequence."""
    lift_file = os.path.join(work_dir, "lift")
    check_call("faSplit about {input} {split_size} {out_root}".format(
        input=input_fasta,
        split_size=split_size,
        out_root=os.path.join(work_dir, "out")), shell=True)
    return glob(os.path.join(work_dir, "out*"))

def split_fasta_job(job, input_fasta, split_size, species):
    """Intro job to the repeat-masking toil pipeline."""
    work_dir = job.fileStore.getLocalTempDir()
    local_fasta = os.path.join(work_dir, 'in.fa')
    job.fileStore.readGlobalFile(input_fasta, local_fasta)
    split_fastas = split_fasta(local_fasta, split_size, work_dir)
    split_fasta_ids = [job.fileStore.writeGlobalFile(f) for f in split_fastas]
    repeat_masked = [job.addChildJobFn(repeat_masking_job, id, species).rv() for id in split_fasta_ids]
    return job.addFollowOnJobFn(concatenate_job, repeat_masked).rv()

# Luigi tasks

class GetAncestralRepeats(NMETask):
    """Get a RepeatMasker .out file. Delegates to a toil pipeline to parallelize."""
    split_size = luigi.IntParameter(default=50000)
    rm_species = luigi.Parameter()

    def requires(self):
        return self.clone(ExtractGenomeFasta)

    def output(self):
        return self.target_in_work_dir('%s.out' % self.genome)

    def run(self):
        jobStorePath = '%s/jobStore-repeats-%s' % (self.work_dir, self.genome)
        opts = Job.Runner.getDefaultOptions(jobStorePath)
        if os.path.exists(jobStorePath):
            opts.restart = True
        opts.disableCaching = True
        opts.batchSystem = self.batchSystem
        opts.parasolCommand = self.parasolCommand
        opts.environment = ["LD_LIBRARY_PATH"]
        with Toil(opts) as toil:
            fasta = toil.importFile('file://' + os.path.abspath(self.input().path))
            if opts.restart:
                result = toil.restart()
            else:
                result = toil.start(Job.wrapJobFn(split_fasta_job,
                                                  fasta, self.split_size, self.rm_species))
            toil.exportFile(result, 'file://' + os.path.abspath(self.output().path))

class ExtractGenomeFasta(NMETask):
    """Get the fasta for a genome from the HAL file."""
    def output(self):
        return self.target_in_work_dir('%s.fa' % self.genome)

    def run(self):
        with self.output().open('w') as f:
            check_call(["hal2fasta", self.hal_file, self.genome], stdout=f)

class GenerateAncestralRepeatsBed(NMETask):
    """Go from an ancestral RepeatMasker .out file to a .bed.

    The repeats are filtered to remove likely-conserved "repeats" such
    as tRNAs, low-complexity regions, etc.
    """
    rm_species = luigi.Parameter()
    

    # Matches RM .out headers and blank lines.
    header_re = re.compile(r'^$|^   SW   perc perc perc .*|score   div\. del\. .*'
                            '|There were no repetitive sequences detected')

    # We want to ignore some repeat families because they may be more conserved than others.
    ignored_family_re = re.compile(r'tRNA|SINE/tRNA.*|Low_complexity')

    def requires(self):
        return self.clone(GetAncestralRepeats)

    def output(self):
        return self.target_in_work_dir('ancestralRepeats.bed')

    def run(self):
        with self.input().open() as rmfile, self.output().open('w') as bedfile:
            for line in rmfile:
                if self.header_re.match(line):
                    continue
                bedfile.write(self.rm_line_to_bed_line(line))

    def rm_line_to_bed_line(self, rm_line):
        r"""Translate a RM .out line to a BED line, filtering out certain repeat families.

        >>> garb = GenerateAncestralRepeatsBed('', '', '')
        >>> garb.rm_line_to_bed_line('  652   13.0  0.0  2.0  rootrefChr1140   42976  43077  (83898) C SINEC_old  SINE/tRNA           (12)    100      1  61\n')
        ''
        >>> garb.rm_line_to_bed_line('  240   20.0  0.0  0.0  rootrefChr21029  184566 184610  (13049) C L1M5             LINE/L1            (329)   5913   5869 272\n')
        'rootrefChr21029\t184566\t184610\tLINE/L1\n'
        """
        fields = rm_line.strip().split()
        # There should be 15 or 16 fields, depending on if the
        # repeat ID has a star next to it
        if not (len(fields) == 15 or len(fields) == 16):
            raise RuntimeError("Found %d fields (contents %s) but there should only be 15"
                               " or 16 fields in a RM .out file" % (len(fields), repr(fields)))
        chr = fields[4]
        start = fields[5]
        end = fields[6]
        repeat_family = fields[10]
        if self.ignored_family_re.match(repeat_family):
            return ''
        return '%s\t%s\t%s\t%s\n' % (chr, start, end, repeat_family)

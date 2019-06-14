import random
import os
from subprocess import check_call

import luigi
from toil.common import Toil
from toil.job import Job

from neutral_model_estimator import NMETask

# Toil jobs for the extract-single-copy-regions phase

def extract_single_copy_regions_parallel(job, hal_file, bed_file_id, genome, chunk_size):
    """Get a BED of single-copy regions from a hal file in parallel.

    Child jobs: extract_single_copy_regions_from_chunk
    Follow-on jobs: collate_chunks"""
    bed_path = job.fileStore.readGlobalFile(bed_file_id)
    lines = []
    with open(bed_path) as bed:
        for line in bed:
            fields = line.split()
            lines.append((fields[0], fields[1], int(fields[2]) - int(fields[1])))
    chunks = []
    for i in xrange(0, len(lines), chunk_size):
        chunks.append(lines[i:i + chunk_size])
    finished_chunks = [job.addChildJobFn(extract_single_copy_regions_from_chunk, hal_file, genome, chunk).rv() for chunk in chunks]
    # Return a sorted file collating the children's output
    return job.addFollowOnJobFn(collate_chunks, finished_chunks).rv()

def extract_single_copy_regions_from_chunk(job, hal_file, genome, chunk):
    """Return a BED containing the single-copy regions from a single chunk."""
    bed = job.fileStore.getLocalTempFile()
    with open(bed, 'w') as f:
        for chr, start, length in chunk:
            check_call(["halSingleCopyRegionsExtract", hal_file, genome, "--refSequence", chr, "--start", str(start),
                        "--length", str(length)], stdout=f)
    return job.fileStore.writeGlobalFile(bed)

def collate_chunks(job, finished_chunk_ids):
    """Merge all the single-copy BED chunks into one large sorted BED file."""
    finished_chunks = [job.fileStore.readGlobalFile(chunk) for chunk in finished_chunk_ids]
    unsorted_bed = job.fileStore.getLocalTempFile()
    with open(unsorted_bed, 'w') as outf:
        for chunk in finished_chunks:
            with open(chunk) as inf:
                for line in inf:
                    outf.write(line)

    sorted_bed = job.fileStore.getLocalTempFile()
    with open(sorted_bed, 'w') as f:
        check_call(["bedtools", "sort", "-i", unsorted_bed], stdout=f)
    return job.fileStore.writeGlobalFile(sorted_bed)

# Luigi tasks

class ExtractSingleCopyRegions(NMETask):
    """Get a BED of single-copy regions from a hal file.

    Delegates to a toil pipeline to parallelize the process."""
    chunk_size = luigi.IntParameter(default=500)
    prev_task = luigi.TaskParameter()

    def requires(self):
        return self.prev_task

    def output(self):
        return self.target_in_work_dir('singleCopyRegions-%s.bed' % self.genome)

    def run(self):
        jobStorePath = '%s/jobStore-singleCopy-%s' % (self.work_dir, self.genome)
        opts = Job.Runner.getDefaultOptions(jobStorePath)
        if os.path.exists(jobStorePath):
            opts.restart = True
        opts.disableCaching = True
        opts.batchSystem = self.batchSystem
        opts.parasolCommand = self.parasolCommand
        opts.environment = ["LD_LIBRARY_PATH"]
        with Toil(opts) as toil:
            if opts.restart:
                result = toil.restart()
            else:
                bed_file = toil.importFile('file://' + self.input().path)
                result = toil.start(Job.wrapJobFn(extract_single_copy_regions_parallel,
                                                  os.path.abspath(self.hal_file), bed_file,
                                                  self.genome, self.chunk_size))
            toil.exportFile(result, 'file://' + os.path.abspath(self.output().path))

class SubsampleBed(NMETask):
    """Randomly sample only a portion of the lines from the input BED."""
    num_bases = luigi.FloatParameter()
    prev_task = luigi.TaskParameter()

    def requires(self):
        return self.prev_task

    def output(self):
        return self.target_in_work_dir('%s-sampled-%s.bed' % (self.genome, self.num_bases))

    def run(self):
        with self.input().open() as in_bed:
            bases = []
            for line in in_bed:
                fields = line.split()
                chr = fields[0]
                start = int(fields[1])
                stop = int(fields[2])
                for i in xrange(start, stop):
                    bases.append((chr, i))
            sample_size = min(self.num_bases, len(bases))
            sample = random.sample(bases, sample_size)
        with self.output().open('w') as out_bed:
            for base in sample:
                out_bed.write("\t".join([base[0], str(base[1]), str(base[1] + 1)]) + "\n")

import random
import os
from collections import namedtuple
from subprocess import check_call, check_output

import luigi
from toil.common import Toil
from toil.job import Job

from neutral_model_estimator import NMETask

# Toil jobs for the extract-single-copy-regions phase

Chunk = namedtuple('Chunk', 'start length')

def extract_single_copy_regions_parallel(job, hal_file, genome, chunk_size):
    """Get a BED of single-copy regions from a hal file in parallel.

    Child jobs: extract_single_copy_regions_from_chunk
    Follow-on jobs: collate_chunks"""
    # Get the total length of the genome: this is necessary for
    # chunking the genome
    stats_lines = [line.split(",") for line in check_output(["halStats", hal_file]).split("\n")]
    relevant_line = next((fields for fields in stats_lines if fields[0] == genome), None)
    assert relevant_line is not None, "No line for genome %s found in halStats output" % genome
    genome_length = int(relevant_line[2])
    # Chunk it up and send the chunked regions to our children
    chunks = chunk_genome(genome_length, chunk_size)
    finished_chunks = [job.addChildJobFn(extract_single_copy_regions_from_chunk, hal_file, genome, chunk).rv() for chunk in chunks]
    # Return a sorted file collating the children's output
    return job.addFollowOnJobFn(collate_chunks, finished_chunks).rv()

def chunk_genome(genome_length, chunk_size):
    """Chunks a genome of length genome_length into Chunks of at most chunk_size length.

    Helper function for extract_single_copy_regions_parallel.

    >>> chunk_genome(10, 3)
    [Chunk(start=0, length=3), Chunk(start=3, length=3), Chunk(start=6, length=3), Chunk(start=9, length=1)]"""
    chunks = []
    for i in range(0, genome_length, chunk_size):
        chunks.append(Chunk(start=i, length=min(chunk_size, genome_length - i)))
    return chunks

def extract_single_copy_regions_from_chunk(job, hal_file, genome, chunk):
    """Return a BED containing the single-copy regions from a single chunk."""
    bed = job.fileStore.getLocalTempFile()
    with open(bed, 'w') as f:
        check_call(["halSingleCopyRegionsExtract", hal_file, genome, "--start", str(chunk.start),
                    "--length", str(chunk.length)], stdout=f)
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
    chunk_size = luigi.IntParameter(default=50000)

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
                result = toil.start(Job.wrapJobFn(extract_single_copy_regions_parallel,
                                                  os.path.abspath(self.hal_file), self.genome,
                                                  self.chunk_size))
            toil.exportFile(result, 'file://' + os.path.abspath(self.output().path))

class ApplySingleCopyFilter(NMETask):
    """Restrict the bed file to be only within single-copy regions."""
    prev_task = luigi.TaskParameter()
    required_overlap = luigi.FloatParameter(default=0.5)

    def requires(self):
        return self.prev_task, self.clone(ExtractSingleCopyRegions)

    def output(self):
        return self.target_in_work_dir('%s-filtered.bed' % self.genome)

    def run(self):
        bed_file = self.input()[0].path
        single_copy_regions = self.input()[1].path
        with self.output().open('w') as f:
            check_call(["bedtools", "intersect", "-a", bed_file,
                        "-b", single_copy_regions, "-f", str(self.required_overlap)],
                       stdout=f)

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

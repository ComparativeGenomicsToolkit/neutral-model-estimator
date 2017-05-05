#!/usr/bin/env python
import luigi
import re

class GenerateAncestralRepeatsBed(luigi.Task):
    """Go from an ancestral RepeatMasker .out file to a .bed.

    The repeats are filtered to remove likely-conserved "repeats" such
    as tRNAs, low-complexity regions, etc.
    """
    rm_out_file = luigi.Parameter()

    # Matches RM .out headers and blank lines.
    header_re = re.compile(r'^$|^   SW   perc perc perc .*|score   div\. del\. .*'
                            '|There were no repetitive sequences detected')

    # We want to ignore some repeat families because they may be more conserved than others.
    ignored_family_re = re.compile(r'tRNA|SINE/tRNA.*|Low_complexity')

    def output(self):
        return luigi.LocalTarget('ancestralRepeats.bed')

    def run(self):
        with open(self.rm_out_file) as rmfile, self.output().open('w') as bedfile:
            for line in rmfile:
                if self.header_re.match(line):
                    continue
                bedfile.write(self.rm_line_to_bed_line(line))

    def rm_line_to_bed_line(self, rm_line):
        r"""Translate a RM .out line to a BED line, filtering out certain repeat families.

        >>> garb = GenerateAncestralRepeatsBed('')
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

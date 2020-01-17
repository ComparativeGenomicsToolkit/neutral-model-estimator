# neutral-model-estimator
Simple pipeline to go from an alignment to a neutral model, using the PHAST toolkit

Requires `bedtools`, `RepeatMasker`, the HAL toolkit, and Kent tools to be installed and available in your `PATH`.

Example usage:
```
PYTHONPATH=$PYTHONPATH:. luigi --module neutral_model_estimator GenerateNeutralModel --num-bases 30000 --neutral-data ancestral_repeats \
  --work-dir ~/avian_ancestral_repeats_model_rev --HalPhyloPTrain-model-type REV --genome root \
  --no-single-copy \
  --hal-file birds.hal  --local-scheduler --GenerateAncestralRepeatsBed-rm-species 'aves' \
```

The process ties together several useful tools, which could almost as easily be done manually in case you have trouble installing the pipeline.

These are:
### Generating ancestral repeats on the ancestral genome
A fasta is extracted and RepeatMasker is run on the specified ancestral genome (usually this is the root of the subtree you are interested in). As a postprocessing step, tRNAs and low complexity repeats are filtered out and the .out file is converted to a BED file containing the other repeats.
### halPhyloPTrain.py
This trains the PHAST model on a randomly sampled set of bases. (There are diminishing returns past tens of thousands of bases, depending on the size of the alignment.) Internally halPhyloPTrain.py is a very simple combination of `hal2maf` and `phyloFit`. We suggest using the `REV` model.
### Rescaling
You may be interested in the difference in rate between different sets of chromosomes (say sex vs autosomal chromosomes). There is an option within the pipeline to rescale the model to reflect the overall rate in different chromosomes. I instead suggest using `halLiftover` to lift the ancestral repeats to a genome with defined chromosomes, filtering the lifted BED to create separate BEDs for each set of interest, then using `phyloFit` to infer a new model for each set like so:
```
phyloFit --init-model <model trained above> --scale-only -o <output name> <MAF file>
```

CORPUS=c19-20180420
REF=ref
QUEUE=infiniband
CORES=5
CMEM=8
RAW=/proj/cssh/nulab/corpora

VT=/home/dasmith/src/vt-passim

spark_run = CORES=${CORES} CMEM=${CMEM} QUEUE=${QUEUE} source run-saspark-ib.sh; \
srun -p ${QUEUE} -N 1 -c 2 --mem=20G -d "after:$$SPARK_MASTER_JOBID" bash -c "$(1)"; \
scancel $$SPARK_WORKER_JOBID $$SPARK_MASTER_JOBID

slurm_run = srun -p ${QUEUE} -N 1 -c 2 bash -c "$(1)";

.SECONDARY:

%/pretty.parquet/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)))

%/pretty.csv/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)))

%/ref.parquet/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) 'ref = 1 AND size <= 2000')

%/ref.csv/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) 'ref = 1 AND size <= 2000')

%/ref.pass.json/_SUCCESS %/ref.cluster.json/_SUCCESS:	%/ref.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/proteus-cluster.py $(dir $<) $*/ref.pass.json $*/ref.cluster.json)

%-w4/out.parquet/_SUCCESS:	%-w4/input
	$(call spark_run,passim -w 4 -o 0.5 -M 0.2 --fields 'ref' --filterpairs 'gid < gid2 AND (ref = 0 OR ref2 = 0)' --input-format parquet --output-format parquet $*-w4/input $*-w4)

# $(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.reducer.maxReqsInFlight=1 --conf spark.reducer.maxBlocksInFlightPerAddress=5 --conf spark.shuffle.io.retryWait=60s --conf spark.shuffle.io.maxRetries=10'; passim -w 4 -o 0.5 -M 0.2 --fields 'ref' --filterpairs 'gid < gid2 AND (ref = 0 OR ref2 = 0)' --input-format parquet --output-format parquet $*-w4/input $*-w4)

$(REF)-%-w4/input:	idx/$(REF)-w4/dfpost.parquet/_SUCCESS idx/%-w4/dfpost.parquet/_SUCCESS
	mkdir -p $(REF)-$*-w4/input
	(cd $(REF)-$*-w4/input; ln -sf ../../$*.parquet ref=0; ln -sf ../../$(REF).parquet ref=1)
	mkdir -p $(REF)-$*-w4/dfpost.parquet
	(cd $(REF)-$*-w4/dfpost.parquet; ln -sf ../../idx/$*-w4/dfpost.parquet ref=0; ln -sf ../../idx/$(REF)-w4/dfpost.parquet ref=1)

idx/$(REF)-w4/dfpost.parquet/_SUCCESS:	$(REF).parquet/_SUCCESS
	$(RM) -r $(dir $@)
	$(call spark_run,passim --postings --minDF 1 -w 4 --input-format parquet --output-format parquet $(dir $<) $(dir $(patsubst %/,%,$(dir $@))))

idx/%-w4/dfpost.parquet/_SUCCESS:	%.parquet
	$(call spark_run,passim --postings --minDF 1 -w 4 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

#$(call spark_run,export SPARK_SUBMIT_ARGS+=' --executor-cores 2'; passim --postings --minDF 1 -w 4 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

transcript.parquet/_SUCCESS:	../Going-the-Rounds/transcriptions
	$(RM) -r $(dir $@)
	$(call slurm_run,spark-submit $(VT)/scripts/ed-load.py $< $(dir $@))

eq := =
comma := ,

serial.done:	serial/open$(eq)true/corpus$(eq)ca/_SUCCESS \
	serial/open=true/corpus=ddd/_SUCCESS \
	serial/open=true/corpus=finnish/_SUCCESS \
	serial/open=true/corpus=onb/_SUCCESS \
	serial/open=false/corpus=aps/_SUCCESS \
	serial/open=false/corpus=gale-us/_SUCCESS
	echo $<

serial/open$(eq)true/corpus$(eq)ca/_SUCCESS:	$(RAW)/chroniclingamerica/ocr
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=2000 --conf spark.sql.shuffle.partitions=32000'; $(VT)/bin/vtrun ChronAm $< $(dir $@))

serial/open$(eq)true/corpus$(eq)ddd/_SUCCESS:	$(RAW)/DDD/alto.json
	$(call spark_run,$(VT)/bin/vtrun MetsAltoDDD $(RAW)/DDD/meta.json $(RAW)/DDD/alto.json $(dir $@))

serial/open$(eq)true/corpus$(eq)finnish/_SUCCESS:	$(RAW)/finnish/every
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=50 --conf spark.sql.shuffle.partitions=50'; $(VT)/bin/vtrun MetsAltoZipped $< $(dir $@))

serial/open$(eq)true/corpus$(eq)onb/_SUCCESS:	$(RAW)/onb/xml
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=20 --conf spark.sql.shuffle.partitions=20'; $(VT)/bin/vtrun ONB $</'{dea$(comma)kro}' $(dir $@))

serial/open$(eq)false/corpus$(eq)aps/_SUCCESS:	$(RAW)/APS
	$(call spark_run,$(VT)/bin/vtrun APS $< $(VT)/data/aps.json $(dir $@))

serial/open$(eq)false/corpus$(eq)gale-us/_SUCCESS:	$(RAW)/gale/us/XML
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=200'; $(VT)/bin/vtrun NCNP $< $(VT)/data/gale.json $(dir $@))

STAMP=$(shell date +'%Y%m%d')

snapshot:	c19-${STAMP}.parquet/_SUCCESS

c19-${STAMP}.parquet/_SUCCESS:
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/c19.py serial $(dir $@))

ref.parquet/_SUCCESS:	ref/corpus$(eq)gtr/_SUCCESS ref/corpus=vac/_SUCCESS
	$(RM) -r $(dir $@)
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/merge-df.py ref $(dir $@))

ref/corpus$(eq)gtr/_SUCCESS:	../Going-the-Rounds/transcriptions
	$(RM) -r $(dir $@)
	$(call slurm_run,spark-submit $(VT)/scripts/ed-load.py $< $(dir $@))

ref/corpus$(eq)vac/_SUCCESS:	$(RAW)/wright.json
	$(RM) -r $(dir $@)
	$(call slurm_run,spark-submit $(VT)/scripts/vac-load.py $< $(dir $@))

ref/corpus$(eq)bible/_SUCCESS:
	$(call slurm_run,spark-submit $(VT)/scripts/cts-load.py ../../cts/bible $(dir $@))

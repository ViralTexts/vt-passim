REF=ref
QUEUE=short
WORKERS=40
CORES=5
CMEM=8
RAW=/work/proj_cssh/nulab/corpora

VT=/home/dasmith/src/vt-passim

# spark_run = WORKERS=${WORKERS} CORES=${CORES} CMEM=${CMEM} QUEUE=${QUEUE} source run-saspark-express.sh; \
# srun -x c0178 -p ${QUEUE} -N 1 -c 2 --mem=40G -d "after:$$SPARK_MASTER_JOBID" bash -c "$(1)"; \
# scancel $$SPARK_WORKER_JOBID $$SPARK_MASTER_JOBID

spark_run = WORKERS=${WORKERS} CORES=${CORES} CMEM=${CMEM} QUEUE=${QUEUE} source run-saspark.sh; \
srun -x c0178 --time 1-0 -p ${QUEUE} -N 1 -c 2 --mem=40G -d "after:$$SPARK_MASTER_JOBID" bash -c "$(1)"; \
scancel $$SPARK_WORKER_JOBID $$SPARK_MASTER_JOBID

slurm_run = srun -p ${QUEUE} -N 1 -c 2 bash -c "$(1)";

.SECONDARY:

%/pretty.parquet/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)))

%/pretty.csv/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)))

%/ref.parquet/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) 'ref = 1')

%/gtr.parquet/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"corpus = 'gtr'\")

%/vac.csv/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"corpus = 'vac'\")

%/wc.json/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"(series = '/lccn/sn85033995') AND (pboiler < 0.2)\")

%/ref.csv/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) 'ref = 1')

%/kossuth.csv/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"text like '%Kossuth%'\")

%/krakatoa.json/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"(text like '%Krakatoa%' OR text like '%Krakatau%') AND date >= '1883-08' AND date < '1883-11'\")

%/ref.json/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) 'ref = 1')

%/afam.json/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"corpus = 'afam' AND pboiler < 0.1\")

%/ref.pass.json/_SUCCESS %/ref.cluster.json/_SUCCESS:	%/ref.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/proteus-cluster.py $(dir $<) $*/ref.pass.json $*/ref.cluster.json)

# %-w4/out.parquet/_SUCCESS:	%-w4/input
# 	$(call spark_run,passim -w 4 -o 0.5 -M 0.2 --fields 'ref' --filterpairs 'gid < gid2 AND (ref = 0 OR ref2 = 0)' --input-format parquet --output-format parquet $*-w4/input $*-w4)

# begin quotes

quote-%-n2-w4/out.parquet/_SUCCESS:	quote-%-n2-w4/input
	$(call spark_run,passim --pairwise -a 10 -g 20 -n 2 -w 4 -o 0.5 -M 0.2 --fields 'ref' --filterpairs 'ref = 1 AND ref2 = 0' --input-format parquet --output-format json quote-$*-n2-w4/input quote-$*-n2-w4)

quote-$(REF)-%-n2-w4/input:	idx/$(REF)-n2-w4/dfpost.parquet/_SUCCESS idx/%-n2-w4/dfpost.parquet/_SUCCESS
	mkdir -p $@
	(cd $@; ln -sf ../../$*.parquet ref=0; ln -sf ../../$(REF).parquet ref=1)
	mkdir -p quote-$(REF)-$*-n2-w4/dfpost.parquet
	(cd quote-$(REF)-$*-n2-w4/dfpost.parquet; ln -sf ../../idx/$*-n2-w4/dfpost.parquet ref=0; ln -sf ../../idx/$(REF)-n2-w4/dfpost.parquet ref=1)

# end quotes

%-w4/out.parquet/_SUCCESS:	%-w4/input
	$(call spark_run,passim -w 4 -o 0.5 -M 0.2 --fields 'ref' --filterpairs 'gid < gid2 AND (ref = 0 OR ref2 = 0)' --labelPropagation --input-format parquet --output-format parquet $*-w4/input $*-w4)

$(REF)-%-w4/input:	idx/$(REF)-w4/dfpost.parquet/_SUCCESS idx/%-w4/dfpost.parquet/_SUCCESS
	mkdir -p $(REF)-$*-w4/input
	(cd $(REF)-$*-w4/input; ln -sf ../../$*.parquet ref=0; ln -sf ../../$(REF).parquet ref=1)
	mkdir -p $(REF)-$*-w4/dfpost.parquet
	(cd $(REF)-$*-w4/dfpost.parquet; ln -sf ../../idx/$*-w4/dfpost.parquet ref=0; ln -sf ../../idx/$(REF)-w4/dfpost.parquet ref=1)

idx/$(REF)-n2-w4/dfpost.parquet/_SUCCESS:	$(REF).parquet/_SUCCESS
	$(RM) -r $(dir $@)
	$(call spark_run,passim --postings --minDF 1 -n 2 -w 4 --input-format parquet --output-format parquet $(dir $<) $(dir $(patsubst %/,%,$(dir $@))))

idx/$(REF)-w4/dfpost.parquet/_SUCCESS:	$(REF).parquet/_SUCCESS
	$(RM) -r $(dir $@)
	$(call spark_run,passim --postings --minDF 1 -w 4 --input-format parquet --output-format parquet $(dir $<) $(dir $(patsubst %/,%,$(dir $@))))

idx/%-n2-w4/dfpost.parquet/_SUCCESS:	%.parquet
	$(call spark_run,passim --postings --minDF 1 -n 2 -s book -w 4 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

idx/%-w4/dfpost.parquet/_SUCCESS:	%.parquet
	$(call spark_run,passim --postings --minDF 1 -w 4 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

names/%/names.parquet/_SUCCESS:	%.parquet
	$(call spark_run,passim --names -s book --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

#$(call spark_run,export SPARK_SUBMIT_ARGS+=' --executor-cores 2'; passim --postings --minDF 1 -w 4 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

transcript.parquet/_SUCCESS:	../Going-the-Rounds/transcriptions
	$(RM) -r $(dir $@)
	$(call slurm_run,spark-submit $(VT)/scripts/ed-load.py $< $(dir $@))

eq := =
comma := ,

serial.done:	serial/open$(eq)true/corpus$(eq)ca/_SUCCESS \
	serial/open=true/corpus=bsb/_SUCCESS \
	serial/open=true/corpus=ddd/_SUCCESS \
	serial/open=true/corpus=europeana/_SUCCESS \
	serial/open=true/corpus=finnish/_SUCCESS \
	serial/open=true/corpus=moa/_SUCCESS \
	serial/open=true/corpus=onb/_SUCCESS \
	serial/open=true/corpus=sbb/_SUCCESS \
	serial/open$(eq)true/corpus$(eq)trove/_SUCCESS \
	serial/open=false/corpus=aps/_SUCCESS \
	serial/open=false/corpus=gale-uk/_SUCCESS \
	serial/open=false/corpus=gale-us/_SUCCESS \
	serial/open=false/corpus=tda/_SUCCESS
	echo $<

serial/open$(eq)true/corpus$(eq)ca/_SUCCESS:	$(RAW)/chroniclingamerica/raw ca-files.json/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=2000 --conf spark.sql.shuffle.partitions=32000'; $(VT)/bin/vtrun ChronAm $< ca-files.json $(dir $@))

serial/open$(eq)true/corpus$(eq)ddd/_SUCCESS:	$(RAW)/DDD/alto.json
	$(call spark_run,$(VT)/bin/vtrun MetsAltoDDD $(RAW)/DDD/meta.json $(RAW)/DDD/alto.json $(dir $@))

serial/open$(eq)true/corpus$(eq)europeana/_SUCCESS:	$(RAW)/europeana/newspapers-by-country
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/europeana.py $< $(dir $@))

serial/open$(eq)true/corpus$(eq)finnish/_SUCCESS:	$(RAW)/finnish/every
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=50 --conf spark.sql.shuffle.partitions=50'; $(VT)/bin/vtrun MetsAltoZipped $< $(dir $@))

serial/open$(eq)true/corpus$(eq)moa/_SUCCESS:	$(RAW)/moa/magazines
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=50'; $(VT)/bin/vtrun moa-load.py $< $(VT)/data/moa.json $(dir $@))

serial/open$(eq)true/corpus$(eq)onb/_SUCCESS:	$(RAW)/onb/xml
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=20 --conf spark.sql.shuffle.partitions=20'; $(VT)/bin/vtrun ONB $</'{dea$(comma)kro}' $(dir $@))

serial/open$(eq)true/corpus$(eq)sbb/_SUCCESS:	$(RAW)/SBB
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=50 --conf spark.sql.shuffle.partitions=50'; $(VT)/bin/vtrun MetsAltoZipped $< $(dir $@))

serial/open$(eq)true/corpus$(eq)trove/_SUCCESS:	$(RAW)/trove/issue-458484.json.bz2 $(RAW)/trove/trove-reprinted.json
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=2000'; $(VT)/bin/vtrun trove-load.py $^ $(dir $@))

serial/open$(eq)false/corpus$(eq)aps/_SUCCESS:	$(RAW)/APS
	$(call spark_run,$(VT)/bin/vtrun APS $< $(VT)/data/aps.json $(dir $@))

serial/open$(eq)false/corpus$(eq)gale-uk/_SUCCESS:	$(RAW)/gale/uk/XML
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=200'; $(VT)/bin/vtrun NCNP $< $(VT)/data/gale.json $(dir $@))

serial/open$(eq)false/corpus$(eq)gale-us/_SUCCESS:	$(RAW)/gale/us/XML
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=200'; $(VT)/bin/vtrun NCNP $< $(VT)/data/gale.json $(dir $@))

serial/open$(eq)false/corpus$(eq)tda/_SUCCESS:	$(RAW)/bl/TimesDigitalArchive_XMLS/TDAO0001
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=800'; $(VT)/bin/vtrun NCCOIssue $< $(dir $@))

STAMP=$(shell date +'%Y%m%d')

snapshot:	c19-${STAMP}.parquet/_SUCCESS

c19-${STAMP}.parquet/_SUCCESS:
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/c19.py serial $(dir $@))

jstor.parquet/_SUCCESS:	jstor.ids
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/ia-filter.py $< raw $(dir $@))

# ref.parquet/_SUCCESS:	ref/corpus=afam/_SUCCESS
ref.parquet/_SUCCESS:	ref/corpus$(eq)gtr/_SUCCESS ref/corpus=vac/_SUCCESS ref/corpus=gut/_SUCCESS
	$(RM) -r $(dir $@)
	$(call spark_run,vtrun merge-df.py ref $(dir $@))

ref/corpus$(eq)gtr/_SUCCESS:	../Going-the-Rounds/transcriptions
	$(RM) -r $(dir $@)
	$(call slurm_run,vtrun ed-load.py $< $(dir $@))

ref/corpus$(eq)vac/_SUCCESS:	$(RAW)/vac/xml
	$(RM) -r $(dir $@)
	$(call spark_run,vtrun VACBooks $< $(dir $@))

ref/corpus$(eq)bible/_SUCCESS:
	$(call slurm_run,spark-submit $(VT)/scripts/cts-load.py ../../cts/bible $(dir $@))

ddd.json/_SUCCESS:	$(RAW)/DDD/meta.json
	$(call spark_run,$(VT)/bin/vtrun MetsMetaDDD $(RAW)/DDD/meta.json $(dir $@))

meta/lccn.json:	$(RAW)/chroniclingamerica/lccn
	$(call slurm_run,$(VT)/bin/vtrun LCMerge $< $(VT)/data/default-lccn-places.json $(VT)/data/dbpedia-canonical.json lccn.json)

# meta/aps.json:	$(RAW)/APS
# 	$(RM) -r aps.json
# 	$(call spark_run,$(VT)/bin/vtrun APSMeta $< $(VT)/data/aps.json aps.json)
# 	cat aps.json/part* > $@

all-series.out/_SUCCESS:	serial.done
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/series-list.py serial $(dir $@))

all-series.lis:	all-series.out/_SUCCESS
	sort all-series.out/part* > $@

lccn.done:	all-series.lis
	grep '^/lccn/' $< | perl -pe 's/$$/.rdf/' | \
	    wget -N -B https://chroniclingamerica.loc.gov -i - -P $(RAW)/chroniclingamerica/lccn

ca-files.json/_SUCCESS:	$(RAW)/chroniclingamerica/batches
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/ca-image-ids.py '$</*/manifest*.txt' $(dir $@))

tcp-pre1800/out.parquet/_SUCCESS:	tcp-pre1800/input
	$(call spark_run,passim --docwise -w 4 -s book --fields 'gold' --filterpairs 'gid <> gid2 AND gold2 = 0' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%/llspikes.csv/_SUCCESS:	%/prettylang.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/term-spikes.py $*/prettylang.parquet $*/llspikes.csv)

%/prettylang.parquet/_SUCCESS:	%/pretty.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/langid.py $*/pretty.parquet $*/prettylang.parquet)

%/gtr.pages.json/_SUCCESS:	%/gtr.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/gtr-cluster.py $(dir $<) $(dir $@))

%/gtr.html:	%/gtr.pages.json/_SUCCESS
	$(call slurm_run,python $(VT)/scripts/gtr-pages.py $(dir $<) $@)

srcgut.out/dfpost.parquet/_SUCCESS:	srcgut
	$(call spark_run,passim -s book --postings -n 2 -w 5 --fields src --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

srcgut.out/out.parquet/_SUCCESS:	srcgut
	$(call spark_run,passim -s book -n 2 -w 5 --fields src --filterpairs 'src = 1 AND src2 = 0' --pairwise --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

srcgut-a10.out/out.parquet/_SUCCESS:	srcgut
	$(call spark_run,passim -s series -a 10 -n 2 -w 5 --fields src --filterpairs 'src = 1 AND src2 = 0' --pairwise --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

srcgut-a10-u1000.out/out.parquet/_SUCCESS:	srcgut
	$(call spark_run,passim -s series -a 10 -n 5 -w 4 -u 1000 --fields src --filterpairs 'src = 1 AND src2 = 0' --pairwise --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

src-pre1800.out/align.parquet/_SUCCESS:	src-pre1800
	$(call spark_run,passim -s book -n 2 -w 5 --fields src --filterpairs 'src = 1 AND src2 = 0' --pairwise --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

srcgut.out/slim.parquet/_SUCCESS:	srcgut.out/full-extents.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS fanpage.py srcgut.out/full-extents.parquet names/srcgut/names.parquet $(dir $@))

%-dw.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/passim --docwise --linewise -w 2 --gap 300 --min-lines 3 --fields 'date(date) as day;coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 180) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))' --metaFields 'series;date' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-dw.out/out.json/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields 'date(date) as day' 'xxhash64(series) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 180) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))' --input-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-net.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields 'date(date) as day' 'xxhash64(series) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 180) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-dref.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields ref 'date(date) as day' 'xxhash64(series) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(ref = 0) AND ((ref2 = 1) OR ((uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 180) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))))' -n 25 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-mag.out/out.json/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields \"(corpus = 'aps' OR corpus = 'moa') as mag\" --filterpairs 'mag AND NOT mag2' --input-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-ra.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields ref --filterpairs 'ref = 1 AND ref2 = 0' -n 25 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-coll.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/passim --docwise --min-lines 4 --gap 500 -n 5 --openGap 4 --fields 'date(date) as day' --filterpairs '(uid <> uid2) AND (abs(datediff(day, day2)) < 32)' --metaFields 'date;series' --input-format parquet --output-format parquet $< $*-coll.out)

%-age.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/passim --docwise --min-lines 4 --gap 500 -n 5 --openGap 4 --fields 'date(date) as day' --filterpairs 'day < day2' --metaFields 'date;series' --input-format parquet --output-format parquet $< $*-age.out)

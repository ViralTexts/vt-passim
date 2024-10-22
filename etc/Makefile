REF=ref
QUEUE=short
WORKERS=40
CORES=5
CMEM=8
RAW=/work/proj_cssh/nulab/corpora
NRAW=/work/nulab/corpora
SWORKERS=10
SMEM=40

N=25
M=5

VT=/home/dasmith/src/vt-passim

# spark_run = WORKERS=${WORKERS} CORES=${CORES} CMEM=${CMEM} QUEUE=${QUEUE} source run-saspark-express.sh; \
# srun -x c0178 -p ${QUEUE} -N 1 -c 2 --mem=40G -d "after:$$SPARK_MASTER_JOBID" bash -c "$(1)"; \
# scancel $$SPARK_WORKER_JOBID $$SPARK_MASTER_JOBID

spark_run = WORKERS=${WORKERS} CORES=${CORES} CMEM=${CMEM} QUEUE=${QUEUE} source run-saspark.sh; \
srun -x c0178,c3085 --time 1-0 -p ${QUEUE} -N 1 -c 2 --mem=40G -d "after:$$SPARK_MASTER_JOBID" bash -c "$(1)"; \
scancel $$SPARK_WORKER_JOBID $$SPARK_MASTER_JOBID

slurm_run = srun --time 1-0 -p ${QUEUE} -N 1 -c $(SWORKERS) --mem=$(SMEM)G bash -c "SPARK_SUBMIT_ARGS='--driver-memory $(SMEM)G --executor-memory $(SMEM)G' $(1)";

.SECONDARY:

%/pretty.parquet/_SUCCESS:	%/out.parquet/_SUCCESS places.csv/_SUCCESS
	$(call spark_run,vtrun pretty-cluster.py --places places.csv meta $(dir $<) $(patsubst %/,%,$(dir $@)))

%/pretty.json/_SUCCESS:	%/out.parquet/_SUCCESS places.csv/_SUCCESS
	$(call spark_run,vtrun pretty-cluster.py --places places.csv meta $(dir $<) $(patsubst %/,%,$(dir $@)))

%/pretty.csv/_SUCCESS:	%/out.parquet/_SUCCESS places.csv/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000 --conf spark.driver.maxResultSize=12G'; $(VT)/bin/pretty-cluster --places places.csv meta $(dir $<) $(patsubst %/,%,$(dir $@)))

%/ref.parquet/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,$(VT)/bin/pretty-cluster --places places.csv meta $(dir $<) $(patsubst %/,%,$(dir $@)) 'ref = 1')

%/gtr.parquet/_SUCCESS:	%/out.parquet/_SUCCESS places.csv/_SUCCESS
	$(call spark_run,$(VT)/bin/pretty-cluster --places places.csv meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"corpus = 'gtr'\")

%/gtr.json/_SUCCESS:	%/out.parquet/_SUCCESS places.csv/_SUCCESS
	$(call slurm_run,$(VT)/bin/pretty-cluster --places places.csv meta $(dir $<) $(patsubst %/,%,$(dir $@)) \"corpus = 'gtr'\")

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

serial/_SUCCESS:	serial/open$(eq)true/corpus$(eq)ca/_SUCCESS \
	serial/open=true/corpus=bsb/_SUCCESS \
	serial/open=true/corpus=ddd/_SUCCESS \
	serial/open=true/corpus=europeana/_SUCCESS \
	serial/open=true/corpus=finnish/_SUCCESS \
	serial/open=true/corpus=moa/_SUCCESS \
	serial/open=true/corpus=digitalnc/_SUCCESS \
	serial/open=true/corpus=oregonnews/_SUCCESS \
	serial/open=true/corpus=panewsarchive/_SUCCESS \
	serial/open=true/corpus=nebnewspapers/_SUCCESS \
	serial/open=true/corpus=onb/_SUCCESS \
	serial/open=true/corpus=sbb/_SUCCESS \
	serial/open=true/corpus=trove/_SUCCESS \
	serial/open=false/corpus=aps/_SUCCESS \
	serial/open=false/corpus=gale-uk/_SUCCESS \
	serial/open=false/corpus=gale-us/_SUCCESS \
	serial/open=false/corpus=tda/_SUCCESS \
	serial/open=true/corpus=ia/_SUCCESS
	touch $@

ca-raw/_SUCCESS:	$(patsubst %.tar.bz2,ca-raw/batch=%/_SUCCESS,$(notdir $(wildcard $(RAW)/chroniclingamerica/raw/*)))
	touch $@

ca-raw/batch$(eq)%/_SUCCESS:	$(RAW)/chroniclingamerica/raw/%.tar.bz2
	$(call slurm_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.adaptive.enabled=true'; vtrun ChronAm $< $(dir $@))

gale-us-raw/_SUCCESS:	$(RAW)/gale/us/XML
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=200'; vtrun NCNP $< $(VT)/data/gale.json $(dir $@))

serial/open$(eq)true/corpus$(eq)ca/_SUCCESS:	ca-raw/_SUCCESS ca-mets.json/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=2000 --conf spark.sql.shuffle.partitions=8000 --conf spark.sql.adaptive.enabled=true'; vtrun ca-page-merge.py ca-raw ca-mets.json $(dir $@))

# serial/open$(eq)true/corpus$(eq)ca/_SUCCESS:	$(RAW)/chroniclingamerica/raw ca-files.json/_SUCCESS
# 	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=2000 --conf spark.sql.shuffle.partitions=8000 --conf spark.sql.adaptive.enabled=true'; $(VT)/bin/vtrun ChronAm $< ca-files.json $(dir $@))

serial/open$(eq)true/corpus$(eq)ddd/_SUCCESS:	$(RAW)/DDD/alto.json
	$(call spark_run,vtrun MetsAltoDDD $(RAW)/DDD/meta.json $(RAW)/DDD/alto.json $(dir $@))

serial/open$(eq)true/corpus$(eq)europeana/_SUCCESS:	$(RAW)/europeana/newspapers-by-country
	$(call spark_run,vtrun europeana.py $< $(dir $@))

serial/open$(eq)true/corpus$(eq)finnish/_SUCCESS:	$(RAW)/finnish/every
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=50 --conf spark.sql.shuffle.partitions=50'; vtrun MetsAltoZipped $< $(dir $@))

serial/open$(eq)true/corpus$(eq)ia/_SUCCESS:	../ia/periodicals.parquet ../ia/kynp.parquet
	$(call spark_run,vtrun recode-series.py ia-maps '../ia/{periodicals$(comma)kynp}.parquet' $(dir $@))

serial/open$(eq)true/corpus$(eq)moa/_SUCCESS:	$(RAW)/moa/magazines
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=50'; vtrun moa-load.py $< $(VT)/data/moa.json $(dir $@))

serial/open$(eq)true/corpus$(eq)onb/_SUCCESS:	$(RAW)/onb/xml
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=20 --conf spark.sql.shuffle.partitions=20'; vtrun ONB $</'{dea$(comma)kro}' $(dir $@))

serial/open$(eq)true/corpus$(eq)sbb/_SUCCESS:	$(RAW)/SBB
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=50 --conf spark.sql.shuffle.partitions=50'; vtrun MetsAltoZipped $< $(dir $@))

serial/open$(eq)true/corpus$(eq)trove/_SUCCESS:	$(RAW)/trove/issue-458484.json.bz2 $(RAW)/trove/trove.json
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=2000'; vtrun trove-load.py $^ $(dir $@))

serial/open$(eq)false/corpus$(eq)aps/_SUCCESS:	$(RAW)/APS $(VT)/data/aps.json
	$(call spark_run,vtrun APS $< $(VT)/data/aps.json $(dir $@))

serial/open$(eq)false/corpus$(eq)gale-uk/_SUCCESS:	$(RAW)/gale/uk/XML
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=200'; vtrun NCNP $< $(VT)/data/gale.json $(dir $@))

serial/open$(eq)false/corpus$(eq)gale-us/_SUCCESS:	gale-us-raw/_SUCCESS
	$(call spark_run,vtrun fix-dates.py $(dir $<) $(VT)/data/gale.json $(VT)/data/gale-date.json $(dir $@))

serial/open$(eq)false/corpus$(eq)tda/_SUCCESS:	$(RAW)/bl/TimesDigitalArchive_XMLS/TDAO0001
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.default.parallelism=800'; vtrun NCCOIssue $< $(dir $@))

serial/open$(eq)true/corpus$(eq)digitalnc/_SUCCESS:	ndnp-raw/digitalnc/_SUCCESS ndnp-mets/digitalnc/_SUCCESS
	$(call spark_run,vtrun ndnp-page-merge.py --urlocr $(dir $^) $(dir $@))

serial/open$(eq)true/corpus$(eq)%/_SUCCESS:	ndnp-raw/%/_SUCCESS ndnp-mets/%/_SUCCESS
	$(call spark_run,vtrun ndnp-page-merge.py $(dir $^) $(dir $@))

STAMP=$(shell date +'%Y%m%d')

snapshot:	c19-${STAMP}.parquet/_SUCCESS

c19-${STAMP}.parquet/_SUCCESS:
	$(call spark_run,vtrun c19.py serial marc-meta/groups $(dir $@))

vrt-snapshot:	vrt-${STAMP}.parquet/_SUCCESS

vrt-${STAMP}.parquet/_SUCCESS:
	$(call spark_run,vtrun c19.py --min-year 1863 --max-year 1922 serial marc-meta/groups $(dir $@))

c1869.parquet/_SUCCESS:
	$(call spark_run,vtrun c19.py --min-year 1868 --max-year 1870 serial marc-meta/groups $(dir $@))

jstor.parquet/_SUCCESS:	jstor.ids
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/ia-filter.py $< raw $(dir $@))

# ref.parquet/_SUCCESS:	ref/corpus=afam/_SUCCESS
ref.parquet/_SUCCESS:	ref/open$(eq)true/corpus$(eq)gtr/_SUCCESS #ref/corpus=vac/_SUCCESS ref/corpus=gut/_SUCCESS
	$(call slurm_run,vtrun merge-df.py ref $(dir $@))

#  ref/corpus=vac/_SUCCESS ref/corpus=gut/_SUCCESS
ref/_SUCCESS:	ref/open$(eq)true/corpus$(eq)gtr/_SUCCESS
	touch $@

ref/open$(eq)true/corpus$(eq)gtr/_SUCCESS:	../transcriptions
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

marc.parquet/_SUCCESS:	$(RAW)/chroniclingamerica/marc-raw.xml
	$(call slurm_run,vtrun marc-parse-xml.py $< $(dir $@))

marc-meta/_SUCCESS:	marc.parquet/_SUCCESS
	$(call slurm_run,vtrun marc-meta.py $(dir $<) $(dir $@))
	touch $@

marc-meta/groups/_SUCCESS:	marc-meta/_SUCCESS
	$(call slurm_run,vtrun series-group.py marc-meta/meta marc-meta/links marc-meta)

# meta/aps.json:	$(RAW)/APS
# 	$(RM) -r aps.json
# 	$(call spark_run,$(VT)/bin/vtrun APSMeta $< $(VT)/data/aps.json aps.json)
# 	cat aps.json/part* > $@

# all-series.out/_SUCCESS:	serial/_SUCCESS
# 	$(call spark_run,$(VT)/bin/vtrun series-list.py serial $(dir $@))

# all-series.lis:	all-series.out/_SUCCESS
# 	jq -r -c '.series' all-series.out/part* | sort -u > $@

stats-series.parquet/_SUCCESS:	serial/_SUCCESS
	$(call spark_run,vtrun series-stats.py serial $(dir $@))

dig-series.csv/_SUCCESS:	serial/_SUCCESS
	$(call spark_run,vtrun series-range.py serial $(dir $@))

pp-series.csv/_SUCCESS:	stats-series.parquet/_SUCCESS
	$(call slurm_run,vtrun series-pages.py $(dir $<) $(dir $@))

output-series.csv/_SUCCESS:	stats-series.parquet/_SUCCESS
	$(call slurm_run,vtrun series-output.py $(dir $<) $(dir $@))

freq.csv/_SUCCESS:	serial/_SUCCESS
	$(call spark_run,vtrun test-freq.py serial $(dir $@))

meta-series/_SUCCESS:	dig-series.csv/_SUCCESS
	$(call slurm_run,vtrun series-meta.py meta $(dir $<) $(patsubst %/,%,$(dir $@)))

dbpedia-sameas.parquet/_SUCCESS:	$(NRAW)/dbpedia/sameAs_lang$(eq)en.ttl.bz2
	$(call spark_run,vtrun ntriples.py $< $(dir $@))

wikidata.parquet/_SUCCESS:	$(NRAW)/wikidata/latest-all.json.bz2
	$(call spark_run,vtrun wd-load.py $< $(dir $@))

places.csv/_SUCCESS:	meta-series/_SUCCESS dbpedia-sameas.parquet/_SUCCESS wikidata.parquet/_SUCCESS
	$(call slurm_run,vtrun place-info.py $(dir $^) $(dir $@))

lccn.done:	all-series.lis
	grep '^/lccn/' $< | perl -pe 's/$$/.rdf/' | \
	    wget -N -B https://chroniclingamerica.loc.gov -i - -P $(RAW)/chroniclingamerica/lccn

ca-files.json/_SUCCESS:	$(RAW)/chroniclingamerica/batches
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/ca-image-ids.py '$</*/manifest*.txt' $(dir $@))

ca-mets.json/_SUCCESS:	$(RAW)/chroniclingamerica/mets
	$(call spark_run,vtrun mets-merge.py \"$(RAW)/chroniclingamerica/{mets$(comma)miss}\" $(dir $@))

ndnp-mets/%/_SUCCESS:	$(NRAW)/%/mets
	$(call slurm_run,vtrun mets-merge.py $< $(dir $@))

ndnp-raw/%/_SUCCESS:	$(NRAW)/%/alto
	$(call spark_run,vtrun ndnp-alto.py \"$</*.warc.gz\" $(dir $@))

tcp-pre1800/out.parquet/_SUCCESS:	tcp-pre1800/input
	$(call spark_run,passim --docwise -w 4 -s book --fields 'gold' --filterpairs 'gid <> gid2 AND gold2 = 0' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%/llspikes.csv/_SUCCESS:	%/prettylang.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/term-spikes.py $*/prettylang.parquet $*/llspikes.csv)

%/prettylang.parquet/_SUCCESS:	%/pretty.parquet/_SUCCESS
	$(call spark_run,spark-submit $$SPARK_SUBMIT_ARGS $(VT)/scripts/langid.py $*/pretty.parquet $*/prettylang.parquet)

%/gtr.pages.json/_SUCCESS:	%/gtr.parquet/_SUCCESS
	$(call slurm_run,vtrun gtr-cluster.py --filter \"ref = 0 OR corpus = 'gtr'\" $(dir $<) $(dir $@))

%/pretty.pages.json/_SUCCESS:	%/pretty.parquet/_SUCCESS
	$(call spark_run,vtrun gtr-cluster.py --filter 'size >= 10' $(dir $<) $(dir $@))

%/pretty.md:	%/pretty.pages.json/_SUCCESS
	$(call slurm_run,zcat $(dir $<)/part* | python $(VT)/scripts/gtr-split.py $@)

#%.split:	%.json
#	python $(VT)/scripts/gtr-split.py $(dir $(patsubst %/,%,$(dir $@)))/pages.html < $< && touch $@

#%/pages.html/_SUCCESS:	%/pretty.pages.json/_SUCCESS
#	ls -1 $(dir $<) | xargs -L 1 -P python $(VT)/scripts/gtr-split.py

%/gtr.html:	%/gtr.pages.json/_SUCCESS
	$(call slurm_run,python $(VT)/scripts/gtr-pages.py $(dir $<) $@)

%/gtr.md:	%/gtr.pages.json/_SUCCESS
	$(call slurm_run,zcat $(dir $<)/part* | python $(VT)/scripts/gtr-split.py $@)

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
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim -n 25 --fields 'date(date) as day' 'xxhash64(series) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 180) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-tok.parquet/_SUCCESS:	%.parquet/_SUCCESS
	$(call spark_run,vtrun doc-words.py $(dir $<) gut.txt $(dir $@))

%.parquet/_SUCCESS:	%.txt
	$(call spark_run,vtrun series-filter.py serial/open$(eq)true/corpus$(eq)ca/ $< $(dir $@))

gut.txt/_SUCCESS:	../gutenberg/pg.parquet/_SUCCESS
	$(call spark_run,vtrun gut-words.py --min-count 40 $(dir $<) $(dir $@))

## Max 26-year (= 9497 day) lag
%-age.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --log-level INFO --beam 20 -n 25 --fields 'date(date) as day' 'xxhash64(group) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 9497) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

N=20
M=5
%-n$(N)m$(M).out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --log-level INFO --beam 20 -n $(N) -m $(M) --fields 'date(date) as day' 'xxhash64(group) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 9497) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

## Max 1-year lag
%-ym.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --log-level INFO --beam 20 -n 25 --fields 'date(date) as day' 'xxhash64(group) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 367) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-raym.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --log-level INFO --beam 20 -n 25 --fields ref 'date(date) as day' 'xxhash64(group) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND (ref2 = 0) AND ((ref = 1) OR ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 367) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2)))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

# 3 years (to account for books)
%-bp.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --log-level INFO --beam 20 -n 25 --fields ref 'date(date) as day' 'xxhash64(group) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND (ref2 <> 1) AND ((ref <> 0 AND ) OR ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 1096) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2)))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-rd.out/out.json/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --docwise --all-pairs --log-level INFO --beam 20 -n 25 --fields 'date(date) as day' 'xxhash64(group) as gid' --filterpairs \"(uid <> uid2) AND (gid2 = xxhash64('/lccn/sn84024738')) AND (abs(datediff(day2,day)) <= 366)\" --input-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-lag.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --log-level INFO --beam 20 -n 25 --fields day daylag 'xxhash64(group) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 9497) OR (gid = gid2 AND ((date_sub(day2, daylag2) = day AND day < day2) OR (day = day2 AND ed < ed2))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%.out/src.csv/_SUCCESS:	%.out/out.parquet/_SUCCESS
	$(call spark_run,vtrun series-src.py $(dir $<) meta $(dir $@))

%.out/clage.csv/_SUCCESS:	%.out/pretty.parquet/_SUCCESS %.out/cluster-period.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-ages.py marc-meta/families $(dir $^) $(subst .,-,$(basename $(subst -,.,$*))).parquet $(subst .,-,$(basename $(subst -,.,$*)))-tok.parquet $(dir $@))

%.out/clage-t08.csv/_SUCCESS:	%.out/pretty.parquet/_SUCCESS %.out/cluster-period.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-ages.py --lex-prop 0.8 marc-meta/families $(dir $^) $(subst .,-,$(basename $(subst -,.,$*))).parquet $(subst .,-,$(basename $(subst -,.,$*)))-tok.parquet $(dir $@))

%.out/clage.docs/_SUCCESS:	%.out/pretty.parquet/_SUCCESS %.out/cluster-period.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-ages.py --docwise marc-meta/families $(dir $^) $(subst .,-,$(basename $(subst -,.,$*))).parquet $(subst .,-,$(basename $(subst -,.,$*)))-tok.parquet $(dir $@))

#$(call spark_run,vtrun cluster-ages.py $(dir $<) $*.out/cluster-period.parquet $(subst .,-,$(basename $(subst -,.,$*))).parquet $(dir $@))

%.out/clage7.csv/_SUCCESS:	%.out/pretty.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-ages.py -s 7 marc-meta/families $(dir $<) $*.out/cluster-period.parquet $(subst .,-,$(basename $(subst -,.,$*))).parquet $(dir $@))

%.out/clage-ilag30.csv/_SUCCESS:	%.out/pretty.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-ages.py -s 30 $(dir $<) $(subst .,-,$(basename $(subst -,.,$*))).parquet $(dir $@))

%-age.out/clage60.csv/_SUCCESS:	%-age.out/pretty.parquet/_SUCCESS
	$(call spark_run,export SPARK_SUBMIT_ARGS+=' --conf spark.sql.shuffle.partitions=8000'; vtrun cluster-ages.py -s 60 $(dir $<) $*.parquet $(dir $@))

%-dref.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields ref 'date(date) as day' 'xxhash64(series) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(ref = 0) AND ((ref2 = 1) OR ((uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 180) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))))' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

#$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields ref 'date(date) as day' 'xxhash64(series) as gid' 'coalesce(int(ed),0) as ed' --filterpairs '(ref = 0) AND ((ref2 = 1) OR ((uid <> uid2) AND ((gid <> gid2 AND day < day2 AND datediff(day2, day) < 180) OR (gid = gid2 AND ((day < day2 AND datediff(day2,day) < 32) OR (day = day2 AND ed < ed2))))))' -n 25 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-mag.out/out.json/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --fields \"(corpus = 'aps' OR corpus = 'moa') as mag\" --filterpairs 'mag AND NOT mag2' --input-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-ra.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --log-level INFO --beam 20 --complete-lines --fields ref --filterpairs '(ref = 1) AND (ref2 < 1)' --src-overlap 0 --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-coll.out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --docwise --log-level INFO -n 25 -m 5 --complete-lines --fields ref --filterpairs 'ref = 1 AND ref2 = 0' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

%-coll-n$(N)m$(M).out/out.parquet/_SUCCESS:	%.parquet
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --docwise --log-level INFO -n $(N) -m $(M) --beam 100 --floating-ngrams --complete-lines --fields ref --filterpairs 'ref = 1 AND ref2 = 0' --input-format parquet --output-format parquet $< $(dir $(patsubst %/,%,$(dir $@))))

tcp-%/lines.json/_SUCCESS:	tcp-%/out.parquet/_SUCCESS
	$(call spark_run,/home/dasmith/src/acdc_train/bin/acdc-run wit-lines.py --fields book -- $(dir $<) $(dir $@))

%/lines.json/_SUCCESS:	%/out.parquet/_SUCCESS
	$(call spark_run,/home/dasmith/src/acdc_train/bin/acdc-run wit-lines.py --fields series date -- $(dir $<) $(dir $@))

# add corpus field to above

%/ca50.json/_SUCCESS:	%/lines.json/_SUCCESS
	$(call spark_run,vtrun line-sample.py --lines 50 $(dir $<) $(dir $@))

%/nyt10k.json/_SUCCESS:	%/lines.json/_SUCCESS
	$(call spark_run,vtrun line-sample.py --filter \"matchRate > 0.5 AND leadGap = 0 AND tailGap = 0 AND maxGap < 4 AND length = dstLength AND (series = '/lccn/sn83030746' OR series = '/lccn/sn78004456')\" --lines 5000 $(dir $<) $(dir $@))

%/ca10k.json/_SUCCESS:	%/lines.json/_SUCCESS
	$(call spark_run,vtrun line-sample.py --lines 10000 $(dir $<) $(dir $@))

%.err:	%.img
	wget -r -N -np -nH -P $(RAW)/chroniclingamerica -B https://chroniclingamerica.loc.gov/data/batches/ -o $@ -i $<

%/ca50-alto/alto.lis:	%/ca50.json/_SUCCESS
	$(call slurm_run,/home/dasmith/src/acdc_train/bin/acdc-run gen-alto.py --scale --base $(RAW)/chroniclingamerica/data/batches/ $(dir $<) $(dir $@); find $(dir $@) -type f -name '*.xml' > $@)

%/ca10k-alto/${series}/alto.lis:	%/ca10k.json/_SUCCESS
	$(call slurm_run,/home/dasmith/src/acdc_train/bin/acdc-run gen-alto.py --scale --base $(RAW)/chroniclingamerica/data/batches/ $*/ca10k.json/series${eq}%2Flccn%2F${series} $(dir $@); find $(dir $@) -type f -name '*.xml' > $@)


%/dataset.arrow:        %/alto.lis
	$(call slurm_run,ketos compile --workers $(SWORKERS) --ignore-splits --random-split 0.9 0.1 0.0 -o $@ -f alto -F $<)

%-general.parquet/_SUCCESS:	%-general.merge/_SUCCESS
	$(call spark_run,vtrun c19.py $(dir $<) marc-meta/groups $(dir $@))

%-general.merge/_SUCCESS:	%-general.ocr/_SUCCESS
	mkdir -p $(dir $@)
	(cd $(dir $@); ln -s ../$*.parquet reocr${eq}0)
	$(call spark_run,vtrun merge-ocr.py $(dir $<) ca-mets.json $(dir $@)/reocr${eq}1)
	touch $@

c1869-general.ocr/_SUCCESS:	$(subst redo.parquet,general.ocr,$(wildcard c1869-redo.parquet/series*))
	touch $@

c1869-general.ocr/%:	c1869-redo.parquet/%
	mkdir -p $(dir $@)
	$(call spark_run,vtrun reocr-lines.py --base /work/proj_cssh/nulab/corpora/chroniclingamerica/data/batches $< models/ca50-sc_best.mlmodel $@)

s08-general.ocr/_SUCCESS:	$(subst .parquet,-general.ocr,$(wildcard s08.parquet/year*/month*))
	touch $@

s08-general.ocr/%:	s08.parquet/%
	mkdir -p $(dir $@)
	$(call spark_run,vtrun reocr-lines.py --base https://chroniclingamerica.loc.gov/data/batches/ $< models/ca50-sc_best.mlmodel $@)


c1869-ca10k.ocr/_SUCCESS:	$(subst redo.parquet,ca10k.ocr,$(wildcard c1869-redo.parquet/series*))
	touch $@

c1869-ca10k.ocr/%:	c1869-redo.parquet/%
	mkdir -p $(dir $@)
	$(call spark_run,vtrun reocr-lines.py $< models/ca10k-$(word 3,$(subst 2F, ,$*))-sc_best.mlmodel $@)

%/series-map/_SUCCESS:	%/tmp/extents.parquet/_SUCCESS
	$(call spark_run,vtrun issue-overlap.py $(dir $<) $*/input $(dir $@))

aps-ia/tmp/extents.parquet/_SUCCESS:	aps-ia/input
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --to-extents --fields ref 'date(date) as day' --filterpairs 'ref = 1 AND ref2 = 0 AND day = day2' --input-format parquet --output-format parquet $< aps-ia)

gale-ia/tmp/extents.parquet/_SUCCESS:	gale-ia/input
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --to-extents --fields ref 'date(date) as day' --filterpairs 'ref = 1 AND ref2 = 0 AND day = day2' --input-format parquet --output-format parquet $< gale-ia)

moa-ia/tmp/extents.parquet/_SUCCESS:	moa-ia/input
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --to-extents --fields ref 'date(date) as day' --filterpairs 'ref = 1 AND ref2 = 0 AND day = day2' --input-format parquet --output-format parquet $< moa-ia)

usnd.json/_SUCCESS:
	$(call spark_run,vtrun ca-dump.py serial $(dir $@))

ca.csv/_SUCCESS:	serial/open$(eq)true/corpus$(eq)ca/_SUCCESS
	$(call spark_run,vtrun series-range.py $(dir $<) $(dir $@))

gale-us.csv/_SUCCESS:	serial/open$(eq)false/corpus$(eq)gale-us/_SUCCESS
	$(call spark_run,vtrun series-range.py $(dir $<) $(dir $@))

%/gtr-period.csv/_SUCCESS:	%/gtr.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-period.py $(dir $<) $(dir $@))

%/cluster-period.parquet/_SUCCESS:	%/pretty.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-period.py --size 2 $(dir $<) $(dir $@))

%/cluster-spread.csv/_SUCCESS:	%/cluster-period.parquet/_SUCCESS
	$(call slurm_run,vtrun cluster-spread.py $(dir $<) $(dir $@))

%-links.csv/_SUCCESS:	%.gz.parquet/_SUCCESS
	$(call spark_run,vtrun cascade-links.py $(dir $<) $(dir $@))

%-lags.csv/_SUCCESS:	%.gz.parquet/_SUCCESS
	$(call spark_run,vtrun cascade-lags.py $(dir $<) $(dir $@))

%-paths.csv/_SUCCESS:	%.gz.parquet/_SUCCESS
	$(call slurm_run,vtrun cascade-paths.py $(dir $<) $(dir $@))

%-gpaths.csv/_SUCCESS:	%.gz.parquet/_SUCCESS
	$(call spark_run,vtrun cascade-paths.py --year $(dir $<) $(dir $@))

%.gz.parquet/_SUCCESS:	%.gz
	$(call spark_run,vtrun cascade-features.py --posteriors $< $(dir $<))

%/casc-2s.model/iter020.gz:
	$(call spark_run,vtrun cascade-features.py --input $*/pretty.parquet --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --witness-fields 'coverage AS city$(comma) topdiv AS state$(comma) country$(comma) series$(comma) year(date) AS year$(comma) nreturn$(comma) issues$(comma) lag75p' --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(year2 AS string) AS ys$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'series + series2 + city*city2 + sameseries + samecity + samestate + samecountry + lagbin + city:ys + city2:ys' $(dir $@))

SEED=42
ITER=20

%/casc-2s-r69.model/s$(SEED)-iter100.gz:
	$(call spark_run,vtrun cascade-features.py --iterations 100 --seed $(SEED) --rate 5 --input $*/pretty.parquet --series-stats rowell-1869.json --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --witness-fields 'coverage AS city$(comma) topdiv AS state$(comma) country$(comma) interest$(comma) circulation$(comma) subscription$(comma) series$(comma) year(date) AS year$(comma) nreturn$(comma) issues$(comma) lag75p$(comma) CAST(floor(log10(circulation)) AS string) AS circmag$(comma) CAST(subscription > 2 AS string) AS pricey' --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(year2 AS string) AS ys$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'pricey*pricey2 + circmag*circmag2 + interest*interest2 + series*series2 + city*city2 + sameseries + samecity + samestate + samecountry + lagbin + city:ys + city2:ys' $(dir $@))

%/casc-2s-r69-noseries.model/s$(SEED)-iter100.gz:
	$(call spark_run,vtrun cascade-features.py --iterations 100 --seed $(SEED) --rate 5 --input $*/pretty.parquet --series-stats rowell-1869.json --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --witness-fields 'coverage AS city$(comma) topdiv AS state$(comma) country$(comma) interest$(comma) circulation$(comma) subscription$(comma) series$(comma) year(date) AS year$(comma) nreturn$(comma) issues$(comma) lag75p$(comma) CAST(floor(log10(circulation)) AS string) AS circmag$(comma) CAST(subscription > 2 AS string) AS pricey' --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(year2 AS string) AS ys$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'pricey*pricey2 + circmag*circmag2 + interest*interest2 + city*city2 + sameseries + samecity + samestate + samecountry + lagbin' $(dir $@))

%/casc-2s-interact.model/s$(SEED)-iter100.gz:
	$(call spark_run,vtrun cascade-features.py --iterations 100 --seed $(SEED) --rate 10 --input $*/pretty.parquet --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --witness-fields 'coverage AS city$(comma) topdiv AS state$(comma) country$(comma) series$(comma) year(date) AS year$(comma) nreturn$(comma) issues$(comma) lag75p' --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(year2 AS string) AS ys$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'series*series2 + city*city2 + sameseries + samecity + samestate + samecountry + lagbin + city:ys + city2:ys' $(dir $@))

%/casc-2s-split.model/iter0$(ITER).gz:
	$(call spark_run,vtrun cascade-features.py --iterations $(ITER) --rate 10 --input $*/split.parquet --filter 'size < 1000' --witness-fields \"coverage AS city$(comma) topdiv AS state$(comma) country$(comma) series$(comma) syear$(comma) daylag$(comma) (CASE WHEN daylag < 2 THEN 'daily' WHEN daylag < 8 THEN 'weekly' ELSE 'longer' END) AS freq\" --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(syear AS string) AS ys$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'series + series2 + city*city2 + state*state2 + country*country2 + sameseries + samecity + samestate + samecountry + lagbin + city:ys + city2:ys' $(dir $@))

#$(call spark_run,vtrun cascade-features.py --iterations $(ITER) --rate 10 --input $*/pretty.parquet --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --witness-fields \"coverage AS city$(comma) topdiv AS state$(comma) country$(comma) series$(comma) year(date) AS year$(comma) nreturn$(comma) issues$(comma) lag75p$(comma) daylag$(comma) (CASE WHEN daylag < 2 THEN 'daily' WHEN daylag < 8 THEN 'weekly' ELSE 'longer' END) AS freq\" --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(year2 AS string) AS ys$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'series*series2 + city*city2 + state*state2 + country*country2 + sameseries + samecity + samestate + samecountry + lagbin + city:ys + city2:ys' $(dir $@))
#	$(call spark_run,vtrun cascade-features.py --iterations $(ITER) --rate 10 --input $*/pretty.parquet --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --witness-fields \"coverage AS city$(comma) topdiv AS state$(comma) country$(comma) series$(comma) year(date) AS year$(comma) nreturn$(comma) issues$(comma) lag75p$(comma) daylag$(comma) (CASE WHEN daylag < 2 THEN 'daily' WHEN daylag < 8 THEN 'weekly' ELSE 'longer' END) AS freq\" --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'series*series2 + city*city2 + state*state2 + country*country2 + sameseries + samecity + samestate + samecountry + lagbin' $(dir $@))

%/casc-2s-place.model/s$(SEED)-iter100.gz:
	$(call spark_run,vtrun cascade-features.py --iterations 100 --seed $(SEED) --rate 10 --input $*/pretty.parquet --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --witness-fields 'coverage AS city$(comma) topdiv AS state$(comma) country$(comma) series$(comma) year(date) AS year$(comma) nreturn$(comma) issues$(comma) lag75p' --pair-fields '(country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity' --formula 'city*city2 + samecity + samestate + samecountry' $(dir $@))

%/casc-2s-series.model/s$(SEED)-iter100.gz:
	$(call spark_run,vtrun cascade-features.py --iterations 100 --seed $(SEED) --rate 10 --input $*/pretty.parquet --cluster-stats $*/cluster-period.parquet --filter 'groupR0 > 1 AND issues < 1000' --pair-fields '(series <=> series2) AS sameseries' --formula 'series*series2 + sameseries' $(dir $@))

%/group-period.csv/_SUCCESS:	%/pretty.parquet/_SUCCESS
	$(call spark_run,vtrun cluster-group-period.py $(dir $<) $(dir $@))

%/links.csv/_SUCCESS:	%/tmp/extents.parquet
	$(call spark_run,vtrun place-net.py $< meta $(dir $@))

# %-vrt.out/_SUCCESS:	%.lis
# 	$(call spark_run,vtrun series-dump.py $< $(lastword $(wildcard vrt-*.parquet)) $(dir $@))

%-wsp.json/_SUCCESS:	%.parquet/_SUCCESS
	$(call spark_run,vtrun series-dump.py wsp.lis $(dir $<) $(dir $@))

%/split.parquet/_SUCCESS:	%/pretty.parquet/_SUCCESS
	$(call spark_run,vtrun cascade-split.py $(dir $<) $(dir $@))

# ITER=20

%.ymod/iter0$(ITER).gz:
	$(call slurm_run,vtrun cascade-features.py --iterations $(ITER) --input $(dir $(patsubst %/,%,$(dir $*)))split.parquet/syear$(eq)$(notdir $*) --filter 'size < 1000' --witness-fields \"coverage AS city$(comma) topdiv AS state$(comma) country$(comma) series$(comma) daylag$(comma) (CASE WHEN daylag < 2 THEN 'daily' WHEN daylag < 8 THEN 'weekly' ELSE 'longer' END) AS freq\" --pair-fields '(series <=> series2) AS sameseries$(comma) (country <=> country2) AS samecountry$(comma) (state <=> state2) AS samestate$(comma) (city <=> city2) AS samecity$(comma) CAST(CAST(log1p(abs(lag)) AS int) AS string) AS lagbin' --formula 'series*series2 + city*city2 + state*state2 + country*country2 + sameseries + samecity + samestate + samecountry + lagbin' $(dir $@))

YEARS=$(shell seq 1840 1899)
#YEARS=1840 1845 1850 1855 1859 1860 1861 1862 1863 1864 1865 1866 1867 1868 1869 1870 1875 1880 1881 1882 1883 1884 1885 1890 1895 1899

%/split-interact.model/_SUCCESS:	%/split.parquet/_SUCCESS
	$(MAKE) $(foreach year,$(YEARS),$(dir $@)$(year).ymod/iter0$(ITER)-paths.csv/_SUCCESS)

rdd6065-n7m5.out/out.parquet/_SUCCESS:	rdd6065.parquet/_SUCCESS
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --docwise --log-level INFO --beam 100 -n 7 -m 5 --fields ref 'date(date) as day' --filterpairs '(ref = 1) AND (ref2 = 0) AND (day = day2)' --input-format parquet --output-format parquet $(dir $<) $(dir $(patsubst %/,%,$(dir $@))))

rdd6065-ca-n7m5.out/out.parquet/_SUCCESS:	rdd6065-ca.parquet/_SUCCESS
	$(call spark_run,/home/dasmith/src/dev/passim/bin/seriatim --docwise --log-level INFO --beam 100 -n 7 -m 5 --fields ref 'date(date) as day' --filterpairs '(ref = 1) AND (ref2 = 0) AND (day = day2)' --input-format parquet --output-format parquet $(dir $<) $(dir $(patsubst %/,%,$(dir $@))))

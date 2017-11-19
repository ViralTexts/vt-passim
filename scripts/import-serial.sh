VT=/home/dasmith/src/vt-passim
# SERIAL=hdfs://discovery3:9000/user/dasmith/corpora/serial
SERIAL=/home/dasmith/work/corpora/vt/serial
CORP=/proj/cssh/nulab/corpora
# TEST=hadoop fs -test
TEST=test

out=$SERIAL/open=true/corpus=onb
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=20" \
		     vtrun ONB $CORP/'onb/xml/{dea,kro}' $out >& onb.err
fi

out=$SERIAL/open=true/corpus=europeana
if ! $TEST -f $out/_SUCCESS; then
    spark-submit $SPARK_SUBMIT_ARGS $VT/scripts/europeana.py \
		 $CORP/europeana/newspapers-by-country $out >& europeana.err
fi

out=$SERIAL/open=true/corpus=finnish
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50" \
		     vtrun MetsAlto $CORP/finnish/every $out >& finnish.err
fi

# SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50" \
# vtrun MetsAlto /proj/cssh/nulab/corpora/HSB $SERIAL/serial/open=true/corpus=hsb >& hsb.err

out=$SERIAL/open=true/corpus=sbb
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50" \
		     vtrun MetsAlto $CORP/SBB $out  >& sbb.err
fi

out=$SERIAL/open=true/corpus=moa
if ! $TEST -f $out/_SUCCESS; then
    spark-submit $SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50 \
		 --packages com.databricks:spark-xml_2.11:0.4.0 \
		 $VT/scripts/moa-load.py $CORP/moa/magazines $VT/data/moa.json $out >& moa.err
fi

out=$SERIAL/open=false/corpus=aps
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=200" \
		     vtrun APS $CORP/APS $VT/data/aps.json $out >& aps.err
fi

out=$SERIAL/open=false/corpus=gale-uk
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=200" \
		     vtrun NCNP "$CORP/Gale - 19c British Newspapers/XML" $out >& gale-uk.err
fi

out=$SERIAL/open=false/corpus=gale-us
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=200" \
		     vtrun NCNP "$CORP/Gale - 19c US Newspapers/XML/" $out >& gale-us.err
fi

out=$SERIAL/open=false/corpus=tda
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=800" \
		     vtrun NCCOIssue "$CORP/bl/TimesDigitalArchive_XMLS/TDAO0001/TDAO0001-C00000/Newspapers/0FFO" $out >& tda.err
fi

## While using hadoop 2.4.x, non-thread-safe bzip2 requires a single core per executor

out=$SERIAL/open=true/corpus=trove
if ! $TEST -f $out/_SUCCESS; then
    spark-submit $SPARK_SUBMIT_ARGS $VT/scripts/trove-load.py \
		 $CORP/trove/issue-458484.json.bz2 $out >& trove.err
fi

out=$SERIAL/open=true/corpus=ca
if ! $TEST -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=2000" \
		     vtrun ChronAm $CORP/chroniclingamerica/raw \
		     $out >& ca.err
fi


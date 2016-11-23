VT=/home/dasmith/src/vt-passim
SERIAL=hdfs://discovery3:9000/user/dasmith/corpora/serial

out=$SERIAL/open=true/corpus=onb/batch=dea-kro
if ! hadoop fs -test -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=20" \
		     vtrun ONB '/proj/cssh/nulab/corpora/onb/xml/{dea,kro}' $out >& onb.err
fi

out=$SERIAL/open=true/corpus=europeana/batch=wget-20160303
if ! hadoop fs -test -f $out/_SUCCESS; then
    spark-submit $SPARK_SUBMIT_ARGS $VT/scripts/europeana.py \
		 /proj/cssh/nulab/corpora/europeana/newspapers-by-country $out >& europeana.err
fi

out=$SERIAL/open=true/corpus=finnish/batch=Digilib-pub_1771_1874_every
if ! hadoop fs -test -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50" \
		     vtrun MetsAlto /proj/cssh/nulab/corpora/finnish/every $out >& finnish.err
fi

# SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50" \
# vtrun MetsAlto /proj/cssh/nulab/corpora/HSB $SERIAL/serial/open=true/corpus=hsb >& hsb.err

out=$SERIAL/open=true/corpus=sbb/batch=SBB-1-8
if ! hadoop fs -test -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50" \
		     vtrun MetsAlto /proj/cssh/nulab/corpora/SBB $out  >& sbb.err
fi

out=$SERIAL/open=true/corpus=moa/batch=cornell
if ! hadoop fs -test -f $out/_SUCCESS; then
    spark-submit $SPARK_SUBMIT_ARGS --conf spark.default.parallelism=50 \
		 --packages com.databricks:spark-xml_2.11:0.4.0 \
		 $VT/scripts/moa-load.py \
		 /proj/cssh/nulab/corpora/moa/magazines $VT/data/moa.json $out >& moa.err
fi

out=$SERIAL/open=true/corpus=trove/batch=issue-458484
# spark-submit $SPARK_SUBMIT_ARGS $VT/scripts/trove-load.py \
#     /proj/cssh/nulab/corpora/trove/issue-458484.json.bz2 \
#     $out >& trove.err

out=$SERIAL/open=false/corpus=aps/batch=APSTextOnly_20150507
if ! hadoop fs -test -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=200" \
		     vtrun APS '/proj/cssh/nulab/corpora/APS' $VT/data/aps.json $out >& aps.err
fi

out=$SERIAL/open=false/corpus=gale-uk/batch=BLC-14-16
if ! hadoop fs -test -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=200" \
		     vtrun NCNP '/proj/cssh/nulab/corpora/Gale - 19c British Newspapers/XML' \
		     $out >& gale-uk.err
fi

out=$SERIAL/open=false/corpus=gale-us/batch=NCNP-1-25
if ! hadoop fs -test -f $out/_SUCCESS; then
    SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS --conf spark.default.parallelism=200" \
		     vtrun NCNP '/proj/cssh/nulab/corpora/Gale - 19c US Newspapers/XML/' \
		     $out >& gale-us.err
fi


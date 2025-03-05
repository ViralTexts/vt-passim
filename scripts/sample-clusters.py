import argparse, sys
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as f

def main(config):
    spark = SparkSession.builder.appName('Sample Clusters').getOrCreate()

    raw = spark.read.load(config.inputPath).filter(config.filter)

    if not config.group or config.group == '':
        pop = raw.groupBy('cluster').agg(
            f.min(f.struct('date', 'uid'))['uid'].alias('uid'))
        pop.cache()

        total = pop.count()

        sample_fraction = max(min(config.samples / total, 1.0), 0.0)

        samples = pop.sample(fraction=sample_fraction).limit(config.samples)
    else:
        pop = raw.groupBy('cluster'
                ).agg(f.min(f.struct('date', 'uid', f.expr(config.group).alias('group'))).alias('info')
                ).select('cluster', 'info.*')
        pop.cache()

        fractions = {r['group']:(config.samples / r['count'])
                     for r in pop.groupBy('group').count().collect()}
        samples = pop.sampleBy('group', fractions=fractions)

    samples.cache()

    jfields = ['cluster', 'uid']
    if config.all:
        jfields = ['cluster']
    
    raw.join(samples, jfields, 'left_semi'
            ).repartition(1).sort('date', 'begin'
            ).coalesce(1).write.csv(config.outputPath,
                                    escape='"',
                                    header=True)
    
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sample Clusters',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-a', '--all', action='store_true', help='Output all reprints')
    parser.add_argument('-f', '--filter', type=str, default='size >= 10 AND pboiler < 0.2',
                        help='SQL query for reprints')
    parser.add_argument('-g', '--group', type=str, default='', help='Group for stratification')
    parser.add_argument('-s', '--samples', type=int, default=1000,
                        help='Number of samples')
    parser.add_argument('inputPath', metavar='<path>', help='input path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()
    print(config)
    main(config)

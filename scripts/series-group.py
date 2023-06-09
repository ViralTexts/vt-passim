import argparse
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.functions import (col, collect_set, explode, lit, size, udf, struct, length)
from pyspark.sql.functions import regexp_replace as rr
import pyspark.sql.functions as f
from graphframes import GraphFrame

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Cluster series',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('metaPath', metavar='<path>', help='meta path')
    parser.add_argument('linkPath', metavar='<path>', help='link path')
    parser.add_argument('outputPath', metavar='<path>', help='output path')

    config = parser.parse_args()

    spark = SparkSession.builder.appName('Cluster series').getOrCreate()
    spark.conf.set('spark.sql.adaptive.enabled', 'true')
    spark.sparkContext.setCheckpointDir(config.outputPath + '/tmp')

    meta = spark.read.csv(config.metaPath, header=True, escape='"')
    link = spark.read.csv(config.linkPath, header=True, escape='"')

    spark.conf.set('spark.sql.shuffle.partitions', meta.rdd.getNumPartitions())

    nodes = meta.withColumnRenamed('series', 'id'
                ).withColumn('start', col('startDate').cast('int')
                ).withColumn('end', col('endDate').cast('int'))

    cand = link.filter((col('type') == '780') & (col('subtype') == '0')
              ).select(col('link').alias('src'), col('series').alias('dst')
              ).union(link.filter((col('type') == '785') & (col('subtype') == '0')
                         ).select(col('series').alias('src'), col('link').alias('dst'))
              ).distinct()

    edges = GraphFrame(nodes, cand
        ).find('(a)-[e]->(b)'
        ).filter('(a.endDate = b.startDate OR a.end IS NULL OR b.start IS NULL) AND a.city = b.city AND a.frequency = b.frequency'
        ).select(col('e.*')
        ).groupBy('dst'
        ).agg(f.min('src').alias('src'), f.count('src').alias('count')
        ).filter(col('count') == 1
        ).groupBy('src'
        ).agg(f.min('dst').alias('dst'), f.count('dst').alias('count')
        ).filter(col('count') == 1
        ).drop('count')

    g = GraphFrame(nodes, edges)
    g.cache()

    cc = g.connectedComponents()

    groups = cc.join(g.inDegrees, ['id'], 'left_outer'
        ).join(g.outDegrees, ['id'], 'left_outer'
        ).na.fill(0, subset=['inDegree', 'outDegree']
        ).groupBy('component'
        ).agg(f.min(struct('inDegree', 'id'))['id'].alias('group'),
              collect_set('id').alias('series')
        ).select('group', explode('series').alias('series')
        ).filter(col('group') != col('series'))
    groups.cache()

    groups.sort('group', 'series'
        ).coalesce(1).write.csv(config.outputPath + '/groups',
                                header=True, escape='"', mode='overwrite')
    g.unpersist()

    fgraph = GraphFrame(meta.select(col('series').alias('id')),
                        link.filter(col('type') == '775'
                            ).select(col('link').alias('src'), col('series').alias('dst')
                            ).union(groups.select(col('series').alias('src'),
                                                  col('group').alias('dst'))
                            ).distinct())
    fgraph.cache()

    fgraph.connectedComponents(
         ).groupBy('component'
         ).agg(f.min('id').alias('family'), collect_set('id').alias('series')
         ).select('family', explode('series').alias('series')
         ).filter(col('family') != col('series')
         ).sort('family', 'series'
         ).coalesce(1).write.csv(config.outputPath + '/families',
                                  header=True, escape='"', mode='overwrite')

    # nodes.coalesce(1).write.json(config.outputPath + '/v')
    # edges.coalesce(1).write.json(config.outputPath + '/e')

    spark.stop()

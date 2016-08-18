package vtpassim;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * TarballInputFormat.
 *
 * The input tarball file is set as non-splittable.
 * The key/value generation delegated to {@link TarballReader}
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 27 Jun 2014
 * @since 1.6.x
 */
public class TarballInputFormat extends FileInputFormat<TarballEntry, Text> {

    public TarballInputFormat() {
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
    
    @Override
    public RecordReader<TarballEntry, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new TarballReader();
    }
}

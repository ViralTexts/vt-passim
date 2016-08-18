package vtpassim;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;

/**
 * TarballReader.
 *
 * Outputs for file included in a tarball a key/value pair where the key is
 * the file name appended with date and time (.DYYMMDD.THHMMSS) and the value
 * is the content of the file.
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 27 Jun 2014
 * @since 1.6.x
 */
public class TarballReader extends RecordReader<TarballEntry, Text> {
    
    private static final Log LOG = LogFactory.getLog(TarballReader.class);
    
    private long pos = 0;

    private long end = 0;
    
    private String tarball = null;
    
    private TarInputStream in = null;
    
    private TarballEntry key = null;

    private Text value = null;

    public TarballReader() {}

    protected TarballReader(String tarball) throws IOException {
        InputStream in = this.getClass().getResourceAsStream(tarball);
        GZIPInputStream gzip = new GZIPInputStream(in);
        TarInputStream tar = new TarInputStream(gzip);
        
        this.in = tar;
        this.key = new TarballEntry();
        this.value = new Text();
        this.tarball = tarball;
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            key = null;
            value = null;
        }
    }

    @Override
    public synchronized boolean nextKeyValue() throws IOException {
        TarEntry tarEntry = in.getNextEntry();
        while (tarEntry != null && tarEntry.isDirectory()) tarEntry = in.getNextEntry();
        if (tarEntry == null) return false;
        
        // clear K/V
        key.clear();
        value.clear();
        
        Calendar timestamp = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        timestamp.setTimeInMillis(tarEntry.getModTime().getTime());
        
        key.setTarball(tarball);
        key.setEntry(tarEntry);

        // read tar entry
        long tarSize = tarEntry.getSize();
        if (tarSize > Integer.MAX_VALUE) throw new IOException("tar entry " + tarEntry.getName() + " exceeds " + Integer.MAX_VALUE);
        
        int bufSize = (int) tarSize;
        int read = 0;
        int offset = 0;
        byte[] buffer = new byte[bufSize];
        
        while ((read = in.read(buffer, offset, bufSize)) != -1) offset += read;
        
        // set value
        value.set(buffer);
        
        // set pos
        pos += bufSize;
        
        LOG.debug("read " + key);
        
        return true;
    }

    @Override
    public synchronized TarballEntry getCurrentKey() {
        return key;
    }

    @Override
    public synchronized Text getCurrentValue() {
        return value;
    }

    @Override
    public synchronized float getProgress() throws IOException {
        return Math.min(1.0f, pos / (float) end);
    }

    @Override
    public void initialize(InputSplit isplit, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            pos = 0;
            end = Long.MAX_VALUE;
            key = new TarballEntry();
            value = new Text();

            FileSplit split = (FileSplit) isplit;
            Path file = split.getPath();
            tarball = file.getName();
            
            Configuration conf = context.getConfiguration();
            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
            CompressionCodec codec = compressionCodecs.getCodec(file);

            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream fileIn = fs.open(split.getPath());
            
            in = new TarInputStream(codec.createInputStream(fileIn));
        }
        catch (IOException ex) {
            Logger.getLogger(TarballReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

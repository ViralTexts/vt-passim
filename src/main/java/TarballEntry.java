package vtpassim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.kamranzafar.jtar.TarEntry;

/**
 * TarballEntry.
 *
 * Holds some tarball information. To be used as key.
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 27 Jun 2014
 * @since 1.6.x
 */
public class TarballEntry implements WritableComparable<TarballEntry> {

    private String tarball = null;
    
    private String entry = null;
    
    private long modtime = 0L;
    
    /**
     * Default constructor required by Hadoop.
     */
    public TarballEntry() {}
    
    public void clear() {
        tarball = null;
        entry = null;
        modtime = 0L;
    }

    @Override
    public int compareTo(TarballEntry o) {
        int comp = this.tarball.compareTo(o.tarball);
        if (comp != 0) return comp;
        
        comp = this.entry.compareTo(o.entry);
        if (comp != 0) return comp;
        
        if (this.modtime < o.modtime) return -1;
        else if (this.modtime == o.modtime) return 0;
        else return 1;
    }
    
    public String getEntry() {
        return entry;
    }
    
    public long getModTime() {
        return modtime;
    }
    
    public String getTarball() {
        return tarball;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tarball = in.readUTF();
        this.entry = in.readUTF();
        this.modtime = in.readLong();
    }
    
    public void setEntry(TarEntry entry) {
        this.entry = entry.getName();
        this.modtime = entry.getModTime().getTime();
    }

    public void setTarball(String tarball) {
        this.tarball = tarball;
    }

    @Override
    public String toString() {
        return tarball + "/" + entry + "/" + modtime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tarball);
        out.writeUTF(entry);
        out.writeLong(modtime);
    }
}

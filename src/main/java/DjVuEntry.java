package vtpassim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * DjVuEntry.
 *
 * Holds some DjVu information. To be used as key.
 *
 */
public class DjVuEntry implements WritableComparable<DjVuEntry> {

    private String id = null;
    
    private int seq = 0;

    private long pos = 0L;
    
    /**
     * Default constructor required by Hadoop.
     */
    public DjVuEntry() {}
    
    public void clear() {
        id = null;
        seq = 0;
	pos = 0L;
    }

    @Override
    public int compareTo(DjVuEntry o) {
        int comp = this.id.compareTo(o.id);
        if (comp != 0) return comp;
        
        if (this.seq < o.seq) return -1;
        else if (this.seq != o.seq) return 1;

	if (this.pos < o.pos) return -1;
	else if (this.pos == o.pos) return 0;
	else return 1;
    }

    public long getPos() {
	return pos;
    }

    public int getSeq() {
        return seq;
    }
    
    public String getID() {
        return id;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.seq = in.readInt();
	this.pos = in.readLong();
    }

    public void setPos(long pos) {
	this.pos = pos;
    }
    
    public void setID(String id) {
        this.id = id;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    @Override
    public String toString() {
        return id + "_" + String.format("%04d", seq);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeInt(seq);
	out.writeLong(pos);
    }
}

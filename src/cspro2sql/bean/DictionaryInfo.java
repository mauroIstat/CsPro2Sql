package cspro2sql.bean;

import java.io.PrintStream;
import java.util.Arrays;

public class DictionaryInfo {

    public static enum Status {
        STOP(0),
        RUNNING(1);
        private final int status;

        private Status(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    };

    private final int id;
    private final String name;
    private final Status status;
    private final int revision;
    private final int total;
    private final int loaded;
    private final byte[] lastGuid;
    private final int nextRevision;

    public DictionaryInfo(int id, String name, int status, int revision, int total, int loaded, byte[] lastGuid, int nextRevision) {
        this.id = id;
        this.name = name;
        this.status = (status == 1) ? Status.RUNNING : Status.STOP;
        this.revision = revision;
        this.total = total;
        this.loaded = loaded;
        this.lastGuid = (lastGuid == null ? null : Arrays.copyOf(lastGuid, lastGuid.length));
        this.nextRevision = nextRevision;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Status getStatus() {
        return status;
    }

    public int getRevision() {
        return revision;
    }

    public int getTotal() {
        return total;
    }

    public int getLoaded() {
        return loaded;
    }

    public byte[] getLastGuid() {
        return lastGuid == null ? null : Arrays.copyOf(lastGuid, lastGuid.length);
    }

    public int getNextRevision() {
        return nextRevision;
    }

    public boolean isRunning() {
        return status == Status.RUNNING;
    }

    public void print(PrintStream out) {
        out.println("Name: " + name);
        out.println("Revision: " + revision);
        out.println("Loader status: " + (status.name()));
        if (status == Status.RUNNING) {
            out.println("Load to revision: " + nextRevision);
            out.println("Loaded: " + loaded);
            out.println("Total: " + total);
        }
    }

}

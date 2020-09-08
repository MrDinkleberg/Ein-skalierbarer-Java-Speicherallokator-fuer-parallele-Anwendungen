// Interface zur Implementierung des Zugriffs auf den OffHeap.
// Die Funktionen sollen dazu dienen dazu, Primitive Datentypen/Objekte in den OffHeap zu schreiben oder zu lesen.


public interface OffHeapAccess {

    void writeInt(long address, int value);

    void writeLong(long address, long value);

    void writeDouble(long address, double value);

    void writeChar(long address, char value);

    void writeByte(long address, byte value);

    void writeShort(long address, short value);

    void writeFloat(long address, float value);

    void writeBoolean(long address, boolean value);

    int readInt(long address);

    long readLong(long address);

    double readDouble(long address);

    char readChar(long address);

    byte readByte(long address);

    short readShort(long address);

    float readFloat(long address);

    boolean readBoolean(long address);

    void freeMemory(long address);
}

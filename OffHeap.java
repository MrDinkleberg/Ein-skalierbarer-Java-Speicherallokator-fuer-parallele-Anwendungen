import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class OffHeap implements OffHeapAccess {

    public long startaddress;   //Adresse des OffHeaps
    private long size;          //Groesse des OffHeaps


    public Unsafe unsafe;

    public OffHeap(long size) throws NoSuchFieldException, IllegalAccessException {
        this.unsafe = initUnsafe();
        this.size = size;
        this.startaddress = this.unsafe.allocateMemory(size);
        unsafe.setMemory(this.startaddress, this.size, (byte) 0);
    }



    private Unsafe initUnsafe() throws NoSuchFieldException, IllegalAccessException {  //Funktion zur Initialisierung der Unsafe Instanz
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (Unsafe) f.get(null);
    }

    @Override
    public void freeMemory(long address){
        unsafe.freeMemory(address);
    }   //Gibt den allozierten Speicher frei

    @Override
    public void writeInt(long address, int value){        //Schreibt die übergebene Integer-Variable in den OffHeap
        unsafe.putInt(address, value);
    }

    @Override
    public void writeLong(long address, long value){      //Schreibt die übergebene Long-Variable in den OffHeap
        unsafe.putLong(address, value);
    }

    @Override
    public void writeDouble(long address, double value){      //Schreibt die übergebene Double-Variable in den OffHeap
        unsafe.putDouble(address, value);
    }

    @Override
    public void writeChar(long address, char value){      //Schreibt die übergebene Char-Variable in den OffHeap
        unsafe.putChar(address, value);
    }

    @Override
    public void writeByte(long address, byte value){      //Schreibt die übergebene Byte-Variable in den OffHeap
        unsafe.putByte(address, value);
    }

    @Override
    public void writeShort(long address, short value){        //Schreibt die übergebene Short-Variable in den OffHeap
        unsafe.putShort(address, value);
    }

    @Override
    public void writeFloat(long address, float value){        //Schreibt die übergebene Float-Variable in den OffHeap
        unsafe.putFloat(address, value);
    }

    @Override
    public void writeBoolean(long address, boolean value){        //Schreibt die übergebene Boolean-Variable in den OffHeap
        unsafe.putBoolean(null, address, value);
    }

    @Override
    public int readInt(long address){   //Gibt die an der übergebenen Adresse gespeicherte Integer-Variable zurueck
        return unsafe.getInt(address);
    }

    @Override
    public long readLong(long address){ //Gibt die an der übergebenen Adresse gespeicherte Integer-Variable zurueck
        return unsafe.getLong(address);
    }

    @Override
    public double readDouble(long address){ //Gibt die an der übergebenen Adresse gespeicherte Double-Variable zurueck
        return unsafe.getDouble(address);
    }

    @Override
    public char readChar(long address){ //Gibt die an der übergebenen Adresse gespeicherte Char-Variable zurueck
        return unsafe.getChar(address);
    }

    @Override
    public byte readByte(long address){ //Gibt die an der übergebenen Adresse gespeicherte Byte-Variable zurueck
        return unsafe.getByte(address);
    }

    @Override
    public short readShort(long address){   //Gibt die an der übergebenen Adresse gespeicherte Short-Variable zurueck
        return unsafe.getShort(address);
    }

    @Override
    public float readFloat(long address){   //Gibt die an der übergebenen Adresse gespeicherte Float-Variable zurueck
        return unsafe.getFloat(address);
    }

    @Override
    public boolean readBoolean(long address){   //Gibt die an der übergebenen Adresse gespeicherte Boolean-Variable zurueck
        return unsafe.getBoolean(null, address);
    }

}

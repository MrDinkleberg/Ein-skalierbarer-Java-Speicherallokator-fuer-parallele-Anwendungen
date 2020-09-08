import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MemoryManager {

    public static final int ADDRESS_SIZE = 5;
    public static final int MAX_BLOCK_SIZE = 16000000;



    private final OffHeapAccess offHeapAccess;
    private final long addressoffset;
    private final long offheapsize;
    private final int segments;
    private final int initblocksize;
    public SegmentHeader[] segmentlist;

    public MemoryManager(long size, int segments, int initblocksize) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        this.initblocksize = Math.min(initblocksize, MAX_BLOCK_SIZE);           //legt die Groesse der Blocks bei der Initialisierung fest
        this.offheapsize = Math.max(size, segments * (initblocksize + 2));
        this.offHeapAccess = new OffHeap(offheapsize);
        addressoffset = ((OffHeap) offHeapAccess).startaddress;                 //Offset fuer virtuelle Adressen
        segmentlist = new SegmentHeader[segments];
        this.segments = segments;
        createSegments(segments);
    }

    public MemoryManager(long size, int segments) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        this.offheapsize = Math.max(size, segments * (MAX_BLOCK_SIZE + 2));
        this.initblocksize = MAX_BLOCK_SIZE;
        this.offHeapAccess = new OffHeap(offheapsize);
        addressoffset = ((OffHeap) offHeapAccess).startaddress;
        segmentlist = new SegmentHeader[segments];
        this.segments = segments;
        createSegments(segments);
    }

    public void createSegments(int segments) throws InterruptedException {
        long segmentsize = offheapsize / segments;
        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        long segmentstart = 0;

        for(int i = 0; i < segments; i++){
            SegmentHeader segment = new SegmentHeader(segmentstart, segmentsize);
            segmentstart += (segmentsize + 1);
            segmentlist[i] = segment;
            es.submit(() -> initSegment(segment));              //startet parallele Initialisierung der Segmente
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.HOURS);
    }

    private void initSegment(SegmentHeader segment){
        long stamp = segment.lock.writeLock();
        try {
            long address = segment.startaddress;
            //Pointer für Verkettung der freien Bloecke
            long previousblock = 0;
            long nextblock = address + (initblocksize + 2);

            int list = segment.findExactBlockList(initblocksize);
            segment.setListAnchor(list, address + 1);  //setzt Anker fuer Freispeicherliste
            byte markervalue = getFreeBlockMarkerValue(initblocksize);

            while (segment.endaddress - address >= initblocksize + 1) {    //erstellt freie Bloecke maximaler Groesse und verkettet sie
                writeMarkerLowerBits(address, markervalue);
                address++;
                createFreeBlock(address, initblocksize, nextblock, previousblock);
                previousblock = address;
                if(segment.endaddress - address >= (initblocksize + 1) * 2){    //wenn der vorletzte Block erreicht wird, wird der naechste Block auf 0 gesetzt
                    nextblock = address + (initblocksize + 1);
                } else {
                    nextblock = 0;
                }
                address += initblocksize;
                writeMarkerUpperBits(address, markervalue);
            }

            int remainingsize = (int) ((segment.endaddress - 1) - address);     //erstellt einen Block aus dem restlichen Speicher
            if (remainingsize > 0 && address + remainingsize < segment.endaddress) {
                byte remainingmarker = getFreeBlockMarkerValue(remainingsize);
                if(remainingmarker == 15){                                      //uebriger Speicher ist 1B gross
                    writeMarkerLowerBits(address, remainingmarker);
                    writeMarkerUpperBits(address + 1, remainingmarker);
                }
                else if(remainingmarker == 0){                                  //uebriger Speicher ist < 12B gross
                    writeMarkerLowerBits(address, remainingmarker);
                    address++;
                    writeLengthField(address, remainingsize, 1);
                    address += remainingsize;
                    writeLengthField(address - 1, remainingsize, 1);
                    writeMarkerUpperBits(address, remainingmarker);
                }
                else {
                    writeMarkerLowerBits(address, remainingmarker);
                    address++;
                    int blocklist = segment.findExactBlockList(remainingsize);
                    createFreeBlock(address, remainingsize, 0, 0);
                    changeListAnchor(segment, blocklist, address);
                    address += remainingsize;
                    writeMarkerUpperBits(address, remainingmarker);
                }
            }
        } finally {
            segment.lock.unlockWrite(stamp);
        }
    }



    public long allocate(Serializable object) throws IOException {
        byte[] objectbytearray = serialize(object);                 //serialisiert das Objekt in ein Byte Array
        byte usedmarkervalue = getUsedBlockMarkerValue(objectbytearray.length);       //Markerwert des allozierten Speichers
        int lengthfieldsize = usedmarkervalue - 8;  //berechnet Laengenfeld fuer belegten Block
        int size = objectbytearray.length + 2 * lengthfieldsize;
        long address;

        SegmentHeader segment = findSegment();
        long stamp = segment.lock.writeLock();
        try {
            segment.usedspace += size;
            int blocklist = segment.findFittingBlockList(size);
            address = segment.getListAnchor(blocklist);             //Suche nach passendem freien Block
            address = findFittingBlockInList(size, address);
            while(address == 0){
                blocklist++;
                address = findFittingBlockInList(size, segment.getListAnchor(blocklist));
            }
            if(address == segment.getListAnchor(blocklist)){        //Falls Block erstes Element der Liste ist wird der Nachfolger neuer Anker
                long nextfreeblock = getNextFreeBlock(address);
                if(nextfreeblock != 0) {
                    int freelengthfieldsize = readMarkerLowerBits(nextfreeblock - 1);
                    writeAddressField(nextfreeblock + freelengthfieldsize + ADDRESS_SIZE, 0);
                }
                segment.setListAnchor(blocklist, nextfreeblock);
            } else {
                removeBlockFromFreeBlockList(address);
            }
            int blocklengthfield = readMarkerLowerBits(address - 1);
            int blocksize = readLengthField(address, blocklengthfield);    //Groesse des angeforderten Blocks
            int newblocksize = blocksize - size - 1;                       //Groesse des neuen freien Blocks

            long newblockaddress = address + size + 1;                  //Adresse des neuen freien Blocks

            //Daten werden in den Block geschrieben
            writeMarkerLowerBits(address - 1, usedmarkervalue);
            writeLengthField(address, objectbytearray.length, lengthfieldsize);
            writeByteArray(address + lengthfieldsize, objectbytearray);
            writeLengthField(address + lengthfieldsize + objectbytearray.length, objectbytearray.length, lengthfieldsize);
            writeMarkerUpperBits(address + size, usedmarkervalue);


            if(newblocksize > 0) {
                cutFreeBlock(segment, newblockaddress, newblocksize); //erstellt aus ueberschuessigem Speicher neuen freien Block
            }
        } finally {
            segment.lock.unlockWrite(stamp);
        }

        return address;


    }

    public long allocateSerialized(byte[] object) {
        byte usedmarkervalue = getUsedBlockMarkerValue(object.length);       //Markerwert des allozierten Speichers
        int lengthfieldsize = usedmarkervalue - 8;  //berechnet Laengenfeldgroesse fuer belegten Block
        int size = object.length + 2 * lengthfieldsize;
        long address;

        SegmentHeader segment = findSegment();
        long stamp = segment.lock.writeLock();
        try {
            segment.usedspace += size;
            int blocklist = segment.findFittingBlockList(size);         //Suche nach passendem freien Block
            address = segment.getListAnchor(blocklist);
            address = findFittingBlockInList(size, address);
            while(address == 0){
                blocklist++;
                address = findFittingBlockInList(size, segment.getListAnchor(blocklist));
            }
            if(address == segment.getListAnchor(blocklist)){            //Falls Block erstes Element der Liste ist wird der Nachfolger neuer Anker
                long nextfreeblock = getNextFreeBlock(address);
                if(nextfreeblock != 0) {
                    int freelengthfieldsize = readMarkerLowerBits(nextfreeblock - 1);
                    writeAddressField(nextfreeblock + freelengthfieldsize + ADDRESS_SIZE, 0);
                }
                segment.setListAnchor(blocklist, nextfreeblock);
            } else {
                removeBlockFromFreeBlockList(address);
            }
            int blocklengthfield = readMarkerLowerBits(address - 1);
            int blocksize = readLengthField(address, blocklengthfield);    //Groesse des angeforderten Blocks
            int newblocksize = blocksize - size - 1;                       //Groesse des neuen freien Blocks

            long newblockaddress = address + size + 1;                  //Adresse des neuen freien Blocks

            //Daten werden in den Block geschrieben
            writeMarkerLowerBits(address - 1, usedmarkervalue);
            writeLengthField(address, object.length, lengthfieldsize);
            writeByteArray(address + lengthfieldsize, object);
            writeLengthField(address + lengthfieldsize + object.length, object.length, lengthfieldsize);
            writeMarkerUpperBits(address + size, usedmarkervalue);
            if(newblocksize > 0) {
                cutFreeBlock(segment, newblockaddress, newblocksize); //erstellt aus ueberschuessigem Speicher neuen freien Block
            }
        } finally {
            segment.lock.unlockWrite(stamp);
        }
        return address;


    }

    public void deallocate(long address){
        SegmentHeader segment = getSegmentByAddress(address);

        if(segment != null) {
            long stamp = segment.lock.writeLock();
            try {
                int lengthfieldsize = readMarkerLowerBits(address - 1) - 8;
                int freeblocksize = readLengthField(address, lengthfieldsize) + 2 * lengthfieldsize;
                segment.usedspace -= freeblocksize;
                long freeblockstart = address;
                long previousblock;


                //ueberprueft ob vorheriger Block frei ist und fuegt ihn ggf zu neuem Block hinzu
                if (isBlockInSegment(freeblockstart - 2, segment) && isPreviousBlockFree(freeblockstart) && freeblocksize < initblocksize) {
                    previousblock = getPreviousBlock(freeblockstart);
                    int prevmarker = readMarkerLowerBits(previousblock - 1);
                    if (prevmarker == 0) {
                        int prevsize = readLengthField(previousblock, 1);
                        if (freeblocksize + prevsize + 1 <= initblocksize) {
                            freeblocksize += prevsize;
                            freeblockstart = previousblock;
                        }
                    } else if (prevmarker == 15) {
                        int prevsize = 1;
                        if (freeblocksize + prevsize + 1 <= initblocksize) {
                            freeblocksize += prevsize;
                            freeblockstart = previousblock;
                        }
                    } else {
                        int prevsize = readLengthField(previousblock, prevmarker);
                        if (freeblocksize + prevsize + 1 <= initblocksize) {
                            removeBlockFromFreeBlockList(previousblock);
                            freeblocksize += prevsize;
                            freeblockstart = previousblock;
                        }
                    }
                }

                long nextblock = getNextBlock(address);

                //ueberprueft ob nachfolgender Block frei ist und fügt ihn ggf zu neuem Block hinzu
                if (isBlockInSegment(nextblock, segment) && isNextBlockFree(address) && freeblocksize < initblocksize) {
                    int nextmarker = readMarkerLowerBits(nextblock - 1);
                    if (nextmarker == 0) {
                        int nextblocksize = readLengthField(nextblock, 1);
                        if (freeblocksize + nextblocksize + 1 <= initblocksize) {
                            freeblocksize += nextblocksize + 1;
                        }
                    } else if (nextmarker == 15) {
                        int nextblocksize = 1;
                        if (freeblocksize + nextblocksize + 1 <= initblocksize) {
                            freeblocksize += nextblocksize + 1;
                        }
                    } else {
                        int nextblocksize = readLengthField(nextblock, nextmarker);
                        if (freeblocksize + nextblocksize <= initblocksize) {
                            freeblocksize += nextblocksize + 1;
                            removeBlockFromFreeBlockList(nextblock);
                        }
                    }


                }
                //erstellt neuen freien Block und fuegt ihn Liste
                int freeblocklist = segment.findExactBlockList(freeblocksize);
                byte markervalue = getFreeBlockMarkerValue(freeblocksize);
                writeMarkerLowerBits(freeblockstart - 1, markervalue);
                writeMarkerUpperBits(freeblockstart + freeblocksize + 1, markervalue);
                createFreeBlock(freeblockstart, freeblocksize, 0, 0);
                changeListAnchor(segment, freeblocklist, freeblockstart);
            }finally {
                segment.lock.unlockWrite(stamp);
            }
        }
        else {
            System.out.println("Unknown Address");
        }

    }

    public void writeObject(long address, Serializable object) throws IOException {
        SegmentHeader segment = getSegmentByAddress(address);
        if(segment != null) {
            long stamp = segment.lock.writeLock();
            try {
                int lengthfieldsize = readMarkerLowerBits(address - 1) - 8;
                int blocksize = readLengthField(address, lengthfieldsize);
                byte[] objectarray = serialize(object);
                if (objectarray.length != blocksize) {
                    System.out.println("Object is of different size");
                } else {
                    writeByteArray(address + lengthfieldsize, objectarray);
                }
            } finally {
                segment.lock.unlockWrite(stamp);
            }
        }
    }

    public void writeSerialized(long address, byte[] object) {
        SegmentHeader segment = getSegmentByAddress(address);
        if(segment != null) {
            long stamp = segment.lock.writeLock();
            try {
                int lengthfieldsize = readMarkerLowerBits(address - 1) - 8;
                int blocksize = readLengthField(address, lengthfieldsize);
                if (object.length != blocksize) {
                    System.out.println("Object is of different size");
                } else {
                    writeByteArray(address + lengthfieldsize, object);
                }
            } finally {
                segment.lock.unlockWrite(stamp);
            }
        }
    }

    public byte[] readObject(long address){
        SegmentHeader segment = getSegmentByAddress(address);
        byte[] object = null;
        if(segment!=null) {
            long stamp = segment.lock.tryOptimisticRead();          //versucht zunächst optimistischen Lesezugriff
            for (int i = 0; i < 3; i++) {
                int lengthfieldsize = readMarkerLowerBits(address - 1);
                if ((lengthfieldsize >= 1 && lengthfieldsize <= 3) || lengthfieldsize == 0 || lengthfieldsize == 15) {
                    System.out.println("No object at this address");
                    return null;
                } else {
                    lengthfieldsize -= 8;
                }
                int objectsize = readLengthField(address, lengthfieldsize);
                object = readByteArray(address + lengthfieldsize, objectsize);
                if (segment.lock.validate(stamp)) {
                    return object;
                }
            }
            stamp = segment.lock.readLock();                    //falls optimistischer Lesezugriff nicht erfolgreich war wird stattdessen ein regulaerer Lesezugriff durchgefuehrt
            try {
                int lengthfieldsize = readMarkerLowerBits(address - 1);
                if ((lengthfieldsize >= 1 && lengthfieldsize <= 3) || lengthfieldsize == 0 || lengthfieldsize == 15) {
                    System.out.println("No object at this address");
                    return null;
                } else {
                    lengthfieldsize -= 8;
                }
                int objectsize = readLengthField(address, lengthfieldsize);
                object = readByteArray(address + lengthfieldsize, objectsize);

            } finally {
                segment.lock.unlockRead(stamp);
            }
        }
        return object;
    }

    public void createFreeBlock(long address, int size, long next, long prev){
        int lengthfieldsize = getFreeBlockMarkerValue(size);
        writeLengthField(address, size, lengthfieldsize);
        writeAddressField(address + lengthfieldsize, next);
        writeAddressField(address + lengthfieldsize + ADDRESS_SIZE, prev);
        writeLengthField(address + size - lengthfieldsize, size, lengthfieldsize);

    }

    private void cutFreeBlock(SegmentHeader segment, long newblockaddress, int newblocksize){

        byte markervalue = getFreeBlockMarkerValue(newblocksize);

        if(markervalue == 15){
            writeMarkerLowerBits(newblockaddress - 1, markervalue);
            writeMarkerUpperBits(newblockaddress, markervalue);
        }
        else if(markervalue == 0){
            writeMarkerLowerBits(newblockaddress - 1, markervalue);
            writeLengthField(newblockaddress, newblocksize, 1);
            writeLengthField(newblockaddress + newblocksize - 1, newblocksize, 1);
            writeMarkerUpperBits(newblockaddress + newblocksize, markervalue);
        }
        else {
            int blocklist = segment.findExactBlockList(newblocksize);
            writeMarkerLowerBits(newblockaddress - 1, markervalue);
            createFreeBlock(newblockaddress, newblocksize, 0, 0);
            writeMarkerUpperBits(newblockaddress + newblocksize, markervalue);
            changeListAnchor(segment, blocklist, newblockaddress);
        }

    }

    private byte[] serialize(Serializable object) throws IOException {
        try(ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        }
    }


    //Bloecke

    public long findFittingBlockInList(int size, long anchor){      //sucht in Liste nach passendem Block
        long block = anchor;
        while(block != 0){
            int lengthfieldsize = readMarkerLowerBits(block - 1);
            if(readLengthField(block, lengthfieldsize) >= size)
                return block;
            else
                block = getNextFreeBlock(block);
        }
        return 0;
    }

    public long getNextBlock(long address){                         //berechnet Adresse des naechsten Blocks
        int marker = readMarkerLowerBits(address - 1);
        int blocksize;
        if(marker == 0){
            blocksize = readLengthField(address, 1);
            return address + blocksize + 1;
        } else if(marker == 15){
            return address + 1;
        } else {
            if (isBlockUsed(address)) marker -= 8;
            blocksize = readLengthField(address, marker);
            if (isBlockUsed(address)) {
                return address + blocksize + 2 * (marker - 8) + 1;
            }
            return address + blocksize + 1;
        }
    }

    public long getPreviousBlock(long address){                     //berechnet Adresse des vorherigen Blocks
        int marker = readMarkerUpperBits(address - 1);
        int blocksize;
        if(marker == 0) {
            blocksize = readLengthField(address - 2, 1);
            return address - blocksize - 1;
        } else if(marker == 15){
            return address - 1;
        }
        else {
            if (marker >= 9 && marker <= 11) {
                marker -= 8;
            }
            blocksize = readLengthField((address - 1 - marker), marker);
            if (isPreviousBlockUsed(address)) return address - blocksize - 2 * marker - 1;
            else return address - blocksize - 1;
        }
    }


    public boolean isBlockInSegment(long address, SegmentHeader segment){
        return ((address >= segment.startaddress) && (address <= segment.endaddress));
    }

    public boolean isPreviousBlockFree(long address){
        int prevmarker = readMarkerUpperBits(address-1);
        return ((prevmarker >=1 && prevmarker <= 3) || prevmarker == 0 || prevmarker == 15);
    }

    public boolean isPreviousBlockUsed(long address){
        int prevmarker = readMarkerUpperBits(address-1);
        return ((prevmarker >=9 && prevmarker <= 11));
    }


    public boolean isNextBlockFree(long address){
        int lengthfieldsize = readMarkerLowerBits(address - 1);
        if(isBlockUsed(address)){
            lengthfieldsize -= 8;
        }
        int blocksize = readLengthField(address, lengthfieldsize);
        if(isBlockUsed(address)){
            blocksize += 2 * lengthfieldsize;
        }
        int nextmarker = readMarkerLowerBits(address + blocksize);
        return (nextmarker >=0 && nextmarker <= 3);
    }

    private boolean isBlockUsed(long address){
        int marker = readMarkerLowerBits(address - 1);
        return (marker >= 9 && marker <= 11);
    }

    //Segmente

    private SegmentHeader findSegment(){
        int index = 0;

        for(int i = 0; i < segments; i++){
            if(segmentlist[i].usedspace < segmentlist[index].usedspace) index = i;      //gibt Segment mit dem wenigsten belegten Speicher zurueck
        }
        return segmentlist[index];
    }

    private SegmentHeader getSegmentByAddress(long address){
        for(int i = 0; i < segments; i++){
            if(address >= segmentlist[i].startaddress && address < segmentlist[i].endaddress){
                return segmentlist[i];
            }
        }

        return null;
    }

    //Laengenfeld

    public void writeLengthField(long address, int size, int fieldsize){

        byte[] buffer = ByteBuffer.allocate(Integer.BYTES).putInt(size).array();
        int counter = 0;
        for(int i = Integer.BYTES - fieldsize; i < Integer.BYTES; i++){                     //schreibt nur so viele Bytes wie bnoetigt werden
            offHeapAccess.writeByte(address + counter + addressoffset, buffer[i]);
            counter++;
        }
    }

    public int readLengthField(long address, int lengthfieldsize){
        byte[] convert = new byte[Integer.BYTES];
        int counter = 0;
        for(int i = Integer.BYTES - lengthfieldsize; i < Integer.BYTES; i++){           //schreibt Bytes so in das Array dass ByteBuffer richtig konvertieren kann
            convert[i] = offHeapAccess.readByte(address + counter + addressoffset);
            counter++;
        }
        return ByteBuffer.allocate(Integer.BYTES).put(convert).flip().getInt();
    }

    //Adresslogik


    public void writeAddressField(long address, long value){

        byte[] bytearray = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        int counter = 0;
        for(int i = Long.BYTES - ADDRESS_SIZE; i < Long.BYTES; i++){                    //schreibt die 5 Byte der Adresse in dn Speicher
            offHeapAccess.writeByte(address + counter + addressoffset, bytearray[i]);
            counter++;
        }

    }

    public long readAddressField(long address){

        int counter = 0;

        byte[] bytearray = new byte[Long.BYTES];
        for(int i = Long.BYTES - ADDRESS_SIZE; i < Long.BYTES; i++){                //schreibt Bytes so in das Array dass ByteBuffer richtig konvertieren kann
            bytearray[i] = offHeapAccess.readByte(address + counter + addressoffset);
            counter++;
        }
        return ByteBuffer.allocate(Long.BYTES).put(bytearray).flip().getLong();

    }

    private long getNextFreeBlock(long address){
        int lengthfieldsize = readMarkerLowerBits(address-1);
        return readAddressField(address + lengthfieldsize);
    }

    private long getPreviousFreeBlock(long address){
        int lengthfieldsize = readMarkerLowerBits(address-1);
        return readAddressField(address + lengthfieldsize + ADDRESS_SIZE);
    }

    private void removeBlockFromFreeBlockList(long address){
        long nextblock = getNextFreeBlock(address);
        long prevblock = getPreviousFreeBlock(address);
        if(nextblock != 0 && prevblock == 0) {                                                      //Block ist erste Element der Liste
            int nextblocklengthfield = readMarkerLowerBits(nextblock - 1);
            writeAddressField(nextblock + nextblocklengthfield + ADDRESS_SIZE, prevblock);
        }
        else if(nextblock == 0 && prevblock != 0) {                                                 //Block ist letztes Element der Liste
            int prevblocklengthfield = readMarkerLowerBits(prevblock - 1);
            writeAddressField(prevblock + prevblocklengthfield, nextblock);
        }
        else {                                                                                      //veraendert Zeiger so dass Block nicht mehr Teil der Freispeicherliste ist
            int nextblocklengthfield = readMarkerLowerBits(nextblock - 1);
            writeAddressField(nextblock + nextblocklengthfield + ADDRESS_SIZE, prevblock);
            int prevblocklengthfield = readMarkerLowerBits(prevblock - 1);
            writeAddressField(prevblock + prevblocklengthfield, nextblock);
        }
    }

    private void changeListAnchor(SegmentHeader segment, int list, long newanchor){
        long oldanchor = segment.getListAnchor(list);
        if(oldanchor != 0) {
            int oldlengthfieldsize = readMarkerLowerBits(oldanchor - 1);
            writeAddressField(oldanchor + oldlengthfieldsize + ADDRESS_SIZE, newanchor); //Vorgaenger des alten Ankers ist neuer Anker
            int newlengthfieldsize = readMarkerLowerBits(newanchor - 1);
            writeAddressField(newanchor + newlengthfieldsize, oldanchor);
        }
        segment.setListAnchor(list, newanchor);
    }

    //Markerlogik

    public int readMarkerUpperBits(long address){
        byte marker = offHeapAccess.readByte(address + addressoffset);
        int value = marker & 0xFF;
        return (byte) (value >>> 4);
    }

    public int readMarkerLowerBits(long address){
        byte marker = offHeapAccess.readByte(address + addressoffset);
        int value = marker & 0xFF;
        return  (value & 0xF);
    }

    public void writeMarkerUpperBits(long address, byte value){
        int lowerbits = readMarkerLowerBits(address);
        byte marker = (byte) (value << 4);
        marker = (byte) (marker | lowerbits);
        offHeapAccess.writeByte(address + addressoffset, marker);
    }

    public void writeMarkerLowerBits(long address, byte value){
        byte marker = offHeapAccess.readByte(address + addressoffset);
        marker = (byte) (marker & 0xF0);
        marker = (byte) (marker | value);
        offHeapAccess.writeByte(address + addressoffset, marker);
    }


    private byte getFreeBlockMarkerValue(int size){

        if(size == 1) return 15;
        else if(size < 12) return 0;
        else if(size < 256) return 1;
        else if(size < 65536) return 2;
        else return 3;
    }

    private byte getUsedBlockMarkerValue(long size){
        if(size < 256) return 9;
        else if(size < 65536) return 10;
        else return 11;
    }

    public void writeByte(long address, byte value){
        offHeapAccess.writeByte(address + addressoffset, value);
    }


    public void writeByteArray(long address, byte[] value){
        for(int i = 0; i < value.length; i++){
            offHeapAccess.writeByte(address + i + addressoffset, value[i]);
        }
    }

    public byte[] readByteArray(long address, int size){
        byte[] value = new byte[size];
        for(int i = 0; i < size; i++){
            value[i] = offHeapAccess.readByte(address + i + addressoffset);
        }
        return value;
    }

    public void cleanup(){
        offHeapAccess.freeMemory(addressoffset);
    }


}

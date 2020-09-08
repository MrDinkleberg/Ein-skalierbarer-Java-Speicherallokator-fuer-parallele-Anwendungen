import java.util.concurrent.locks.StampedLock;

public class SegmentHeader {

    public static final int MAXBLOCKSIZE_EXPONENT = 24;

    public long startaddress;
    public long endaddress;
    public long[] freeblocks;
    public long usedspace;

    public final StampedLock lock;


    public SegmentHeader(long startaddress, long size) {
        this.startaddress = startaddress;
        this.endaddress = startaddress + size;
        this.usedspace = 0;
        lock = new StampedLock();
        freeblocks = new long[MAXBLOCKSIZE_EXPONENT+1];
    }


    public int findFittingBlockList(int size){

        int index = 0;

        if(size >= 12 && size <= 23 && freeblocks[0] != 0 ) index = 0;     //Die ersten Freispeicherlisten besitzen ein anderes Groessenintervall
        if(size >= 24 && size <= 35 && freeblocks[1] != 0 ) index = 1;      //als die folgenden
        if(size >= 36 && size <= 47 && freeblocks[2] != 0 ) index = 2;
        if(size >= 48 && size <= 63 && freeblocks[3] != 0 ) index = 3;




        for(int i = 4; i <= MAXBLOCKSIZE_EXPONENT; i++ ){                                       //ab der fuenften Liste wird in der i-ten Liste
            if(size >= Math.pow(2, i+2) && size < Math.pow(2, i+3)){                            //Bloecke der Groesse 2^(i+1) bis 2^(i+2)-1
                index = i;                                                                      //gespeichert (Array-Indizes, deshalb erste Liste
            }                                                                                  //bei i=0)
        }
        while(freeblocks[index] == 0) index++;

        return index;
    }

    public int findExactBlockList(int size){
        if(size >= 12 && size <= 23) return 0;     //Die ersten Freispeicherlisten besitzen ein anderes Groessenintervall
        if(size >= 24 && size <= 35) return 1;     //als die anderen
        if(size >= 36 && size <= 47) return 2;
        if(size >= 48 && size <= 63) return 3;

        for(int i = 4; i <= MAXBLOCKSIZE_EXPONENT; i++ ){                                       //ab der fuenften Liste wird in der i-ten Liste
            if(size >= Math.pow(2, i+2) && size < Math.pow(2, i+3)){                            //Bloecke der Groesse 2^(i+1) bis 2^(i+2)-1
                return i;                                                                      //gespeichert (Array-Indizes, deshalb erste Liste
            }                                                                                  //bei i=0)
        }

        return MAXBLOCKSIZE_EXPONENT;
    }

    public void setListAnchor(int index, long address){
        freeblocks[index] = address;
    }

    public long getListAnchor(int index){
        return freeblocks[index];
    }




}

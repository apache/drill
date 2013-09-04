package org.apache.drill.exec;

import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class SortTest {
  private static final int RECORD_COUNT = 10*1000*1000;
  private static final int KEY_SIZE = 10;
  private static final int DATA_SIZE = 90;
  private static final int RECORD_SIZE = KEY_SIZE + DATA_SIZE; 
  
  private byte[] data;
  
  public static void main(String[] args) throws Exception{
    for(int i =0; i < 100; i++){
      SortTest st = new SortTest();
      long nanos = st.doSort();
      System.out.print("Sort Completed in ");
      System.out.print(nanos);
      System.out.println(" ns.");
    }
  }
  
  SortTest(){
    System.out.print("Generating data... ");
    data = new byte[RECORD_SIZE*RECORD_COUNT];
    Random r = new Random();
    r.nextBytes(data);
    System.out.print("Data generated. ");
  }
  
  public long doSort(){
    QuickSort qs = new QuickSort();
    ByteSortable b = new ByteSortable();
    long nano = System.nanoTime();
    qs.sort(b, 0, RECORD_COUNT);
    return System.nanoTime() - nano;
  }
  
  private class ByteSortable implements IndexedSortable{
    final byte[] space = new byte[RECORD_SIZE];
    final BytesWritable.Comparator comparator = new BytesWritable.Comparator();
    
    @Override
    public int compare(int index1, int index2) {
      return comparator.compare(data, index1*RECORD_SIZE, KEY_SIZE, data, index2*RECORD_SIZE, KEY_SIZE);
    }

    @Override
    public void swap(int index1, int index2) {
      int start1 = index1*RECORD_SIZE;
      int start2 = index2*RECORD_SIZE;
      System.arraycopy(data, start1, space, 0, RECORD_SIZE);
      System.arraycopy(data, start2, data, start1, RECORD_SIZE);
      System.arraycopy(space, 0, data, start2, RECORD_SIZE);
    }
  }
}

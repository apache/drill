/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.netty.buffer;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class TestPoolChunkTrim {
	
	int pageSize=8192; // default
	
	/** A convenience method to do all the tests. */
	public void test() {
		singlePageTest();
		tiniestTinyTest();
		largestTinyTest();
		smallestSmallTest();
		largestSmallTest();
		trimTest();
		trimTiny();
		tinyGrow();
	}

	/**
	 * Unit test the memory allocator and trim() function.
	 * The results are confirmed by examining the state of the memory allocator.
	 * In this test, we are working with a single chunk and a 
	 * small set of subpage allocations.
	 * 
	 * Chunk status can be verified by matching with 
	 *      "Chunk ... <bytes consumed>/".
	 * The subpage allocators are verified by matching with
	 *      "<nr allocated>/ ... elemSize: <size>"
	 *      	 *   
	 *   A cleaner approach would create new methods to query
	 *      - how many pages (total) have been allocated, and
	 *      - how many elements of a particular size have been allocated.   
	 */
	
	/** Allocate and free a single page */
	@Test
	public void singlePageTest() {
        normalTest(pageSize/2+1, pageSize, 1);
	}
	
	/** Allocate and free a larger run of pages */
	@Test
	public void multiPageTest() {
		normalTest(pageSize*25+1, pageSize*31+1, 32);
	}
	
	/** Allocate and free the tiniest tiny allocation */
	@Test
	public void tiniestTinyTest() {
		subpageTest(1, 1, 16);
	}
	
	/** Allocate and free the largest tiny allocation */
	@Test
	public void largestTinyTest() {
		subpageTest(512-16-15, 512-16-15, 512-16);
	}
	
	/** Allocate and free the smallest small allocation */
	@Test
	public void smallestSmallTest() {
		subpageTest(512-15, 512-15, 512);
	}
	
	/** Allocate and free the largest small allocation */
	@Test
	public void largestSmallTest() {
		subpageTest(pageSize/2-1, pageSize/2-1, pageSize/2);
	}
		

	/** Trim a large block to a smaller block. */
    @Test
    public void trimTest() {

		// Allocate a large block and trim to a single page
		TestAllocator allocator = new TestAllocator();
		ByteBuf block = allocator.directBuffer(25*pageSize, 256*pageSize);
		Assert.assertTrue(25*pageSize <= block.capacity() && block.capacity() <= 256*pageSize);
		allocator.assertPages(256);
		
		block.capacity(pageSize/2+1);
		Assert.assertTrue(block.capacity() == pageSize/2+1);
		allocator.assertPages(1);
    }
    
    
    /** Trim a page down to a tiny buffer. */
    @Test
    public void trimTiny() {

		// Allocate a single page
		TestAllocator allocator = new TestAllocator();
		ByteBuf block = allocator.directBuffer(pageSize, pageSize);
		Assert.assertTrue(pageSize == block.capacity());
		allocator.assertPages(1);
		
		// Trim the single page to a tiny size.
		block.capacity(31);
		allocator.assertElement(1, 32).assertPages(1);
    }
    
    
    /** Grow a tiny buffer to a normal one */
    @Test
    public void tinyGrow() {

		// Allocate a tiny buffer
		TestAllocator allocator = new TestAllocator();
		ByteBuf block = allocator.directBuffer(1, 1);
		allocator.assertPages(1).assertElement(1, 16);
		
		// Resize the tiny block to two pages
		block = block.capacity(pageSize+1);
		Assert.assertTrue(block.capacity() == pageSize+1);
		allocator.assertPages(3).assertElement(0, 16);
    }
    
   

    
    
    /** Test the allocation and free of a "normal" allocation */
	private void normalTest(int min, int max, int pages) {
		TestAllocator allocator = new TestAllocator();
		
		ByteBuf block = allocator.directBuffer(min, max);
		Assert.assertTrue(block.capacity() >= min);
		Assert.assertTrue(block.capacity() <= max);
	    allocator.assertPages(pages);
	    
		block.release();
		allocator.assertPages(0);
	}
		
	
	
	/** Test the allocation and free of a "subpage" allocation */
    private void subpageTest(int min, int max, int expected) {
    	
		TestAllocator allocator = new TestAllocator();
		
		// Allocate the buffer and verify we have the expected number of pages
		ByteBuf block = allocator.directBuffer(min, max);
		Assert.assertTrue(block.capacity() >= min);
		Assert.assertTrue(block.capacity() <= max);
	    allocator.assertPages(1).assertElement(1, expected);
	    
	    // Release the buffer. Verify the element is returned to pool and page still allocated.
		block.release();
		allocator.assertPages(1).assertElement(0, expected);
	}
    	
    
    
    /** An allocator with some stuff added to aid testing */
    class TestAllocator extends PooledByteBufAllocatorL {

    	TestAllocator() {super(true);}
    	
    	public TestAllocator assertPages(int pages) {
    		return assertMatch("Chunk.* "+pages*pageSize+"/");
    	}

    	public TestAllocator assertElement(int count, int size) {
    		return assertMatch(count+"/.*elemSize: "+size);
    	}

    	/** 
    	 * Verify our current state matches the pattern. 
    	 * 
    	 * Note: Uses the existing "toString()" method and extracts information
    	 *   by matching a pattern to one of the output lines.
    	 *
    	 */
    	TestAllocator assertMatch(String pattern) {

    		// Get our current state as a string
    		String s = toString();

    		// Do for each line in the string
    		for (int f=0, l=s.indexOf('\n',f); l != -1; f=l+1, l=s.indexOf('\n',f)) {

    			// if the line contains pattern, then success.
    			if (s.substring(f,l).matches(".*"+pattern+".*")) return this;
    		}

    		// We didn't find a matching line, so fail the test
    		Assert.fail("Test failed to match pattern " + pattern);
    		return this;
    	}
    }

}

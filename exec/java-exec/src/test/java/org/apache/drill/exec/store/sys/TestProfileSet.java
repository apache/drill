/*
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
package org.apache.drill.exec.store.sys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.store.sys.store.ProfileSet;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the size-constrained ProfileSet for use in the webserver's '/profiles' listing
 */
public class TestProfileSet {
  private final static String PROFILE_PREFIX = "t35t-pr0fil3-";
  static int initCapacity;
  static int finalCapacity;
  static int storeCount;
  static Random rand;
  static List<String> masterList;

  @BeforeClass
  public static void setupProfileSet() {
    initCapacity = 50;
    finalCapacity = 70;
    storeCount = 100;
    rand = new Random();
    //Generating source list of storeCount # 'profiles'
    masterList = new LinkedList<String>();
    for (int i = 0; i < storeCount; i++) {
      masterList.add(PROFILE_PREFIX + StringUtils.leftPad(String.valueOf(i), String.valueOf(storeCount).length(), '0'));
    }
  }

  @Test
  public void testProfileOrder() throws Exception {
    //clone initial # profiles and verify via iterator.
    ProfileSet testSet = new ProfileSet(initCapacity);
    List<String> srcList = new LinkedList<String>(masterList);

    //Loading randomly
    for (int i = 0; i < initCapacity; i++) {
      String poppedProfile = testSet.add(srcList.remove(rand.nextInt(storeCount - i)));
      assert (poppedProfile == null);
      assertEquals(null, poppedProfile);
    }

    //Testing order
    String prevProfile = null;
    while (!testSet.isEmpty()) {
      String currOldestProfile = testSet.removeOldest();
      if (prevProfile != null) {
        assertTrue( prevProfile.compareTo(currOldestProfile) > 0 );
      }
      prevProfile = currOldestProfile;
    }
  }

  //Test if inserts exceeding capacity leads to eviction of oldest
  @Test
  public void testExcessInjection() throws Exception {
    //clone initial # profiles and verify via iterator.
    ProfileSet testSet = new ProfileSet(initCapacity);
    List<String> srcList = new LinkedList<String>(masterList);

    //Loading randomly
    for (int i = 0; i < initCapacity; i++) {
      String poppedProfile = testSet.add(srcList.remove(rand.nextInt(storeCount - i)));
      assertEquals(null, poppedProfile);
    }

    //Testing Excess by looking at oldest popped
    for (int i = initCapacity; i < finalCapacity; i++) {
      String toInsert = srcList.remove(rand.nextInt(storeCount - i));
      String expectedToPop = ( toInsert.compareTo(testSet.getOldest()) > 0 ?
          toInsert : testSet.getOldest() );

      String oldestPoppedProfile = testSet.add(toInsert);
      assertEquals(expectedToPop, oldestPoppedProfile);
    }

    assertEquals(initCapacity, testSet.size());
  }

  //Test if size internally resizes to final capacity with no evictions
  @Test
  public void testSetResize() throws Exception {
    //clone initial # profiles into a 700-capacity set.
    ProfileSet testSet = new ProfileSet(finalCapacity);
    List<String> srcList = new LinkedList<String>(masterList);

    //Loading randomly
    for (int i = 0; i < initCapacity; i++) {
      String poppedProfile = testSet.add(srcList.remove(rand.nextInt(storeCount - i)));
      assertEquals(null, poppedProfile);
    }

    assertEquals(initCapacity, testSet.size());

    //Testing No Excess by looking at oldest popped
    for (int i = initCapacity; i < finalCapacity; i++) {
      String poppedProfile = testSet.add(srcList.remove(rand.nextInt(storeCount - i)));
      assertEquals(null, poppedProfile);
    }

    assertEquals(finalCapacity, testSet.size());
  }

}

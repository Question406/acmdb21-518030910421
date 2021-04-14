# ACM-DB Lab2

This is the answer document for lab2.

## Design Choices

1. Exercise 1:

   * BufferPool.java flushPage()
  * eviction policy: `LRU`
     * mark all pages fetched as dirty, set them to be clean when flushed

2. Exercise 2:

   * findLeafPage()
     * nothing special

3. Exercise 3:

   * splitLeafPage(), splitInternalPage()
     * nothing special

4. Exercise 4:

   * stealFromLeafPage()...

     * nothing special

       

## Changes to API

None.



## Missing or Incomplete elements

I believe None.



## Time Spent

1 week.

* Stuck on a weird bug in Exercise 3 for some time. 

  When rootPtr is changed in an insert process, it still remains unchanged when the insertion is completed. Turned out the reason is that I forgot to mark pages dirty in BufferPool and rootPtr page was flushed without committing change.


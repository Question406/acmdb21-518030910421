# ACM-DB Lab4

This is the answer document for lab4.

## Design Choices

1. Exercise 1:
    * Implement the histogram method as the doc says.

2. Exercise 2:
    * Implement the simple heuristic method as the doc says.

3. Exercise 3:
    * Implement the estimation as the doc says.
    
4. Exercise 4:
    * Implement `orderingJoins()` following the pseudo code in the doc.
    * In `JoinOptimizer.java`, use `HashEquiJoin()` if predicate is `EQUALS` for a little speedup.

## Changes to API

* A new method `numPages()` in `DbFile.java`, used to get the number of pages in a single `DbFile`.

## Missing Components

I believe None

## Time Spent

1 week

* Stuck on the time limit in QueryTest.join for some time. Then I took advantage of `HashEquiJoin()` which is implemented in lab3 to get a little speedup. Ta-da! It works.
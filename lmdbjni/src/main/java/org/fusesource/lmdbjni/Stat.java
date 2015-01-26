package org.fusesource.lmdbjni;


/**
 * Statistics about the LMDB environment.
 */
public class Stat extends JNI.MDB_stat {
  private final long psize;
  private final long depth;
  private final long branchPages;
  private final long leafPages;
  private final long overflowPages;
  private final long entries;

  Stat(JNI.MDB_stat rc) {
    this.psize = rc.ms_psize;
    this.depth = rc.ms_depth;
    this.branchPages = rc.ms_branch_pages;
    this.leafPages = rc.ms_leaf_pages;
    this.overflowPages = rc.ms_overflow_pages;
    this.entries = rc.ms_entries;
  }

  /**
   * @return Size of a database page. This is currently the same for all databases.
   */
  public long getPsize() {
    return psize;
  }

  /**
   * @return Depth (height) of the B-tree.
   */
  public long getDepth() {
    return depth;
  }

  /**
   * @return Number of internal (non-leaf) pages.
   */
  public long getBranchPages() {
    return branchPages;
  }

  /**
   * @return Number of leaf pages.
   */
  public long getLeafPages() {
    return leafPages;
  }

  /**
   * @return Number of overflow pages.
   */
  public long getOverflowPages() {
    return overflowPages;
  }

  /**
   * @return Number of data items.
   */
  public long getEntries() {
    return entries;
  }

  @Override
  public String toString() {
    return "Stat{" +
      "psize=" + psize +
      ", depth=" + depth +
      ", branchPages=" + branchPages +
      ", leafPages=" + leafPages +
      ", overflowPages=" + overflowPages +
      ", entries=" + entries +
      '}';
  }
}

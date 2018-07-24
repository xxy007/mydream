package namenode.namespace;
//package namespace;
//
//import java.util.Iterator;
//
//import tools.set.LightSet;
//import tools.set.XSet;
//
//public class INodeMap {
//  
//  static INodeMap newInstance(INodeDirectory rootDir) {
//    // Compute the map capacity by allocating 1% of total memory
//    int capacity = LightSet.computeCapacity(1);
//    XSet<Integer, INode> map = new LightSet<>(capacity);
//    map.put(rootDir);
//    return new INodeMap(map);
//  }
//
//  /** Synchronized by external lock. */
//  private final XSet<INode, INodeWithAdditionalFields> map;
//  
//  public Iterator<INodeWithAdditionalFields> getMapIterator() {
//    return map.iterator();
//  }
//
//  private INodeMap(GSet<INode, INodeWithAdditionalFields> map) {
//    Preconditions.checkArgument(map != null);
//    this.map = map;
//  }
//  
//  /**
//   * Add an {@link INode} into the {@link INode} map. Replace the old value if 
//   * necessary. 
//   * @param inode The {@link INode} to be added to the map.
//   */
//  public final void put(INode inode) {
//    if (inode instanceof INodeWithAdditionalFields) {
//      map.put((INodeWithAdditionalFields)inode);
//    }
//  }
//  
//  /**
//   * Remove a {@link INode} from the map.
//   * @param inode The {@link INode} to be removed.
//   */
//  public final void remove(INode inode) {
//    map.remove(inode);
//  }
//  
//  /**
//   * @return The size of the map.
//   */
//  public int size() {
//    return map.size();
//  }
//  
//  /**
//   * Get the {@link INode} with the given id from the map.
//   * @param id ID of the {@link INode}.
//   * @return The {@link INode} in the map with the given id. Return null if no 
//   *         such {@link INode} in the map.
//   */
//  public INode get(long id) {
//    INode inode = new INodeWithAdditionalFields(id, null, new PermissionStatus(
//        "", "", new FsPermission((short) 0)), 0, 0) {
//      
//      @Override
//      void recordModification(int latestSnapshotId) {
//      }
//      
//      @Override
//      public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
//        // Nothing to do
//      }
//
//      @Override
//      public QuotaCounts computeQuotaUsage(
//          BlockStoragePolicySuite bsps, byte blockStoragePolicyId,
//          boolean useCache, int lastSnapshotId) {
//        return null;
//      }
//
//      @Override
//      public ContentSummaryComputationContext computeContentSummary(
//          int snapshotId, ContentSummaryComputationContext summary) {
//        return null;
//      }
//      
//      @Override
//      public void cleanSubtree(
//          ReclaimContext reclaimContext, int snapshotId, int priorSnapshotId) {
//      }
//
//      @Override
//      public byte getStoragePolicyID(){
//        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
//      }
//
//      @Override
//      public byte getLocalStoragePolicyID() {
//        return HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
//      }
//    };
//      
//    return map.get(inode);
//  }
//  
//  /**
//   * Clear the {@link #map}
//   */
//  public void clear() {
//    map.clear();
//  }
//}

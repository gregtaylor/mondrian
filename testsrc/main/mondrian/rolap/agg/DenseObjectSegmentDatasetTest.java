package mondrian.rolap.agg;

import mondrian.olap.CacheControl;
import mondrian.olap.Cube;
import mondrian.olap.MondrianServer;
import mondrian.rolap.agg.SegmentCacheManager.CompositeSegmentCache;
import mondrian.spi.SegmentCache;
import mondrian.spi.SegmentHeader;
import mondrian.test.BasicQueryTest;

import java.util.ArrayList;
import java.util.List;

public class DenseObjectSegmentDatasetTest extends BasicQueryTest {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        getTestContext().getConnection().getCacheControl(null)
            .flushSchemaCache();
    }
	
    public void testSegmentCacheEvents() throws Exception {
        SegmentCache mockCache = new MockSegmentCache();
        SegmentCacheWorker testWorker =
            new SegmentCacheWorker(mockCache, null);

        // Flush the cache before we start. Wait a second for the cache
        // flush to propagate.
        final CacheControl cc =
            getTestContext().getConnection().getCacheControl(null);
        Cube salesCube = getCube("Sales");
        cc.flush(cc.createMeasuresRegion(salesCube));
        Thread.sleep(1000);

        MondrianServer.forConnection(getTestContext().getConnection())
            .getAggregationManager().cacheMgr.segmentCacheWorkers
            .add(testWorker);

        final List<SegmentHeader> createdHeaders =
            new ArrayList<SegmentHeader>();
        final List<SegmentHeader> deletedHeaders =
            new ArrayList<SegmentHeader>();
        final SegmentCache.SegmentCacheListener listener =
            new SegmentCache.SegmentCacheListener() {
                public void handle(SegmentCacheEvent e) {
                    switch (e.getEventType()) {
                    case ENTRY_CREATED:
                        createdHeaders.add(e.getSource());
                        break;
                    case ENTRY_DELETED:
                        deletedHeaders.add(e.getSource());
                        break;
                    default:
                        throw new UnsupportedOperationException();
                    }
                }
            };

        try {
            // Register our custom listener.
            ((CompositeSegmentCache)MondrianServer
                .forConnection(getTestContext().getConnection())
                .getAggregationManager().cacheMgr.compositeCache)
                .addListener(listener);
            // Now execute a query and check the events
            executeQuery(
                "WITH SET [__DateSet0] AS {Ancestor([Time].[1997].[Q1].[1], [Time].[Quarter])}	" + 
					" MEMBER [Time].[__DateSet0_NODE] AS Sum([__DateSet0]) " + 
					" SELECT " + 
					" {[Measures].[Unit Sales], [Measures].[Store Sales], [Measures].[Mohsin Test2]} ON COLUMNS, " + 
					" CrossJoin({[Time].[__DateSet0_NODE]}, {Hierarchize({Descendants([Product].[All Products], [Product].[Product Department], SELF)})}) ON ROWS "  +
					" FROM [Sales]");

            executeQuery(
				"WITH SET [__DateSet0] AS {Ancestor([Time].[1997].[Q1].[1], [Time].[Quarter])}	" + 
					" MEMBER [Time].[__DateSet0_NODE] AS Sum([__DateSet0]) " + 
					" SELECT " + 
					" {[Measures].[Unit Sales], [Measures].[Store Sales], [Measures].[Mohsin Test2]} ON COLUMNS, " + 
					" CrossJoin({[Time].[__DateSet0_NODE]}, {Hierarchize({Descendants([Product].[All Products], [Product].[Product Family], SELF)})}) ON ROWS " + 
					" FROM [Sales] ");
			
        } finally {
            ((CompositeSegmentCache)MondrianServer
                .forConnection(getTestContext().getConnection())
                .getAggregationManager().cacheMgr.compositeCache)
                .removeListener(listener);
            MondrianServer.forConnection(getTestContext().getConnection())
                .getAggregationManager().cacheMgr.segmentCacheWorkers
                .remove(testWorker);
        }
    }
	
	private Cube getCube(String cubeName) {
        for (Cube cube
            : getConnection().getSchemaReader().withLocus().getCubes())
        {
            if (cube.getName().equals(cubeName)) {
                return cube;
            }
        }
        return null;
    }
}

import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;


public class Test {

    public static void main(String[] args) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;


        QueryBuilder qb = QueryBuilders.termQuery("content", "is");

//Index the query = register it in the percolator
        client.prepareIndex("test", ".percolator", "two")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("query", qb) // Register the query
                        .endObject())
                .setRefresh(true) // Needed when the query shall be available immediately
                .execute().actionGet();

        //Build a document to check against the percolator
        XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
        docBuilder.field("doc").startObject(); //This is needed to designate the document
        docBuilder.field("content", "This is amazing!");
        docBuilder.endObject(); //End of the doc field
        docBuilder.endObject(); //End of the JSON root object
//Percolate
        PercolateResponse response = client.preparePercolate()
                .setIndices("test")
                .setDocumentType("myDocumentType")
                .setSource(docBuilder).execute().actionGet();
//Iterate over the results
        for(PercolateResponse.Match match : response) {
            //Handle the result which is the name of
            //the query in the percolator
            System.out.println(match.toString());
        }
    }
}

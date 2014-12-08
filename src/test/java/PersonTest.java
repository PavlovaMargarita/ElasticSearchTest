import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class PersonTest {

    public String addPerson(Person person){
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        //create bean
        ObjectMapper mapper = new ObjectMapper();
        String json = "";
        try {
            json = mapper.writeValueAsString(person);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        IndexResponse insert = transportClient.prepareIndex("person_index", "person")
                .setSource(json)
                .execute()
                .actionGet();
        transportClient.close();
        return insert.getId();
    }

    public Person getPersonById(String id){
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        int scrollSize = 1000;
        List<Map<String,Object>> personList = new ArrayList<Map<String,Object>>();
        SearchResponse response = null;
        int i = 0;
        while( response == null || response.getHits().hits().length != 0){
            response = client.prepareSearch("person_index")
                    .setTypes("person")
                    .setQuery(QueryBuilders.matchQuery("_id", id))
                    .setSize(scrollSize)
                    .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            for(SearchHit hit : response.getHits()){
                personList.add(hit.getSource());
            }
            i++;
        }
        Person person = null;
        if(personList.size() == 1){
            Map<String, Object> element = personList.get(0);
            person = new Person();
            person.setFirstName( element.get("firstName").toString());
            person.setLastName(element.get("lastName").toString());
        }
        client.close();
        transportClient.close();
        return person;
    }

    public void deletePerson(String id){
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new TransportClient(settings);
        client = client.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        client.prepareDelete("person_index", "person", id)
                .execute()
                .actionGet();
        client.close();
    }

    public void testInsert(){
        Person addPerson = new Person();
        addPerson.setFirstName("Sveta");
        addPerson.setLastName("Qwerty");

        GregorianCalendar calendar = new GregorianCalendar();
        int year = randBetween(1900, 2010);
        calendar.set(calendar.YEAR, year);
        int dayOfYear = randBetween(1, calendar.getActualMaximum(calendar.DAY_OF_YEAR));
        calendar.set(calendar.DAY_OF_YEAR, dayOfYear);

        java.util.Date date= new java.util.Date();
        date.setTime(calendar.getTimeInMillis());

        addPerson.setDate(new Date(date.getTime()));

        String personId = addPerson(addPerson);
        Person getPerson = getPersonById(personId);
        Assert.assertEquals(addPerson, getPerson);
        deletePerson(personId);
    }

    @Test
    public void search(){
        Person addPerson = new Person();
        addPerson.setFirstName("Petrov");
        addPerson.setLastName("Petr");
        searchPersonOr();
//        searchPersonOr("Petrov", "Petr");
//        searchDate();
    }

    private long searchPersonOneValueSeveralFields(String value, String... fieldNames){

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        int scrollSize = 1000;
        List<Map<String,Object>> personList = new ArrayList<Map<String,Object>>();
        SearchResponse response = null;
        int i = 0;
        long count = 0;
        while( response == null || response.getHits().hits().length != 0){
            response = client.prepareSearch("person_index")
                    .setTypes("person")
                    .setQuery(QueryBuilders.multiMatchQuery(value, fieldNames))
                    .setSize(scrollSize)
                    .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            count += response.getHits().getTotalHits();
            for(SearchHit hit : response.getHits()){
                personList.add(hit.getSource());
            }
            i++;
        }

        transportClient.close();
        client.close();
        return count;
    }

    private long searchPersonLike(String value, String... fieldNames){

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        int scrollSize = 1000;
        List<Map<String,Object>> personList = new ArrayList<Map<String,Object>>();
        SearchResponse response = null;
        int i = 0;
        long count = 0;
        while( response == null || response.getHits().hits().length != 0){
            response = client.prepareSearch("person_index")
                    .setTypes("person")
                    .setQuery(QueryBuilders.multiMatchQuery(value, fieldNames).type(MatchQueryBuilder.Type.PHRASE_PREFIX))
                    .setSize(scrollSize)
                    .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            count += response.getHits().getTotalHits();
            for(SearchHit hit : response.getHits()){
                personList.add(hit.getSource());
            }
            i++;
        }

        transportClient.close();
        client.close();
        return count;
    }

    private long searchPersonOr() {

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        int scrollSize = 1000;
        List<Map<String, Object>> personList = new ArrayList<Map<String, Object>>();
        SearchResponse response = null;
        int i = 0;
        long count = 0;
        while (response == null || response.getHits().hits().length != 0) {
            response = client.prepareSearch("person_index")
                    .setTypes("person")
                    .setQuery(QueryBuilders.disMaxQuery().add(QueryBuilders.matchQuery("firstName", "Ivanov")).add(QueryBuilders.matchQuery("firstName", "Petr")))
                    .addFacet(FacetBuilders.termsFacet("firstName")
                            .field("firstName")
                            .size(10000))
                    .setSize(scrollSize)
                    .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            count += response.getHits().getTotalHits();
            for (SearchHit hit : response.getHits()) {
                personList.add(hit.getSource());
            }
            i++;

            // sr is here your SearchResponse object
            TermsFacet f = (TermsFacet) response.getFacets().facetsAsMap().get("firstName");

            f.getTotalCount();      // Total terms doc count
            f.getOtherCount();      // Not shown terms doc count
            f.getMissingCount();    // Without term doc count

// For each entry
            for (TermsFacet.Entry entry : f) {
                entry.getTerm();    // Term
                entry.getCount();   // Doc count
            }
        }
        transportClient.close();
        client.close();
        return count;
    }

    private long searchPersonAnd(String firstName, String lastName) {

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        int scrollSize = 1000;
        List<Map<String, Object>> personList = new ArrayList<Map<String, Object>>();
        SearchResponse response = null;
        int i = 0;
        long count = 0;
        while (response == null || response.getHits().hits().length != 0) {
            response = client.prepareSearch("person_index")
                    .setTypes("person")
                    .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("firstName", "Petr")).must(QueryBuilders.matchQuery("lastName", "Petrov")))

                    .setSize(scrollSize)
                    .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            count += response.getHits().getTotalHits();
            for (SearchHit hit : response.getHits()) {
                personList.add(hit.getSource());
            }
            i++;
        }
        transportClient.close();
        client.close();

        return count;
    }

    public long searchDate(String dateFrom, String dateEnd){
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        int scrollSize = 1000;
        List<Map<String, Object>> personList = new ArrayList<Map<String, Object>>();
        SearchResponse response = null;
        int i = 0;
        long count = 0;
        while (response == null || response.getHits().hits().length != 0) {
            response = client.prepareSearch("person_index")
                    .setTypes("person")
//                    .setQuery(QueryBuilders.matchAllQuery())
                    .setQuery(QueryBuilders.rangeQuery("date").from(dateFrom).to(dateEnd))
                    .setSize(scrollSize)
                    .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            count += response.getHits().getTotalHits();
            for (SearchHit hit : response.getHits()) {
                personList.add(hit.getSource());
            }
            i++;
        }
        transportClient.close();
        client.close();
        return count;
    }

    public void getAllPerson() throws ExecutionException, InterruptedException {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        int scrollSize = 1000;
        List<Map<String,Object>> personList = new ArrayList<Map<String,Object>>();
        SearchResponse response = null;
        int i = 0;
        while( response == null || response.getHits().hits().length != 0){
            response = client.prepareSearch("person_index")
                    .setTypes("person")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(scrollSize)
                    .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            for(SearchHit hit : response.getHits()){
                Map<String, Object> element = hit.getSource();
                element.put("_id", hit.getId());
                personList.add(element);
            }
            i++;
        }

        List result = new ArrayList();
        for(Map<String, Object> element: personList){
            Person person = new Person();
            person.setLastName(element.get("secondName").toString());
            person.setFirstName(element.get("firstName").toString());
        }
        client.close();
        transportClient.close();
        System.out.println(personList);
    }

    public void addPercolator(String indexName, String percolatorName, String field, String content) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        QueryBuilder qb = QueryBuilders.termQuery(field, content);

        client.prepareIndex(indexName, ".percolator", percolatorName)
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("query", qb)
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        client.close();
    }

    public void deletePercolator(String indexName, String percolatorName){
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        DeleteResponse response = client.prepareDelete(indexName, ".percolator", percolatorName)
                .execute()
                .actionGet();
        client.close();
        transportClient.close();
    }


    public List<String> createDocumentAndGetPercolateName(String indexName, String type, String field, String content) throws IOException {
        XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
        docBuilder.field("doc").startObject(); //This is needed to designate the document
        docBuilder.field(field, content);
        docBuilder.endObject(); //End of the doc field
        docBuilder.endObject(); //End of the JSON root object

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        Client client = transportClient;

        PercolateResponse response = client.preparePercolate()
                .setIndices(indexName)
                .setDocumentType(type)
                .setSource(docBuilder).execute().actionGet();
        List<String> percolateList = new ArrayList<String>();

        for(PercolateResponse.Match match : response) {
            //Handle the result which is the name of
            //the query in the percolator
            percolateList.add(match.getId().toString());
        }
        return percolateList;
    }

//    @Test
    public void testPercolate() throws IOException {
        List<String> percolatorName = new ArrayList<String>();
        percolatorName.add("this");
        addPercolator("test", "three", "content", "this");
        List result = createDocumentAndGetPercolateName("test", "something", "content", "this is amazing!");
        System.out.println(result);

        if(result.size() == 1 && result.get(0).equals("three")){

            Assert.assertTrue(true);
        }else{
            Assert.assertTrue(false);
        }

        deletePercolator("test", "three");
    }

    public int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
}

package com.mongo.service;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

@Path("/Service")
public class MongoService {
	
	
	@GET
	@Path("/get/{coll}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getData(@PathParam("coll") String coll,@PathParam("name") String name){
		DBObject object = null;
		// To connect to mongodb server
        Mongo mongoClient;
        String resp = "";
		try {
			mongoClient = new Mongo( "localhost" , 27017 );
		
			
        // Now connect to your databases
        DB db = mongoClient.getDB("test" );
        System.out.println("Connect to database successfully");
        
        DBCollection collection = db.getCollection(coll);
        BasicDBObject doc = new BasicDBObject();
        doc.append("oilFieldName","Alon USA Bakersfield Refinery");
        BasicDBObject doc1 = new BasicDBObject();
        doc1.append("oilFieldName","US Oil & Refining Co.");
        BasicDBObject doc2=new BasicDBObject();
        doc2.append("oilFieldName","Shell Martinez Refinery");
        BasicDBObject doc3 = new BasicDBObject();
        doc3.append("oilFieldName","Total Petrochemicals & Refining USA, Inc.");
        //Date prevDate = new Date(System.currentTimeMillis() - 15000L);
       // doc.put("createDate", new BasicDBObject("$gte",prevDate));
        //doc.append("createDate",{$gte:prevDate});
        
       // object = collection.find(doc);
        List<DBObject> objects = new ArrayList<DBObject>();
        DBCursor cursor = collection.find(doc).sort(new BasicDBObject().append("_id",-1)).limit(1);
        objects.add(cursor.next());
        DBCursor cur1 = collection.find(doc1).sort(new BasicDBObject().append("_id",-1)).limit(1);
        objects.add(cur1.next());
        DBCursor cur2 = collection.find(doc2).sort(new BasicDBObject().append("_id",-1)).limit(1);
        objects.add(cur2.next());
        DBCursor cur3 = collection.find(doc3).sort(new BasicDBObject().append("_id",-1)).limit(1);
        objects.add(cur3.next());
        resp = getJson(objects);
		}
		catch (MongoException | UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Response response = Response.ok(resp).header("Access-Control-Allow-Origin", "*").build();
		return response; 
	}
	
	//Get Last inserted 
	@SuppressWarnings("deprecation")
	@GET
	@Path("/getLast/{coll}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLast(@PathParam("coll") String coll){
		// To connect to mongodb server
        Mongo mongoClient;
        String resp = "";
		try {
			mongoClient = new Mongo( "localhost" , 27017 );
		
			
        // Now connect to your databases
        DB db = mongoClient.getDB( "test" );
        System.out.println("Connect to database successfully");
        
        DBCollection collection = db.getCollectionFromString(coll);
        
        DBCursor cur = collection.find().sort(new BasicDBObject().append("_id",-1)).limit(1);
        DBObject obj = cur.next();
        resp=getJson(obj);
        
		}catch (MongoException | UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Response.ok(resp).build();
	}
	
	@SuppressWarnings("deprecation")
	@POST
	@Path("/insert/{coll}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response insert(String input,@PathParam("coll") String coll){
		
		// To connect to mongodb server
        Mongo mongoClient;
		try {
			mongoClient = new Mongo( "localhost" , 27017 );
		
			
        // Now connect to your databases
        DB db = mongoClient.getDB( "test" );
        System.out.println("Connect to database successfully");
        
        DBCollection collection = db.getCollectionFromString(coll);
        
        /*ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<String, String>();

		// convert JSON string to Map
		map = mapper.readValue(input, new TypeReference<Map<String,List<String>>>(){});
		
        BasicDBObject doc = new BasicDBObject("title", "MongoDB");
        
    
        doc.append("createDate",new Date());
        
        		for (Map.Entry<String,String> entry : map.entrySet()){
        			doc.append(entry.getKey(), entry.getValue());
        		}*/
        DBObject doc = (DBObject) JSON.parse(input);

        collection.insert(doc);
             System.out.println("Document inserted successfully");
		} catch (MongoException | UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Response.ok("Inserted successfully").build();
		
	}
	
	
	

	public static String getJson(final Object obj)
	{
		String json = null;
		try
		{
			final ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(Include.ALWAYS);
			json = mapper.writer().writeValueAsString(obj);
		}
		catch (final Exception exception)
		{
			System.out.println("Exception occured when converting Java object to json string. Exception message: "+ exception);
		}
		return json;
	}

}

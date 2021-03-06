package upgrad;

import static com.mongodb.client.model.Filters.ne;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

import java.sql.*;
import org.bson.conversions.Bson;

public class Driver {

  static enum Electronics {
    CAMERA,
    HEADPHONE,
    MOBILE
  }

  /**
   * Driver class main method
   *
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    // MySql credentials
    final String rdbmsUrl = "jdbc:mysql://pgc-sd-bigdata.cyaielc9bmnf.us-east-1.rds.amazonaws.com:3306/pgcdata";
    final String rdmsUser = "student";
    final String rdbmsPassword = "STUDENT123";

    //jdbc query strings
    final String camerasQuery = "select * from cameras";
    final String headphonesQuery = "select * from headphones";
    final String mobilesQuery = "select * from mobiles";

    // MongoDB Configurations
    final String mongodbUrl = "mongodb://54.147.63.10:27017";

    // Connection Default Value Initialization
    Connection sqlConnection = null;
    MongoClient mongoClient = null;

    try {
      // Creating database connections
      sqlConnection = DriverManager.getConnection(rdbmsUrl, rdmsUser, rdbmsPassword);

      mongoClient = MongoClients.create(mongodbUrl);

      //get electronicsstore db/products collection from mongodb
      MongoDatabase mongoDatabase = mongoClient.getDatabase("electronicsstore");
      MongoCollection<Document> collection = mongoDatabase.getCollection("products");

      //clear mongodb products collection
      //this avoids repeated inserts when the app runs
      Bson delFilter = ne("_id", null);
      collection.deleteMany(delFilter);

      //create list to hold rdbms records
      List<Document> electronicsItems = new ArrayList<Document>();
      Statement stmt = sqlConnection.createStatement();

      //add mobiles,cameras,headphones records from rdbms to electronicsItems list
      processElectronicsItems(sqlConnection, stmt, electronicsItems, mobilesQuery,
          Electronics.MOBILE.ordinal());
      processElectronicsItems(sqlConnection, stmt, electronicsItems, camerasQuery,
          Electronics.CAMERA.ordinal());
      processElectronicsItems(sqlConnection, stmt, electronicsItems, headphonesQuery,
          Electronics.HEADPHONE
              .ordinal());

      //insert electronics items list into mongodb
      collection.insertMany(electronicsItems);

      // List all products in the inventory
      CRUDHelper.displayAllProducts(collection);

      // Display top 5 Mobiles
      CRUDHelper.displayTop5Mobiles(collection);

      // Display products ordered by their categories in Descending Order Without autogenerated Id
      CRUDHelper.displayCategoryOrderedProductsDescending(collection);

      // Display product count in each category
      CRUDHelper.displayProductCountByCategory(collection);

      // Display wired headphones
      CRUDHelper.displayWiredHeadphones(collection);
      stmt.close();
    } catch (Exception ex) {
      System.out.println("Got Exception.");
      ex.printStackTrace();
    } finally {
      // Close Connections
      if (null != sqlConnection) {
        sqlConnection.close();
      }
      if (null != mongoClient) {
        mongoClient.close();
      }
    }

  }

  /*
  helper function to query mobiles/headphones/cameras records from rdbms
  converts records to mongodb Document, adds them to local electronicsitems
  document list
   */
  public static void processElectronicsItems(Connection sqlConnection, Statement stmt,
      List<Document> electronicsItems, String itemQuery, int itemType) throws SQLException {

    ResultSet resultSet = stmt.executeQuery(itemQuery);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int itemFields = resultSetMetaData.getColumnCount();

    while (resultSet.next()) {
      Document electronicsItem = new Document();
      for (int i = 1; i <= itemFields; i++) {
        electronicsItem = electronicsItem
            .append(resultSetMetaData.getColumnName(i), resultSet.getString(i));
      }

      switch (itemType) {
        case 0:
          electronicsItem.append("Category", "Cameras");
          break;
        case 1:
          electronicsItem.append("Category", "Headphones");
          break;
        case 2:
          electronicsItem.append("Category", "Mobiles");
          break;
      }
      electronicsItems.add(electronicsItem);
    }
    resultSet.close();
  }
}
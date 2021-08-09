package upgrad;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import java.sql.*;
import java.util.Arrays;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Sorts.descending;

public class CRUDHelper {

  /**
   * Display ALl products
   *
   * @param collection
   */
  public static void displayAllProducts(MongoCollection<Document> collection) {
    System.out.println("------ Displaying All Products ------");
    MongoCursor<Document> cursor = collection.find().iterator();
    while (cursor.hasNext()) {
      PrintHelper.printSingleCommonAttributes(cursor.next());
    }
    System.out.println();
  }

  /**
   * Display top 5 Mobiles
   *
   * @param collection
   */
  public static void displayTop5Mobiles(MongoCollection<Document> collection) {
    System.out.println("------ Displaying Top 5 Mobiles ------");
    Bson mobileFilter = eq("Category", "Mobiles");
    MongoCursor<Document> cursor = collection.find(mobileFilter).limit(5).iterator();
    while (cursor.hasNext()) {
      PrintHelper.printAllAttributes(cursor.next());
    }
    System.out.println();
  }

  /**
   * Display products ordered by their categories in Descending order without auto generated Id
   *
   * @param collection
   */
  public static void displayCategoryOrderedProductsDescending(
      MongoCollection<Document> collection) {
    System.out.println("------ Displaying Products ordered by categories ------");
    Bson orderFilter = descending("Category");
    MongoCursor<Document> cursor =
        collection.find().projection(fields(excludeId())).sort(orderFilter).iterator();
    while (cursor.hasNext()) {
      PrintHelper.printAllAttributes(cursor.next());
    }
    System.out.println();
  }

  /**
   * Display number of products in each group
   *
   * @param collection
   */
  public static void displayProductCountByCategory(MongoCollection<Document> collection) {
    System.out.println("------ Displaying Product Count by categories ------");
    Bson sortFilter = sort(descending("_id"));
    for (Document document : collection.aggregate(Arrays.asList(
        Aggregates.group("$Category",
            Accumulators.sum("Count", 1)), sortFilter
    ))) {
      PrintHelper.printProductCountInCategory(document);
    }
    System.out.println();
  }

  /**
   * Display Wired Headphones
   *
   * @param collection
   */
  public static void displayWiredHeadphones(MongoCollection<Document> collection) {
    System.out.println("------ Displaying Wired headphones ------");
    Bson wiredHeadphoneFilter = and(eq("Category", "Headphones"),
        eq("ConnectorType", "Wired"));
    MongoCursor<Document> cursor = collection.find(wiredHeadphoneFilter).iterator();
    while(cursor.hasNext()){
      PrintHelper.printAllAttributes(cursor.next());
    }
  }
}
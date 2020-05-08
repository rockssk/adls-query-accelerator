package queryAccel;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;
import com.azure.storage.common.*;
import com.azure.storage.quickquery.*;
import com.azure.storage.quickquery.models.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import org.apache.commons.csv.*;

public class QueryAccel{


public static void main( String[] args )
{

	
	BlobClient blobClient = new BlobClientBuilder()
		    .endpoint("https://queryaccel.blob.core.windows.net")
		    .sasToken("<sastoken>")//Enter your SAS token here
		    .containerName("airbnb")
		    .blobName("amsterdam/listings.csv")
		    .buildClient();
	try {    
		
		QueryBlobData(blobClient);
	}
	
	catch (Exception ex) 
	{
		System.out.println(ex.getMessage());
	}finally 
	{
		System.out.println("The program has completed successfully.");
		
	}
}
	
	


static void QueryBlobData(BlobClient blobClient) {

	String expression ="SELECT count(*) FROM BlobStorage where zipcode='1017'";
    DumpQueryCsv(blobClient, expression, true);
}

/**
 * @param blobClient
 * @param query
 * @param headers
 */
static void DumpQueryCsv(BlobClient blobClient, String query, Boolean headers) {
    try {
    
        BlobQuickQueryDelimitedSerialization input = new BlobQuickQueryDelimitedSerialization()
            .setRecordSeparator('\n')
            .setColumnSeparator(',')
            .setHeadersPresent(headers)
            .setFieldQuote('\0')
            .setEscapeChar('\\');

        BlobQuickQueryDelimitedSerialization output = new BlobQuickQueryDelimitedSerialization()
            .setRecordSeparator('\n')
            .setColumnSeparator(',')
            .setHeadersPresent(false)
            .setFieldQuote('\0')
            .setEscapeChar('\n');
                
        BlobRequestConditions requestConditions = null;
        /* ErrorReceiver determines what to do on errors. */
        ErrorReceiver<BlobQuickQueryError> errorReceiver = System.out::println;

        /* ProgressReceiver details how to log progress*/
        com.azure.storage.common.ProgressReceiver progressReceiver = System.out::println;
    
        /* Create a query acceleration client to the blob. */
        BlobQuickQueryClient qqClient = new BlobQuickQueryClientBuilder(blobClient)
            .buildClient();
        /* Open the query input stream. */
    
        InputStream stream = qqClient.openInputStream(query, input, output, requestConditions, errorReceiver, progressReceiver);
    
    
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            /* Read from stream like you normally would. */
                for (CSVRecord record : CSVParser.parse(reader, CSVFormat.EXCEL)) {
              System.out.println(record.toString());
            }
        }
    } catch (Exception e) {
        System.err.println("Exception: " + e.toString());
        //System.out.println("Exception: "+ );
    	e.printStackTrace();
        
    }
}
static void QueryPublishDates(BlobClient blobClient)
{
    String expression = "SELECT PublicationYear FROM BlobStorage";
    DumpQueryCsv(blobClient, expression, true);
}
static void QueryMysteryBooks(BlobClient blobClient)
{
    String expression = "SELECT BibNum, Title, Author, ISBN, Publisher FROM BlobStorage WHERE Subjects LIKE '%Mystery%'";
    DumpQueryCsv(blobClient, expression, true);
}
}

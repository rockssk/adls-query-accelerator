# adls-query-accelerator

Repo contains Maven java project to query ADLS blob files data directly from java code without needing Databricks or Synapse . Though there are only limited SQL features supported currently, This is a good start.
For more details refer here https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-query-acceleration-how-to?tabs=java

Steps to execute this code
1. Create a Storage Account in France Central with ZRS redundancy,V2 Kind and Hierarchical Name space enabled
2. Upload Data from airbnb (available in this repo also)
3. clone this repo and import the project into Eclipse as it is and Run the java code , Code is self explanatory

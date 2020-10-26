 **Description**
 
Using ​**Java 11**​, ​**Spring Boot 2​** and **M​aven​** implement a backend service running in **Cloud Run​** and create (configure) a data pipeline in GCP that uses this service for automatic processing of ​**Avro**​ files with embedded schema that are being uploaded to a Cloud Storage​ bucket.

Whenever a new file is uploaded to the bucket, it is automatically processed by the backend service. This service parses each file and writes the data from the file into two different tables in ​**BigQuery**​. One table contains all the fields from the files and another table contains only a subset of fields (only non-optional fields according to the Avro schema).

Avro file schema example:

	{	"namespace": "example.gcp",
	 	"type": "record",
		"name": "Client", "fields": [
	       {"name": "id", "type": "long"},
	       {"name": "name", "type": "string"},
	       {"name": "phone",  "type": ["string", "null"]},
	       {"name": "address", "type": ["string", "null"]}
		] 
	}
**Requirements**

* Push code and any supplementary documentation (if any) to a ​**GitHub** repository. --ЩЛ
* Adhere to the best practices of software development (e.g.SOLID,type safety, code readability, testability, ...). --partially
* A candidate is expected to be able to present the solution, explain it's different parts and their configuration, and demonstrate it in real-time using its GCP account and the GCP Free Tier:
https://cloud.google.com/free --I'll try

**Optional requirements (tasks)**

* enlarge the schema with more fields of different types. --did not have enough time
* write data into different BigQuery tables in parallel; -- OK
* gracefully handle all possible errors and exceptions in code including when using GCP Java SDKs; --OK possible
* investigate and present possible options for productionizing the solution from the point of view of scalability, availability, and performance. --I do not have the necessary experience for this 

1. Create GCP project
    https://cloud.google.com/resource-manager/docs/creating-managing-projects#gcloud
    
2. Enable Cloud Storage, PubSub, Cloud Run and BigQuery APIs
    https://cloud.google.com/endpoints/docs/openapi/enable-api
3. create GS Bucket
    https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-gsutil
4. create Pub/Sub topic
    https://cloud.google.com/pubsub/docs/admin
5. Download this project from GitHub :)
    
6. Compile and deploy project to Cloud Run, copy URL from project Props
    https://cloud.google.com/run/docs/quickstarts/build-and-deploy
7. Create push subscription (use Endpoint URL from CR project)
    https://cloud.google.com/run/docs/triggering/pubsub-push#create-push-subscription
8. Upload to Bucket clients.avro file
    https://cloud.google.com/storage/docs/uploading-objects#gsutil
9. Check logs of your CR project 
    https://cloud.google.com/run/docs/logging
10. Find more docs at https://cloud.google.com/

![Project Diagram](/RinfTech Project Diagram.png)

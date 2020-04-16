# Assemblyline 4 - Service Server

The service server is a API that the service clients can call to interface with the system. This is the only access the services have to the system as they are completely segregated from the other components.

##### API functionality

Service server provides the following functions via API to the client:

* File download and upload
* Register service to the system
* Get a new task
* Publish results for a task
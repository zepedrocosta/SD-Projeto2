# SD2324 project 2

This repository includes the API and a set of files that should be used in project 1.

* ```test-sd-tp1.bat``` / ```test-sd-tp1.sh``` :  script files for running the project in Windows and Linux/Mac
* ```shorts.props``` : file with information for starting servers
* ```Dockerfile``` : Dockerfile for creating the docker image of the project
* ```hibernate.cfg.xml``` : auxiliary file for using Hibernate
* ```pom.xml``` : maven file for creating the project

## Run

```cmd
mvn clean compile assembly:single docker:build
```

```cmd
-image sd2324-tp2-api-62637-63184
```

## TESTS

* 101a - **OK**
* 101b - **OK**
* 102a - **OK**
* 102b - **OK**
* 102c - **OK**
* 102d - **OK**
* 102e - **OK**
* 102f - **OK**
* 103a - **OK**
* 103b - **OK**
* 103c - **OK**
* 103d - **OK**
* 103e - **OK**
* 104a - **OK**
* 104b - **OK**
* 104c - **OK**

### Dropbox

* 105a - **OK**
* 105b - **OK**
* 105c - **OK**
* 105d - **OK**

### 

* 106a - **OK**

### Blobs Replication

* 107a - **Sometimes fail**
* 107b - **Sometimes fail**
* 107c - **Sometimes fail**

### Kafka

* 108a - **OK**
* 108b - **Sometimes fail Download null**
* 108c - **OK**

* 109a - **Fail Upload 403 Timeout after fail**
* 109b - **OK**

* 110a - **OK**
* 110b - **OK**
* 110c - **OK**
* 110d - **OK**
* 110e - **OK**
* 110f - **Fail CreateShort 409 after recovering from fail**

### Security

* 111a - **OK**
* 111b - **OK**
* 111c - **Not implemented**

### Concurrent

* 112a - **OK**
* 112b - **OK**
* 112c - **Not implemented**

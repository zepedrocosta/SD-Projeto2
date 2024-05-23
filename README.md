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
-image sd2324-tp2-62637-63184
```

## TESTS

* 101a - **OK**
* 101b - **OK**
* 102a - **OK**
* 102b - **OK**
* 102c -
* 102d -

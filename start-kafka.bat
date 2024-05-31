echo off

for /f %%i in ('docker network ls --filter name^=sdnet ^| find /c /v ""') do set RESULT=%%i

if NOT %RESULT%==2 docker network create --driver=bridge sdnet

set argC=0
for %%x in (%*) do Set /A argC+=1

if %argC% LEQ 0 echo "usage: start-kafka localhost or start-kafka kafka" & GOTO END

docker pull smduarte/sd2324-kafka

echo "Launching Kafka/Zookeeper Servers: " %1

docker rm -f kafka

docker run -h %1 --name=kafka --network=sdnet --net-alias=zookeeper --rm -t -p 9092:9092 -p 2181:2181 smduarte/sd2223-kafka
           
:END

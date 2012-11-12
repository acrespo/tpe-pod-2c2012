tpe-pod-2c2012
==============

Trabajo Práctico Especial para la materia Programación de Objetos Distribuidos del ITBA.

Alumno: Alvaro Crespo
Legajo: 50758

Fecha: 12/11/2012


Compilación:
-------------
Ejectuar `mvn package` sobre src. 
En la carpeta target se creará el jar signal-1.0-jar-with-dependencies.jar con todas las dependencias externas.


Ejecución
-------------
Desde la misma carpeta src correr el comando 

    java -jar -Djgroups.bind_addr=numeroIP signal-1.0-jar-with-dependencies.jar numeroPuerto threadsProcesamiento

donde numeroIP es la dirección a la que bindeará jgroups, numeroPuerto es el puerto donde escuchará el RMI Registry 
y threadsProcesamiento es el número de threads de procesamiento que se utilizarán.

Para ejecutar en las máquinas del ITBA el siguiente comando bastará:

    java -jar -Djgroups.bind_addr=10.6.0.$(echo $HOSTNAME | sed 's/lab//') target/signal-1.0-jar-with-dependencies.jar 20007 2

asumiendo que se desea escuchar en el puerto 20007 y utilizar 2 threads de procesamiento.

Opcionalmente también se puede pasar el parámetro -Xmx que setea el maximo tamaño del heap del JVM. Posibles opciones son:

    -Xmx2g 
    -Xmx1g
    -Xmx512m






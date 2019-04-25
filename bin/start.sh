#!/bin/sh
#参数为处理前$1天的日志文件，不传默认只处理前一天的日志
nohup java -Xms16m -Xmx32m -jar ./meari-active-user-1.0.0-SNAPSHOT.jar $1 > ./log 2>&1 &
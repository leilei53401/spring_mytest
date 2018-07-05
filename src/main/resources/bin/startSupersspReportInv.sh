#!/usr/bin/sh
# -----------------------------------------------------------------------------
# Start/Stop Script for the Cmccdata Dns Collect Server
#

. ./setEnv.sh
$JAVA_HOME/bin/reportInv -Xms1024m -Xmx2048m -Djava.security.policy=java.policy -Djava.awt.headless=true -Dfile.encoding=utf-8 -classpath $CLASSPATH com.voole.ad.main.StartUp


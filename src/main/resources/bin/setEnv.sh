# -----------------------------------------------------------------------------
#  Set CLASSPATH and Java options
#
# -----------------------------------------------------------------------------

#set JAVA_HOME & SUPSERSSP_HOME here
JAVA_HOME=/opt/jdk1.8.0_25;export JAVA_HOME
SUPSERSSP_HOME=..;export SUPSERSSP_HOME

# Make sure prerequisite environment variables are set
if [ -z "$JAVA_HOME" ]; then
  echo "The JAVA_HOME environment variable is not defined"
  echo "This environment variable is needed to run this program"
  exit 1
fi
if [ -z "$SUPSERSSP_HOME" ]; then
  echo "The SUPSERSSP_HOME environment variable is not defined"
  echo "This environment variable is needed to run this program"
  exit 1
fi
if [ ! -r "$SUPSERSSP_HOME"/bin/setEnv.sh ]; then
  echo "The SUPSERSSP_HOME environment variable is not defined correctly"
  echo "This environment variable is needed to run this program"
  exit 1
fi

# Set standard CLASSPATH
CLASSPATH="$JAVA_HOME"/jre/lib:"$SUPSERSSP_HOME":"$SUPSERSSP_HOME"/bin:"$SUPSERSSP_HOME"/conf:"$SUPSERSSP_HOME"/lib:"$SUPSERSSP_HOME"/classes:"$SUPSERSSP_HOME"/images:"$SUPSERSSP_HOME"/src

# Append jars to CLASSPATH
if [ -d "$SUPSERSSP_HOME"/lib ]; then
  for i in "$SUPSERSSP_HOME"/lib/*.jar; do
    CLASSPATH="$CLASSPATH":"$i"
  done
fi


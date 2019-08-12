sudo mkdir -p ~/downloads
HBASE_VERSION=1.2.4
if [ ! -f $HOME/downloads/hbase-$HBASE_VERSION-bin.tar.gz ]; then sudo wget -O $HOME/downloads/hbase-$HBASE_VERSION-bin.tar.gz https://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz; fi
sudo mv $HOME/downloads/hbase-$HBASE_VERSION-bin.tar.gz hbase-$HBASE_VERSION-bin.tar.gz && tar xzf hbase-$HBASE_VERSION-bin.tar.gz
sudo rm -f hbase-$HBASE_VERSION/conf/hbase-site.xml && sudo mv .travis/hbase/hbase-site.xml hbase-$HBASE_VERSION/conf
sudo hbase-$HBASE_VERSION/bin/start-hbase.sh
#!/bin/bash
version="0.1.0"
yum -y update

yum -y groupinstall "Development Tools"

#install python 2.7
yum -y install python27-devel.x86_64
wget https://bootstrap.pypa.io/get-pip.py
python27 ./get-pip.py 
# install zeroMQ
wget http://download.zeromq.org/zeromq-4.0.4.tar.gz
tar xzvf zeromq-4.0.4.tar.gz
cd zeromq-4.0.4
./config
make install
#install ipython
/usr/local/bin/pip2.7 install "ipython[all]"==2.4


createSparknotebookprofile(){
    sudo -u sparknotebook /usr/local/bin/ipython profile create sparknotebook
    rm -f ~sparknotebook/.ipython/profile_sparknotebook/ipython_config.py
    sudo -u sparknotebook cat << EOF >> ~sparknotebook/.ipython/profile_sparknotebook/ipython_config.py
# Configuration file for ipython.

c = get_config()

c.KernelManager.kernel_cmd = ["/usr/share/sparknotebook/bin/sparknotebook", 
#"-mem","28000",
 "--profile", "{connection_file}",
 "--parent"]
c.NotebookApp.ip = "*" # only add this line if you want IPython-notebook being open to the public
c.NotebookApp.open_browser = False # only add this line if you want to suppress opening a browser after IPython-notebook initialization
EOF
}


# Adding system user/group : sparknotebook and sparknotebook
if ! getent group | grep -q "^sparknotebook:" ;
then
    echo "Creating system group: sparknotebook"
    groupadd sparknotebook
fi
if ! getent passwd | grep -q "^sparknotebook:";
then
    echo "Creating system user: sparknotebook"
    useradd --gid sparknotebook --create-home --comment "SparkNotebook Interactive User" sparknotebook
fi

# install Spark Notebook
file="/tmp/sparknotebook-$version.zip"
if ! test -f "$file" 
then
	aws s3 cp s3://sparknotebook-public/sparknotebook/sparknotebook-$version.zip /tmp/
fi

unzip -o /tmp/sparknotebook-$version.zip -d /usr/share
rm -f /tmp/sparknotebook-$version.zip
rm -f /usr/share/sparknotebook
ln -s /usr/share/sparknotebook-$version /usr/share/sparknotebook

chown -R sparknotebook:sparknotebook  /usr/share/sparknotebook-$version
chown  sparknotebook:sparknotebook  /usr/share/sparknotebook

#install ipython init.d scripts
mkdir -p /var/log/sparknotebook
chown sparknotebook:sparknotebook /var/log/sparknotebook
mkdir -p /etc/default/sparknotebook
chown sparknotebook:sparknotebook /etc/default/sparknotebook
mkdir -p /var/run/sparknotebook
chown sparknotebook:sparknotebook /var/run/sparknotebook
sudo -u sparknotebook ipython profile create sparknotebook

createSparknotebookprofile
chmod +x /etc/init.d/sparknotebook

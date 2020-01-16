sudo -u postgres psql -c "CREATE USER structqldemo PASSWORD 'structqlpw';"
sudo -u postgres psql -c "CREATE DATABASE testdb;"
sudo service postgresql start
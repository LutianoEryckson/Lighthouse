# Criar banco, usuário e senha; garante as permissões
!sudo -u postgres psql -c "CREATE DATABASE banvic;"
!sudo -u postgres psql -c "CREATE USER data_engineer WITH PASSWORD 'v3rysecur&pas5w0rd';"
!sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE banvic TO data_engineer;"
# Altan SFTP

Altan SFTP script automater is a script for get files from the SFTP altan server and then process al the csv files to insert in a table sftp under altan_seq. For get credentials and more info on this database ask your db administrator.

Basically what the script does is to get files from the SFTP, copy the files to the WIWI 
S3 altan-data/sftp directory and the process files to insert in to a database. It runs every 24 hour hours in a docker container. 

Be careful to run standalone since it will insert data on the sftp table.

The script runs every 24 hours with a cron job.

If the script is running inside a Docker Container you have to 
run with this command

Its IMPERATIVE to mount the volume for acess the awscli credentials o the ec2 container

```
 docker run --network=host --rm -v "$HOME/.aws:/root/.aws:rw" -it -e DB_HOST_WIWI_MS -e DB_USER_WIWI_MS -e DB_PASSWORD_WIWI_MS -e DB_DATABASE_NAME_WIWI_MS -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e ALTAN_SFTP_USER -e ALTAN_SFTP_PASSWORD imageidcontainer
```


# DataIntensive
Be sure the server is running:
```{bash}
ping 35.175.92.154
```
If it is not, ask me (Dani) to restart the instance from AWS.

If it is running, be sure Hadoop is running properly:
http://35.175.92.154:9870

If it is not running connect to the server via SSH and start Hadoop daemons:
```{bash}
ssh -i "ID2221keys.pem" ubuntu@ec2-35-175-92-154.compute-1.amazonaws.com
./start-hadoop.sh
```
You should be able tou run the scripts now:
Just run [this script](run.sh).



#!/bin/bash
###############################################################
# author:	wen tian				      #
# date	:	2021-8-17				      #
# versin:	v0.1				              #
# hive迁移脚本,上TB数据不建议使用			      #
###############################################################
read -p  "please input hive user:" user
read -s  -p  "please input hive user password:" pass

echo

PS3="In what way migration? "
#read -p "In what way migration? DB/TB >>>> " W
CHMOD="sudo -u hdfs hdfs dfs -chmod 777 /data"
MKDIR="sudo -u hdfs hdfs dfs -mkdir /data"

TEST(){
	EXITS="sudo -u hdfs hdfs dfs -ls  /data"
	$EXITS &>/dev/null
	if [ $? -eq 0 ];then
		echo -e "\033[1;33mwarn:IN HDFS /data directory to exist\033[0m"
		read -p "Continue? Y/N " CONTI
		echo -e "\033[1;35minfo:waiting ......\033[0m"
		if [[ $CONTI == N ]];then
			exit
		fi
	else
		$MKDIR
		$CHMOD
		
	fi

}


CONN(){
	beeline -n$user -p$pass -e  "$1" 2>/dev/null | sed -e "s/|//g" -e "s/-//g"  -e "s/+//g" | grep -v "^$" |sed "1d"
}


DB(){
	TEST
        ALL_DB=`CONN 'show databases;'`
 	for i in $ALL_DB;do
		tb=`beeline -n$user -p$pass -e "use $i;show tables" 2>/dev/null | sed -e "s/|//g" -e "s/-//g"  -e "s/+//g" | grep -v "^$" |sed "1d"|wc -w`
		count=0
		for a in `beeline -n$user -p$pass -e "use $i;show tables" 2>/dev/null | sed -e "s/|//g" -e "s/-//g"  -e "s/+//g" | grep -v "^$" |sed "1d"`;do
			beeline -n$user -p$pass -e "export table $i.$a to '/data/$i.$a;'" 2>/dev/null
			if [  $? -eq 0 ];then
				let count+=1
				printf  "\e[1;32mSUSSES [%d/%s:%d]\r"  $count $i $tb 
			else 
				echo -e "\033[1;31merror: $i.$a\033[0m"      
                                exit
			fi
		done
	done	
}


TB(){
	TEST
	CONN "show databases;" 2>/dev/null
        read -p "please input database name:" DB_NAME
        CONN "use  $DB_NAME;show tables;" 2>/dev/null
        read -p "please input table name:" TB_NAME
	beeline -n$user -p$pass -e "export table $DB_NAME.$TB_NAME to '/data/$DB_NAME.$TB_NAME';"
	if [ $? -eq 0 ];then
		echo -e "\033[1;32msuccess\033[0m"
	else
		echo -e "\033[1;31mERROR $DB_NAME.$TB_NAME;\033[0m"
		exit 1
	fi
}

GET(){
	read -p "Download the hdfs file  to /home/hdfs/data ,continue? Y/N " get

		ls /home/hdfs/data &>/dev/null
			if [  $? -eq 0 ];then
				echo -e "\033[1;31mThe /home/hdfs/data directory already exists on the local file system\033[0m"
				exit 99		
			fi 

	case $get in 
		N)
			exit 1
			;;
		Y)
			echo -e "\033[1;35mdownloading ......\033[0m"
			sudo -u hdfs hdfs dfs  -get /data /home/hdfs/
			;;
		*)
			echo -e "\033[1;31mERROR!\033[0m"
			exit 1
	
	esac

}

CD="/home/hdfs"
tar="/home/hdfs/data.tar.gz"

SCP(){
read -p "Ensure that /home/hdfs/data* does not exist on the local file system on the remote host,Continue? Y/N " DIR
case $DIR in
	N)
		echo -e "\033[1;34mmbye~\033[0m"
		exit 
	;;
	Y)
		cd $CD;tar czvf data.tar.gz data  &>/dev/null
		cd $CD;scp $tar exe.sh $1@$2:/home/hdfs/ &>/dev/null
		if [ $? -eq 0 ];then echo -e "\033[1;32msend to $2:$tar\033[0m" ;else echo -e "\033[1;31msend false\033[0m";fi
	;;
	*)
		echo -e "\033[1;31mInput error\033[0m"
		exit 1
	;;
esac
}

read -p "Enter remote user:" MOVER_USER
read -p "Enter remote host:" MOVER_HOST

cat > $CD/exe.sh << eof
set -x 
#!/bin/bash
tar xf $tar -C /home/hdfs 
if [ `sudo -u hdfs hdfs dfs -ls /data 2>/dev/null| wc -l`  -eq  "0"  ];then 
	sudo -u hdfs hdfs dfs -put /home/hdfs/data /
	$CHMOD
	sudo -u hdfs hdfs dfs -ls /data
else
	echo -e '\033[1;31merror:HDFS /data exists on the remote host!\033[0m'
	exit
fi
eof

IMPORT(){
	ssh $MOVER_USER@$MOVER_HOST "bash $CD/exe.sh;rm -f $CD/exe.sh"

}

select W in DATABASE_ALL TABLE_SELECT  PARTITION_SELECT;do
	case $W in 
		DATABASE_ALL)
			DB
			GET
			SCP $MOVER_USER $MOVER_HOST
			IMPORT
			break
			;;
		TABLE_SELECT)
			TB
			GET
			SCP $MOVER_USER $MOVER_HOST
			IMPORT 
			break
			;;
		PARTITION_SELECT)
			echo "PARTITION_SELECT"
			GET
			break
			;;
		*)
			echo -e "\033[1;31merror!!\033[0m"
			break
	esac	
done

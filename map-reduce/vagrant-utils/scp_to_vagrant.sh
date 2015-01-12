OPTIONS=`vagrant ssh-config | awk -v ORS=' ' '{print "-o " $1 "=" $2}'`

scp ${OPTIONS} $1  vagrant@$3:$2


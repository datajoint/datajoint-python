#! /bin/sh
sed "s|{{SUBDOMAINS}}|${SUBDOMAINS}|g" /base.conf | tee /etc/nginx/conf.d/base.conf
sed -i "s|{{URL}}|${URL}|g" /etc/nginx/conf.d/base.conf
sed -i "s|{{MINIO_SERVER}}|${MINIO_SERVER}|g" /etc/nginx/conf.d/base.conf
sed -i "s|{{REDIRECT_SERVER}}|${REDIRECT_SERVER}|g" /etc/nginx/conf.d/base.conf
sed "s|{{MYSQL_SERVER}}|${MYSQL_SERVER}|g" /nginx.conf | tee /etc/nginx/nginx.conf
nginx -g "daemon off;"

#! /bin/sh

sed "s|{{SUBDOMAINS}}|${SUBDOMAINS}|g" /base.conf | tee /etc/nginx/conf.d/base.conf
sed -i "s|{{URL}}|${URL}|g" /etc/nginx/conf.d/base.conf
sed -i "s|{{MINIO_SERVER}}|${MINIO_SERVER}|g" /etc/nginx/conf.d/base.conf
# tail -f /dev/null

nginx -g "daemon off;"
# nginx

# echo "Waiting for initial certs"
# while [ ! -d /etc/letsencrypt/archive/${SUBDOMAINS}.${URL} ]; do
#     sleep 5
# done

# echo "Enabling SSL feature"
# mv /ssl.conf /etc/nginx/conf.d/ssl.conf
# nginx -s reload

# inotifywait -m /etc/letsencrypt/archive/${SUBDOMAINS}.${URL} |
#     while read path action file; do
#         if [ "$(echo $action | grep MODIFY)" ] || [ "$(echo $action | grep CREATE)" ] || [ "$(echo $action | grep MOVE)" ]; then
#             echo "Renewal: Reloading NGINX since $file issue $action event"
#             nginx -s reload
#         fi
#     done
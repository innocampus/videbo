#!/bin/bash

config_file="config.ini"
content_segments=('general' 'lms' 'content')
encoder_segments=('general' 'lms' 'encoder')

sed -i '/^#/ d' "$config_file"
awk -v RS= '{print > ("config-fragment-" NR ".ini")}' "$config_file"

for fragment in ./config-fragment-*; do
	segment_name=$(head -n 1 "$fragment" | sed -e 's/[^A-Za-z0-9._-]//g')
	mv "$fragment" "segment-$segment_name.ini"
done

#cat segment-general.ini > content.config.ini

for f in "${content_segments[@]}"; do
	(cat "segment-${f}.ini"; echo) >> content.config.ini
done

sed -i "s/^domain =.*/domain = {{ domain }}/" content.config.ini
sed -i "s/^internal_api_secret =.*/internal_api_secret = {{ internal_api_secret }}/" content.config.ini
sed -i "s/^api_secret =.*/api_secret = {{ api_secret }}/" content.config.ini
sed -i "s/^max_clients =.*/max_clients = {{ max_clients }}/" content.config.ini

mv content.config.ini ansible/init-content-node/templates/config.ini.j2

for f in "${encoder_segments[@]}"; do
	(cat "segment-${f}.ini"; echo) >> encoder.config.ini
done

sed -i "s/^domain =.*/domain = {{ domain }}/" encoder.config.ini
sed -i "s/^internal_api_secret =.*/internal_api_secret = {{ internal_api_secret }}/" encoder.config.ini
sed -i "s/^api_secret =.*/api_secret = {{ api_secret }}/" encoder.config.ini
sed -i "s/^max_streams =.*/max_streams = {{ max_streams }}/" encoder.config.ini

mv encoder.config.ini ansible/init-encoder-node/templates/config.ini.j2

rm segment-*.ini

mkdir /opt/livestreaming-server
tar -czvf /opt/livestreaming-server/livestream_content_deploy.tar.gz \
      --exclude="*/__pycache__" \
      ./livestreaming ./requirements.txt

tar -czvf /opt/livestreaming-server/livestream_encoder_deploy.tar.gz \
      --exclude="*/__pycache__" \
      ./livestreaming ./requirements.txt
#! /bin/bash

# used to generate certs for nebula-graph
# usage:
# 1. ./cert.sh root
# 2. ./cert.sh server
# 3. ./cert.sh client

# config:
# server:
# --cert_path=server.crt
# --key_path=server.key
# --ca_path=root.crt

# client: follow per client repo

set -eu
DN_C=CH
DN_O=vesoft
DN_OU=Eng
DN_CN=
DN_EMAIL=harris.chu@xxxx.com

SERVER_ADDRESS_IP=""
SERVER_ADDRESS_DNS="localhost graphd0 graphd1 graphd2"
CLIENT_ADDRESS_IP=""
CLIENT_ADDRESS_DNS=""

if [ $# != 1 ]; then
  echo "USAGE: $0 <root|server|client>"
  exit 1;
fi

function gen_cert {
	cert_type=$1
	subject_name_ip=$2
	subject_name_dns=$3
	cat << EOF > ${cert_type}.cnf
[ req ]
default_bits = 2048
prompt = no
distinguished_name = dn
req_extensions = req_ext

[ dn ]
C = CH
O = test-ca
CN = ${cert_type}

[ v3_ca ]
basicConstraints = critical,CA:TRUE
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer:always

[ req_ext ]
subjectAltName = @alt_names

[alt_names]
IP.1 = 127.0.0.1
EOF
	if [ "$subject_name_ip" != "" ];then
		start=2
		for i in ${subject_name_ip}; do
			cat << EOF >> ${cert_type}.cnf
IP.${start} = ${i}
EOF
			start=$(($start+1))
		done
	fi
	if [ "$subject_name_dns" != "" ];then
		start=1
		for i in ${subject_name_dns}; do
			cat << EOF >> ${cert_type}.cnf
DNS.${start} = ${i}
EOF
			start=$(($start+1))
		done
	fi
	openssl genrsa -out ${cert_type}.key 1024
	openssl req -new -config ${cert_type}.cnf -out ${cert_type}.csr -key ${cert_type}.key
	if [ ${cert_type} == "root" ]; then
		openssl x509 -req -in ${cert_type}.csr -out ${cert_type}.crt -extfile ${cert_type}.cnf -extensions v3_ca -signkey ${cert_type}.key -CAcreateserial -days 3650
	else
		openssl x509 -req -in ${cert_type}.csr -out ${cert_type}.crt -CA root.crt -CAkey root.key -CAcreateserial -days 3650 -extfile ${cert_type}.cnf -extensions req_ext
	fi

}

cert_type=${1}
if [ ${cert_type} != "root" ] && [ ! -e root.crt ];then
	echo "root.crt not exist"
	exit 1
fi
echo "generate ${cert_type} cert"
if [ ${cert_type} == "server" ]; then
	gen_cert ${cert_type} "${SERVER_ADDRESS_IP[*]}" "${SERVER_ADDRESS_DNS[*]}"
elif [ ${cert_type} == "client" ]; then
	gen_cert ${cert_type} "${CLIENT_ADDRESS_IP[*]}" "${CLIENT_ADDRESS_DNS[*]}"
else
	gen_cert ${cert_type} "" ""
fi
echo "finish"
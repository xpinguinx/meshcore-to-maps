#!/bin/bash

# 📡 Config MQTT per il SERVER centrale
# ATTENZIONE: queste sono le variabili che mctomqtt.py si aspetta davvero (prefisso MCTOMQTT_)
export MCTOMQTT_MQTT1_ENABLED="true"
export MCTOMQTT_MQTT1_SERVER="nodi.meshcoreitalia.it"         # Server MQTT nodi.meshcoreitalia.it
export MCTOMQTT_MQTT1_PORT="1883"

export MCTOMQTT_MQTT1_USERNAME="meshcore"
export MCTOMQTT_MQTT1_PASSWORD="meshcore25"

# QoS e retain
export MCTOMQTT_MQTT1_QOS="0"
export MCTOMQTT_MQTT1_RETAIN="true"

# NAMESPACE TEMATICO (uguale a quello che usi sul server decoder)
# Esempio: meshcore/FCO/{PUBLIC_KEY}/packets e status
export MCTOMQTT_IATA="ITA"
export MCTOMQTT_TOPIC_PACKETS="meshcore/${MCTOMQTT_IATA}/{PUBLIC_KEY}/packets"
export MCTOMQTT_TOPIC_STATUS="meshcore/${MCTOMQTT_IATA}/{PUBLIC_KEY}/status"

# Porta seriale (se diversa, aggiorna qui)
export MCTOMQTT_SERIAL_PORTS="/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0"
export MCTOMQTT_SERIAL_BAUD_RATE="115200"

cd /home/meshcore/meshcore-to-maps
./venv/bin/python3 mctomqtt.py

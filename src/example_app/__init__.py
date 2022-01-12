# Mini Flask example app.
from confluent_kafka import Producer
from flask import Flask,request,jsonify
import socket
import json
import pprint

app = Flask(__name__)

#Conexión a Kafka
conf = {'bootstrap.servers': "172.16.100.22:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

#Funciones de producción de Topicos
def producir_ventas(data):
        producer.produce("Log_ventas", key="ventas", value=data)
        producer.flush()

def producir_CRM(data):
        producer.produce("Log_CRM", key="CRM", value=data)
        producer.flush()

def producir_Contactos(data):
        producer.produce("Log_Contactos", key="contactos", value=data)
        producer.flush()

def producir_Pagos(data):
        producer.produce("Log_Pagos", key="pagos", value=data)
        producer.flush()

def producir_Casos(data):
        producer.produce("Log_Casos", key="casos", value=data)
        producer.flush()

def producir_medios(data):
        producer.produce("Log_medios", key="medios", value=data)
        producer.flush()

def producir_dwh(data):
        producer.produce("Log_dwh", key="dwh", value=data)
        producer.flush()
#Funciones de prod_message  


#pprint.pprint(respuesta)

@app.route("/")
def root():
    return "Hola API ECOQUINTAS"

@app.route("/producer/ventas/",methods=['POST'])
def prod_ventas():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"Ventas":content}
        producir_ventas(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/CRM/",methods=['POST'])
def prod_CRM():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json
        content["ip"]=request.remote_addr
        respuesta={"CRM":content}
        producir_CRM(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/contactos/",methods=['POST'])
def prod_ontactos():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json
        content["ip"]=request.remote_addr
        respuesta={"Contactos":content}
        producir_Contactos(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/pagos/",methods=['POST'])
def prod_pagos():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json
        content["ip"]=request.remote_addr
        respuesta={"Pagos":content}
        producir_Pagos(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/casos/",methods=['POST'])
def prod_casos():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json
        content["ip"]=request.remote_addr
        respuesta={"Casos":content}
        producir_Casos(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/medios/",methods=['POST'])
def prod_medios():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json
        content["ip"]=request.remote_addr
        respuesta={"Medios":content}
        producir_medios(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/dwh/",methods=['POST'])
def prod_dwh():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json
        content["ip"]=request.remote_addr
        respuesta={"dwh":content}
        producir_dwh(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"
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

#Topicos de Ventas____________________________________________________________________________________________________________________________
def producir_ventas_pagos(data):
        producer.produce("topic_ventas_pagos", key="pagos", value=data)
        producer.flush()
def producir_ventas_venta(data):
        producer.produce("topic_ventas_venta", key="ventas", value=data)
        producer.flush()
def producir_ventas_cotizacion(data):
        producer.produce("topic_ventas_cotizacion", key="cotizador", value=data)
        producer.flush()
def producir_ventas_fe(data):
        producer.produce("topic_ventas_fe", key="facturaelectronica", value=data)
        producer.flush()
def producir_ventas_int_conta(data):
        producer.produce("topic_ventas_int_conta", key="dag-asientosContables", value=data)
        producer.flush()
#______________________________________________________________________________________________________________________________________________

#Topicos de CRM________________________________________________________________________________________________________________________________
def producir_crm_clientes(data):
        producer.produce("topic_crm_clientes", key="clientes", value=data)
        producer.flush()
def producir_crm_contactos(data):
        producer.produce("topic_crm_contactos", key="contactos", value=data)
        producer.flush()
def producir_crm_casos(data):
        producer.produce("topic_crm_casos", key="casos", value=data)
        producer.flush()
def producir_crm_camp(data):
        producer.produce("topic_crm_camp", key="campañas", value=data)
        producer.flush()
def producir_int_cierres(data):
        producer.produce("topic_int_cierres", key="dag-cierres", value=data)
        producer.flush()
#______________________________________________________________________________________________________________________________________________



#Funciones de prod_message
@app.route("/")
def root():
    return "Hola API ECOQUINTAS"

#Funciones de prod_ventas______________________________________________________________________________________________________________________
@app.route("/producer/ventas/pago/",methods=['POST'])
def prod_ventas_pagos():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"Ventas":content}
        producir_ventas_pagos(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/ventas/venta/",methods=['POST'])
def prod_ventas_venta():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"Ventas":content}
        producir_ventas_venta(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/ventas/cotizacion/",methods=['POST'])
def prod_ventas_cotizacion():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"Ventas":content}
        producir_ventas_cotizacion(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/ventas/facturaelectronica/",methods=['POST'])
def prod_ventas_fe():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"Ventas":content}
        producir_ventas_fe(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/integracion/conta/",methods=['POST'])
def prod_int_conta():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"Ventas":content}
        producir_ventas_int_conta(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

#Funciones de prod_crm______________________________________________________________________________________________________________________

@app.route("/producer/crm/clientes/",methods=['POST'])
def prod_crm_clientes():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"CRM":content}
        producir_crm_clientes(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/crm/contactos/",methods=['POST'])
def prod_ventas_contactos():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"CRM":content}
        producir_crm_contactos(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/crm/casos/",methods=['POST'])
def prod_crm_casos():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"CRM":content}
        producir_crm_casos(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/crm/casos/",methods=['POST'])
def prod_crm_casos():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"CRM":content}
        producir_crm_casos(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/crm/email/",methods=['POST'])
def prod_crm_email():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"CRM":content}
        producir_crm_camp(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"

@app.route("/producer/integracion/cierres/",methods=['POST'])
def prod_int_cierres():
    token = request.headers.get('token')
    if (token=="3e26b17c-3e96-40d6-91fa-7f355bf2c570"):
        content = request.json 
        content["ip"]=request.remote_addr
        respuesta={"Int":content}
        producir_int_cierres(json.dumps(respuesta))
        return respuesta
    else:
        return "Token invalido"
#______________________________________________________________________________________________________________________________________________________________________
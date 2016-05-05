from starbase import Connection
import urllib.request,json,pprint,sys

"""
Daniel Correa - Cristina Extremera
Entrega 3, HBase, MDAD

Implementación de busqueda de restaurantes consultando el open data de Cáceres, indexándolo en HBase y buscando por valor. 
"""

def consultar_opendata():
	"""
	Permite consultar el open data de Cáceres de restaurantes y recibirlo en formato JSON
	"""
	response = urllib.request.urlopen("http://opendata.caceres.es/GetData/GetData?dataset=om:Restaurante&format=json")
	data = response.read().decode(response.info().get_param('charset') or 'utf-8')
	data_json = json.loads(data)
	return data_json['results']['bindings']

def indexar(json_data,c):
	"""
	Permite indexar la información obtenida del open data en la tabla restaurantes
	"""
	tabla = c.table('restaurantes')
	result = tabla.create('uri','geo_long','om_tenedores','schema_url','geo_lat','schema_email','schema_telephone','rdfs_label','om_capacidadPersonas','om_categoriaRestaurante','schema_address_streetAddress','schema_address_addressLocality','schema_address_addressCountry','schema_address_postalCode')
	if result:
		print('Tabla creada:'+str(result))
	else:
		print('La tabla ya existe, no ha sido creada')
	i = 0
	for restaurante in json_data:
		tabla.insert(str(i),restaurante)
		i+=1
	return tabla

def imprimir_restaurante(res):
	"""
	Permite imprimir la información obtenida de la busqueda en formato entendible
	"""
	print("Nombre: " + str(res['rdfs_label']['value']))
	print("Teléfono: " + str(res['schema_telephone']['value']))
	print("Dirección: " + str(res['schema_address_streetAddress']['value']))

def main():
	"""
	Se encarga de gestionar la comunicación entre hbase y usuario
	"""
	c = Connection()
	data_json = consultar_opendata()
	
	tabla = indexar(data_json,c)

	print("Buscador de restaurantes!")
	opcion = 0
	while opcion!=2:
		print("Menu: \n - 1 para buscar por id \n - 2 para finalizar")
		try:
			opcion = int(input("Digite la opción que desea: "))
		except:
			opcion=0
		if opcion == 1:
			opcion = input("Ingresa el id del restaurante: ")
			imprimir_restaurante(tabla.fetch(opcion,['rdfs_label:value','schema_telephone:value','schema_address_streetAddress:value']))
		elif opcion == 2:
			sys.exit()
		else:
			print("Opción inválida")
main()
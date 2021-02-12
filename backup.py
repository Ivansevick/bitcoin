import boto3
import pyodbc
import os 
try:
    import configparser
except:
    from six.moves import configparser
# Ubicacion de backuups de base de datos NOTA: NO ES NECESARIO MODIFICAR!!! 
cliente = 'EnriqueRomero/'
server_backup_path = r'c:\\BackUpProyect\\dbbacks\\'
awsBucket = 'sql-folders-backups'

def decrypt(key, encryped):
	msg = []
	for i, c in enumerate(encryped):
		key_c = ord(key[i % len(key)])
		enc_c = ord(c)
		msg.append(chr((enc_c - key_c) % 128))
	return ''.join(msg)
def defineClient(user,pwd,key):
	s3 = boto3.client(
		's3',
		aws_access_key_id=decrypt(key,user),
		aws_secret_access_key=decrypt(key,pwd),
	)
	return s3

def createConnection(config):
	#conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-8RKIRE2\\MSSQLSERVER14;Trusted_Connection=yes;UID=sa;PWD=123456', autocommit=True)
	conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + config['instance']+ ';Trusted_Connection=yes;UID=' + config['user'] + ';PWD='+ config['pwd'], autocommit=True)
	return conn
# Listamos todas las bases de datos disponibles
def list_databases(conn_obj):
	dbs = []
	cur = conn_obj.cursor()
	result = cur.execute('SELECT name from sysdatabases').fetchall()
	cur.close()
	for db in result:
		if db == 'master' or db == 'tempdb' or db == 'model' or db == 'msdb':
			print('DefaultDatabase')
		else:
			dbs.append(db[0])
	return dbs

def backup_db(conn_obj, db_name, server_backup_path):
	cur = conn_obj.cursor()
	try:
		cur.execute('BACKUP DATABASE ? TO DISK=?', [db_name, server_backup_path + db_name + r'_sql.bak'])

		while cur.nextset(): 
			pass
		print('Back is done')
		cur.close()
	except Exception as e:
		print(e)
		print ('cant backup: ' + db_name)


try:
	with open('helpers\\config.mbge','r') as configFile:
		conf =configFile.read().split('|')
		configDic ={
			"instance" 	: conf[0],
			"user"		: conf[1],
			"pwd"		: conf[2],
			"folders"	: conf[3].split(','),
			"IAMkey"	: conf[4],
			"IAMSkey"	: conf[5],
			"decryptKey": conf[6]
		}
except FileNotFoundError as e:
	with open('helpers\\config.mbge','w') as configFile:
		instance = input('Introduzca la instancia a respaldar: ')
		sqlUser	= input('Usuario SQL: ')
		sqlPwd	= input('Contraseña SQL: ')
		folders	= input('Ruta carpetas a respaldar separados por ","')
		IAMkey	= input('Usuario AWS: ')
		IAMSkey	= input('Contraseña AWS: ')
		decryptKey = input('Lave de desebcriptacion: ')
		
		configFile.write(instance + '|' + sqlUser + '|' + sqlPwd + '|' + folders + '|' + IAMkey + '|' + IAMSkey + '|' + decryptKey)
		configFile.close()

		configDic ={
			"instance" 	: instance,
			"user"		: sqlUser,
			"pwd"		: sqlPwd,
			"folders"	: folders.split(','),
			"IAMkey"	: IAMkey,
			"IAMSkey"	: IAMSkey,
			"decryptKey": decryptKey
		}
conn = createConnection(configDic)

# Generamps lista de cada base de datos existente
dbs = list_databases(conn)

#Creamos una backup por cada base encontrada.
for db in dbs:
	print(db)
	backup_db(conn, db, server_backup_path)

# cerramos la conexion a SQL.
conn.close()

s3 = defineClient(configDic['IAMkey'],configDic['IAMSkey'],configDic['decryptKey'])


files = []
# r=root, d=directories, f = files
for r, d, f in os.walk(server_backup_path):
	for file in f:
		files.append(os.path.join(r, file))
folderitems =[]
for folder in configDic['folders']:
	for r, d, f in os.walk(folder):
		for file in f:
			folderitems.append(os.path.join(r, file))


for f in files:
	s3.upload_file(f,awsBucket,cliente + 'backups/' + f.split('\\')[-1])
for f in folderitems:
	s3.upload_file(f,awsBucket,cliente + 'backups/' + f.replace('\\','/'))

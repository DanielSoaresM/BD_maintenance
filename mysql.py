 # -*- coding: utf-8 -*-
#######
# Nombre mysql.py
# Version: 1.0
# Autor: Mauricio Olmos
# Funcionalidad: Control y manejo de la conexión a mysql con persistencia y consulta instantanea
#                Donde la consulta instantanea abre y cierra la conexion, mientras que la persistente
#                Mantiene viva la misma conexión para el envío de consultas recurrentes
#
#######

import pymysql

class DBase:
    cursor = None
    conn = None
    _env = 'DEV'
    _commit = 0

    def __init__(self, commit=0, env="DEV"):
        self._env = env
        self._commit = commit

    def connectMysql(self): 
        # Conexion estandar
        self._env = self._env.upper()
        if self._env == 'PROD':
            self.conn = pymysql.connect(host="db-izimedia.cwfsmqfn6erb.us-east-1.rds.amazonaws.com", user="scraper_electronico", 
            passwd="x$3ewe2qA1", db="accionmedia")
        else:
            self.conn = pymysql.connect(host="core.realit.cl", user="infoldernews", passwd="));!34ras&xAX", db="accionmedia")
        self.cursor = self.conn.cursor()

    def closeMysql(self): #used
        self.cursor.close()                 # Cerrar el cursor
        self.conn.close()                   # Cerrar la conexión
        return True

    def runQuery(self, query='', args=None , mogry=None): 
        # mogry = True muestra la query a insertar
        if mogry == True:
            print(self.cursor.mogrify(query, args))
        if not args:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, args)
        if query.upper().startswith('SELECT') or query.upper().startswith('CALL GET'):
            # Solo para las consultas SELECT o llamadas a procedures CALL
            if query.upper().startswith('CALL GET') and self._commit == 1:
                self.conn.commit() 
            data = self.cursor.fetchall()   # Traer los resultados de un select
        else:
            if self._commit == 1:
                self.conn.commit()              # Hacer efectiva la escritura de datos
                data = self.cursor.lastrowid
        return data


    def runQueryMany(self, query, args =None, mogry=None):   
        if mogry == True: 
            print(self.cursor.mogrify(query, args[0]))
        inserts = self.cursor.executemany(query,args)
        if self._commit == 1:
            self.conn.commit()              # Hacer efectiva la escritura de datos
        return inserts

from mysql import DBase



class MantenedorBD:
    bd = None

    def __init__(self):
        bd = DBase(commit=1,env="PROD")


    def limpiar_impresa(self):
        pass
    

    def clean_electronic(self,med_id=None,date_ini=None,date_end=None):
        """Marca notas repetidas de medios electronicos y los marca como 0,ademas de cambiar las notas en el modulo pr a la nota mas vieja"""
        # buscar ids de not_ctrl donde type 2 y status manticore 0
        # busca copias en medios en un rango de fechas
        
        if med_id !=None:
            sql = "SELECT MIN(id),nota.urlfuente, count(*) AS clones FROM not_nota AS nota,not_ctrl AS ctrl WHERE nota.id = ctrl.not_nota_id AND ctrl.status_manticore = 1 AND nota.medio_id = {} AND nota.tipo = 2 GROUP BY nota.urlfuente HAVING clones > 1" 
            sql = sql.format(med_id)
        if date_ini != None and date_end != None:
            sql = "SELECT MIN(id),nota.urlfuente, count(*) as clones FROM not_nota as nota,not_ctrl as ctrl WHERE nota.id = ctrl.not_nota_id and ctrl.status_manticore = 1 and nota.fpublicacion >= '{}' AND nota.fpublicacion <= '{}' AND nota.tipo = 2 GROUP BY nota.urlfuente HAVING clones > 1"
            sql = sql.format((date_ini,date_end))
        self.bd.connectMysql()
        repeated_urls = self.bd.runQuery(sql)
        sql = sql.format()
        for url in repeated_urls:
            keep_id = url[0]
            repeated_url = url[1]
            if med_id != None:
                base_sql = "SELECT id FROM not_nota WHERE medio_id = {} AND id != {} url = '{}'"
                sql = base_sql.format((med_id,keep_id,repeated_url))
            if date_ini != None and date_end != None:
                base_sql = "SELECT id FROM not_nota WHERE fpublicaion AND id != {} url = '{}'"
                sql = base_sql.format((med_id,keep_id,repeated_url))

        
        
        
        
        
        
        
        
                    # eliminar datos en not_nota, not_ctrl, not_extra_atrr
            # selecccinar id de notas que tengan la misma url titulo pero tengan status 1
            # cambiar todas las notas en news_delibery con id vieja a id nueva

        # 
        pass
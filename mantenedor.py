from mysql import DBase
import re
from colorama import Fore


class Mantenedordb:
    db = None

    def __init__(self):
        self.db = DBase(commit=1,env="PROD")


    def limpiar_impresa(self):
        pass
    

################################################## LIMPIEZA ELECTRONICOS ##################################################


    def clean_electronic(self,med_id=None,date_ini=None,date_end=None):
        """Marca notas repetidas de medios electronicos y los marca como 0,ademas de cambiar las notas en el modulo pr a la nota mas vieja"""
        # buscar ids de not_ctrl donde type 2 y status manticore 0
        # busca copias en medios en un rango de fechas
        
        if med_id !=None:
            sql = "SELECT MIN(nota.id),nota.urlfuente, count(*) AS clones FROM not_nota AS nota,not_ctrl AS ctrl WHERE nota.id = ctrl.not_nota_id AND ctrl.status_manticore = 1 AND nota.medio_id = {} AND nota.tipo = 2 GROUP BY nota.urlfuente HAVING clones > 1" 
            sql = sql.format(med_id)
        if date_ini != None and date_end != None:
            sql = "SELECT MIN(id),nota.urlfuente, count(*) as clones FROM not_nota as nota,not_ctrl as ctrl WHERE nota.id = ctrl.not_nota_id and ctrl.status_manticore = 1 and nota.fpublicacion >= '{}' AND nota.fpublicacion <= '{}' AND nota.tipo = 2 GROUP BY nota.urlfuente HAVING clones > 1"
            sql = sql.format((date_ini,date_end))
        self.db.connectMysql()
        repeated_urls = self.db.runQuery(sql)
        sql = sql.format()
        for url in repeated_urls:
            keep_id = url[0]
            repeated_url = url[1]
            if med_id != None:
                base_sql = "SELECT id FROM not_nota WHERE medio_id = {} AND id != {} AND url = '{}'"
                res = base_sql.format((med_id,keep_id,repeated_url))
            if date_ini != None and date_end != None:
                base_sql = "SELECT id FROM not_nota WHERE {} <= fpublicaion and fpublicacion <= {} AND id != {} url = '{}'"
                res = base_sql.format((date_ini,date_end,med_id,keep_id,repeated_url))
            for x in res:
                print(x)








################################################## LIMPIEZA LOGS ###########################################################################



    def clean_logs(self, source_id):
        sql_logs = "SELECT id, value FROM grl_log WHERE reference_id = {}".format(source_id)
        sql_delete = "DELETE FROM grl_log WHERE id IN {}"
        logs = self.db.runQuery(sql_logs)
        self.mk_filter_obj(source_id)
        for i in self.filters["regex"]:
            try:
                exp = re.compile(i)
                ids = []
                print("regex: ", i)
                for log in logs:
                    if exp.findall(log[1]):
                        # print(Fore.GREEN + "BORRANDO LOG: id:{}, url:{}".format(log[0],log[1]))
                        # self.db.runQuery(sql_delete.format(log[0]))
                        ids.append(str(log[0]))
                        if len(ids) == 100:
                            in_query = "(" + ",".join(ids)  + ")"
                            self.db.runQuery(sql_delete.format(in_query),mogrify=True)
                            ids = []
                in_query = "(" + ",".join(ids)  + ")"
                if len(ids) > 0: 
                    self.db.runQuery(sql_delete.format(in_query),mogrify=True)
            except Exception as e:
                print("NO TOMO EXPRESION REGULAR:::: {}".format(e))
                print("BUSQUEDA CON FALLAS--> {}".format(i))


    def mk_filter_obj(self,source_id):
        sql_filters = "SELECT valor, tipo FROM med_excluirscrap WHERE medio_id = {} AND seccionscrap_id = 0".format(source_id)
        filters =  self.db.runQuery(sql_filters)
        self.filters = {"literal":[],"regex":[]}
        for filter in filters:
            if filter[1] == 1:
                self.filters["literal"].append(filter[0])
            if filter[1] == 2:
                self.filters["regex"].append(filter[0])



    def clean_all_filters(self):
        sql = "SELECT id, nombreact FROM med_general WHERE tipo = 2 AND estado = 1"
        sources = self.db.runQuery(sql)
        for source in sources:
            print(Fore.RED + "Limpiando medio: {}   {}".format(source[0],source[1]))
            self.clean_logs(source[0])
        
        
        
        
        
        
                    # eliminar datos en not_nota, not_ctrl, not_extra_atrr
            # selecccinar id de notas que tengan la misma url titulo pero tengan status 1
            # cambiar todas las notas en news_delibery con id vieja a id nueva

        # 
mantenedor = Mantenedordb()
mantenedor.clean_electronic(134)
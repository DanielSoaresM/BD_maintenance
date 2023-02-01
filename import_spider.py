import sys
from mysql import DBase
from colorama import Fore
############################################
#                                          #
#           importador de spiders          #
#                                          #
############################################

# 

class SpiderImport:
    # resumen colores 
    i = Fore.WHITE  #info 
    e = Fore.RED    #error
    h = Fore.YELLOW #highlight
    s = Fore.GREEN  #success
    r = Fore.RESET  #reset
    #print(f"{self.i}{self.r}")

    target_table = "not_nota_spider6"
    final_table = "not_nota"
    # final_table = "not_nota_tmp"

    meds = []


    def __init__(self,origin,destiny) -> None:
        self.origin = str(origin)
        self.destiny = str(destiny)
        self.db = DBase(1,"PROD")
        self.db.connectMysql()

    def get_meds_to_import(self):
        sql = f"SELECT DISTINCT medio_id FROM med_seccionscrap WHERE assigned_spider = {self.origin}"
        res = self.db.runQuery(sql)
        self.meds = list(map(lambda x:x[0],res))

        print(f"{self.i} Medios a trabajar: {str(self.meds)}{self.r}")

    def remove_duplicates(self,med_id:str)->None:
        """Remueve duplicados entre la tabla destino y la tabla origen """

        print(f"{self.h}Buscando registros repetidos de medio {med_id}{self.r}")
        sql = f"SELECT urlfuente, id FROM {self.target_table} WHERE medio_id = {med_id}"
        res = self.db.runQuery(sql)
        notes_to_delete = []
        new_notes = {}
        # new_notes = {n[0]:str(n[1]) for n in res}
        for reg in res:
            if reg[0] in new_notes:
                notes_to_delete.append(str(reg[1]))
            else:
                new_notes[reg[0]] = str(reg[1])



        sql = f"SELECT urlfuente FROM {self.final_table} WHERE medio_id = {med_id}"
        res = self.db.runQuery(sql)
        for url in list(map(lambda x:x[0],res)):
            if url in new_notes:
                notes_to_delete.append(str(new_notes[url]))
                print(f"{self.e}Nota Repetida: {url}{self.r}")
                if len(notes_to_delete)> 100:
                    ids = ",".join(notes_to_delete)
                    notes_to_delete = []
                    delete_sql = f"DELETE FROM {self.target_table} WHERE id in ({ids})"
                    self.db.runQuery(delete_sql)
                    print(delete_sql)
        if len(notes_to_delete)>0:
            ids = ",".join(notes_to_delete)
            notes_to_delete = []
            delete_sql = f"DELETE FROM {self.target_table} WHERE id in ({ids})"
            self.db.runQuery(delete_sql)
            print(delete_sql)


    def insert_new_notes(self,med:str):
        print(f"{self.h}Insertando registros del medio {med}{self.r}")
        sql = f"SELECT  titulo, cachetxt, fpublicacion, fcarga, hcarga, proveedor, \
                        medio_id, seccion, urlfuente, url_img, tipo, valor, tono, hex_bin \
                        FROM {self.target_table} WHERE medio_id = {med}"
        res = self.db.runQuery(sql)
        new_notes = []
        for reg in res:
            sql =   """INSERT INTO """ + self.final_table +""" (titulo, cachetxt, fpublicacion, fcarga, hcarga, proveedor, \
                    medio_id, seccion, urlfuente, url_img, tipo, valor, tono,hex_bin) \
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,hex(unhex(replace(uuid(),'-',''))))"""
            args = [reg[0],reg[1],reg[2],reg[3],reg[4],reg[5],reg[6],reg[7],reg[8],reg[9],reg[10],reg[11]]
            try:
                nota = str(self.db.runQuery(sql,args))
            except Exception as E:
                print(E)
                print(f"{self.e}ERROR INSERTADO NOTA {reg[8]} {self.r}")            
                raise Exception
            new_notes.append(nota)

            if len(new_notes)>100:
                ids = ",".join(new_notes)
                new_notes = []
                ctrl_sql = f"UPDATE not_ctrl SET ana_ctrl = 1, updated_at = NOW(), ana_updated_at = NOW() WHERE not_nota_id IN ({ids})"
                self.db.runQuery(ctrl_sql)

        if len(new_notes)>0:
            ids = ",".join(new_notes)
            new_notes = []
            ctrl_sql = f"UPDATE not_ctrl SET ana_ctrl = 1, updated_at = NOW(), ana_updated_at = NOW() WHERE not_nota_id IN ({ids})"
            self.db.runQuery(ctrl_sql)


    def move_to_new_spider(self,med:str):
        sql = f"UPDATE med_seccionscrap SET assigned_spider = {self.destiny} WHERE assigned_spider = {self.origin} AND medio_id = {med}"
        self.db.runQuery(sql)


    def del_old_table(self,med:str):
        sql = f"DELETE FROM {self.target_table} WHERE medio_id = {med}"
        self.db.runQuery(sql)

    

    def run(self):
        self.get_meds_to_import()
        for med in self.meds:
            s_med = str(med)
            self.remove_duplicates(s_med)
            self.insert_new_notes(s_med)
            self.move_to_new_spider(s_med)
            self.del_old_table(s_med)
            print(f"{self.s}MEDIO {med} COMPLETADO {self.r}")



if __name__ == "__main__":
    """importador de spiders"""
    params = sys.argv
    origin = params[1]
    destiny = params[2]
    input("DETENER SPIDERS y ANALIZERS DURANTE PROCESO, PRESIONE ENTER PARA CONTINUAR")
    si = SpiderImport(origin,destiny)
    si.run()
    input("PROCESO COMPLETO REINICIAR SPIDERS y ANALIZERS PARA TERMINAR")

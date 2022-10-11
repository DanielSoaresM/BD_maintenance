import locale
import os
import datetime
from tabnanny import check
import time
import requests
import sys
import urllib3
import ssl
import math
import socket
import demjson
from io import BytesIO
from mysql import DBase
from utils import S3_Upload
from PIL import Image
import pymysql
from pymysql.converters import escape_string

import cv2
import easyocr
import urllib3
import numpy as np
# from hunspell import Hunspell

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)





def get_paragraph(raw_result, x_ths=1, y_ths=0.5, mode = 'ltr'):
        """Recibe el resultado del OCR y retorna el texto obtenido de este, con lineas incluidas"""
        line_enders = [".","?","!"]
        # create basic attributes
        box_group = []
        for box in raw_result:
            all_x = [int(coord[0]) for coord in box[0]]
            all_y = [int(coord[1]) for coord in box[0]]
            min_x = min(all_x)
            max_x = max(all_x)
            min_y = min(all_y)
            max_y = max(all_y)
            height = max_y - min_y
            box_group.append([box[1], min_x, max_x, min_y, max_y, height, 0.5*(min_y+max_y), 0]) # last element indicates group
        # cluster boxes into paragraph
        current_group = 1
        first_letter = True
        while len([box for box in box_group if box[7]==0]) > 0:
            box_group0 = [box for box in box_group if box[7]==0] # group0 = non-group
            # new group
            if len([box for box in box_group if box[7]==current_group]) == 0:
                box_group0[0][7] = current_group # assign first box to form new group
            # try to add group
            else:
                current_box_group = [box for box in box_group if box[7]==current_group]
                mean_height = np.mean([box[5] for box in current_box_group])
                min_gx = min([box[1] for box in current_box_group]) - x_ths*mean_height
                max_gx = max([box[2] for box in current_box_group]) + x_ths*mean_height
                min_gy = min([box[3] for box in current_box_group]) - y_ths*mean_height
                max_gy = max([box[4] for box in current_box_group]) + y_ths*mean_height
                add_box = False
                for box in box_group0:
                    same_horizontal_level = (min_gx<=box[1]<=max_gx) or (min_gx<=box[2]<=max_gx)
                    same_vertical_level = (min_gy<=box[3]<=max_gy) or (min_gy<=box[4]<=max_gy)
                    if same_horizontal_level and same_vertical_level:
                        box[7] = current_group
                        add_box = True
                        break
                # cannot add more box, go to next group
                if add_box==False:
                    current_group += 1
        # arrage order in paragraph
        result = []
        parragraph_jump = False
        end_of_line = False
        last_box = None
        # print(box_group)
        for i in set(box[7] for box in box_group):
            text = ''
            current_box_group = [box for box in box_group if box[7]==i]
            mean_height = np.mean([box[5] for box in current_box_group])
            min_gx = min([box[1] for box in current_box_group])
            max_gx = max([box[2] for box in current_box_group])
            min_gy = min([box[3] for box in current_box_group])
            max_gy = max([box[4] for box in current_box_group])

            while len(current_box_group) > 0:
                highest = min([box[3] for box in current_box_group])
                candidates = [box for box in current_box_group if box[3]<highest+0.4*mean_height]
                # get the far left
                if mode == 'ltr':
                    most_left = min([box[1] for box in candidates])
                    for box in candidates:
                        if box[1] == most_left: best_box = box
                elif mode == 'rtl':
                    most_right = max([box[2] for box in candidates])
                    for box in candidates:
                        if box[2] == most_right: best_box = box

                if parragraph_jump:
                    letter_lenght = int(best_box[2]-best_box[1]/len(best_box[0]))
                    if min_gx <= best_box[1]<= min_gx + letter_lenght*2:
                        if last_box[3]> best_box[3]+mean_height*3:
                            best_box[0] = '\n\n'+best_box[0]
                        else:
                            best_box[0] = '\n'+best_box[0]
                if end_of_line:
                    if (best_box[3] - last_box[4]) >= int(mean_height*0.9):
                        if best_box[0][0] == "\n" :
                            best_box[0] = '\n' + best_box[0]
                        else:
                            best_box[0] = '\n\n' + best_box[0]

        
                if len(candidates) == 1:
                    if best_box[0][-1] in line_enders:
                        parragraph_jump = True
                    else:
                        parragraph_jump = False
                    end_of_line = True
                    last_box = best_box
                else:
                    parragraph_jump = False
                    end_of_line = False
                
                text += ' '+best_box[0]
                current_box_group.remove(best_box)
            result.append([ [[min_gx,min_gy],[max_gx,min_gy],[max_gx,max_gy],[min_gx,max_gy]], text[1:]])
        complete_text = ""
        for x in result:
            if x[1][-1] == ".":
                complete_text = complete_text + x[1].replace(" - ","").replace("- ","").replace(" -","")+"\n"
            else:
                complete_text = complete_text + x[1].replace(" - ","").replace("- ","").replace(" -","") + " "
        result = complete_text.replace(" - ","").replace("- ","").replace(" -","")
        return result








def ocr_process(w_preview, h_preview, x, y, w, h, n_page, path_pages, f_page,reader):
    #las preview de  H y W son de la imagen con un margen de 3 pixeles agregardos en los bordes
    #se debe corregir restando 6 pixeles antes de calcular el ratio y -3 para corregir la posicion

    try:
        w_preview = round(float(w_preview))-6
        h_preview = round(float(h_preview))-6
        x = float(x)
        y = float(y)
        w = float(w)
        h = float(h)
    except Exception as e:
        print("ERROR OBJETO INCOMPLETO {}".format(e))
        return -6

    n_page = "0_" + str(f"{int(n_page):03d}")
    url = path_pages+ '/' +n_page+f_page
    print("URL:", url)
    response = requests.get(url, verify=False)
    bytes_imagen= BytesIO(response.content)
    image_to_process = np.array(Image.open(bytes_imagen))
    dimenciones = image_to_process.shape
    w_original = dimenciones[1]
    h_original = dimenciones[0]
    w_ratio = float(w_original/w_preview)
    h_ratio = float(h_original/h_preview)
    left = round(x * w_ratio)-3
    top = round(y * h_ratio)+3
    right = round(x * w_ratio + w * w_ratio)
    bottom = round(y * h_ratio + h * h_ratio)
    try:
        image_crop = image_to_process[top:bottom , left:right]
        lineas = reader.readtext(image_crop)
        image_to_process = None
        image_crop = None
    except:
        print('ERROR EN PROCESO OCR')
        print('URL IMAGEN:',url)
        return -5
    lineas = get_paragraph(lineas)
    return (lineas, w_ratio)

#Var Globales
PATH_SRC_NEWS = "https://core.izimedia.io:10088/"
f_page = ".jpg"
reader =easyocr.Reader(['es'])





# sql = "SELECT * FROM ima_edition WHERE source_id = 14 and date_publication = '2022-07-16' and status =3 LIMIT 300;"
# sql = "SELECT * FROM ima_edition WHERE id = %s LIMIT 300;"
# value = 193023
id = ["979406"]

value = " , ".join(id)
value = "( " +value + " )"
sql = "SELECT * FROM ima_edition WHERE id IN "+value+" LIMIT 300;"
db = DBase(1, "PROD")
registros = db.instantQuery(sql)
# registros = db.instantQuery(sql,value)
for registro in registros:
    print(" "*90 + "id nota:"+ str(registro[0]))
    source_id = registro[1]
    pubdate = registro[2]
    ima_edition_object = demjson.decode(registro[7])
    w_preview = ima_edition_object["width_preview"]
    h_preview = ima_edition_object["height_preview"]
    path_pages = os.path.join(PATH_SRC_NEWS, str(pubdate).replace("-","/"), str(source_id))
    complete_text = ""
    primero = True
    for parraf in ima_edition_object["body"]:
        x = parraf["x"]
        y = parraf["y"]
        w = parraf["w"]
        h = parraf["h"]
        n_page = parraf["page"]

        complete_text +=  ocr_process(w_preview,h_preview,x,y,w,h,n_page,path_pages,f_page,reader)[0] + " "
    
    print(complete_text)
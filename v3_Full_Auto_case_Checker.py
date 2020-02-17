import json
import os
import threading
import time
import traceback
import subprocess
import sqlite3
import re

import_loc ={
             'CH_TEST': r'\\FIHEL2STDB026\glm\import', 'CH_UAT': r'\\DEFRA1SUDB016\glm\import', #CH
             'FR_TEST': r'\\FIHEL2STDB025\glm\import', 'FR_UAT': r'\\DEFRA1SUDB015\glm\import', #FR
             'FI_TEST': r'\\FIHEL2STDB027\glm\import', 'FI_UAT': r'\\DEFRA1SUDB017\glm\import', #FI
             'HU_TEST': r'\\FIHEL2STDB029\glm\import', 'HU_UAT': r'\\DEFRA1SUDB020\glm\import', #HU
             'SE_TEST': r'\\FIHEL2STDB028\glm\import', 'SE_UAT': r'\\DEFRA1SUDB018\glm\import', #SE
             'NO_TEST': r'\\FIHEL2STDB030\glm\import', 'NO_UAT': r'\\DEFRA1SUDB019\glm\import', #NO
             'DE_TEST': r'\\FIHEL2STDB032\glm\import', 'DE_UAT': r'\\DEFRA1SUDB022\glm\import', #DE
             'ES_TEST': r'\\FIHEL2STDB031\glm\import', 'ES_UAT': r'\\DEFRA1SUDB021\glm\import', #ES
             'CZ_TEST': r'\\Defra3stdb079\glm\import', 'CZ_UAT': r'\\DEFRA3SUDB064\glm\Import', #CZ
             'IE_TEST': r'\\Defra3stdb078\glm\Import', 'IE_UAT': r'\\DEFRA3SUDB063\glm\Import', #IE
             'LV_TEST': r'\\Defra3stdb080\glm\Import', 'LV_UAT': r'\\DEFRA3SUDB065\glm\Import', #LV
             'PT_TEST': r'\\Defra3stdb077\glm\Import', 'PT_UAT': r'\\DEFRA3SUDB062\glm\Import', #PT
             'BE_TEST': r'\\DEFRA3STDB082\glm\import', 'BE_UAT': r'\\DEFRA3SUDB067\glm\Import', #BE
             'AT_TEST': r'\\DEFRA3STDB083\glm\import', 'AT_UAT': r'\\DEFRA3SUDB068\glm\Import', #AT
             'NL_TEST': r'\\DEFRA3STDB084\glm\import', 'NL_UAT': r'\\DEFRA3SUDB069\glm\Import', #NL
             'IT_TEST': r'\\DEFRA3STDB085\glm\import', 'IT_UAT': r'\\DEFRA3SUDB070\glm\Import'  #IT 
            }

def update_database(user_data, json_content): #Failed orders are registered in the database

    conn = sqlite3.connect('users.db')
    curr = conn.cursor()
    for order in json_content:
        for num in range(len(json_content[order]['accref'])):
            if json_content[order]['accref'][num] is False:
                curr.execute('SELECT * FROM failed_orders WHERE username=?', (user_data[0],))
                if len(curr.fetchall()) == 0:
                    curr.execute('INSERT INTO failed_orders (username,order_nr,case_nr) VALUES(?,?,?);',(user_data[0], 0, 0))
                        
                curr.execute('SELECT order_nr,case_nr FROM failed_orders WHERE username=?', (user_data[0],))
                order_nr, case_nr = curr.fetchall()[0]
                order_nr += 1
                case_nr += json_content[order]['nr_cases'][num]
                curr.execute('UPDATE failed_orders SET case_nr=?,order_nr=? WHERE username=?', (case_nr, order_nr, user_data[0]))
                conn.commit()
    conn.close()

def stuck_in_import(user_data, import_stuck): #Continue to monitor order every hour

    email_execut_link = r'C:/Users/konygal/Desktop/Python notebook/Auto_case_Generator/Auto_case_gen_v3/OrderSendEmail_IMPORT.ahk'
    email = user_data[1]

    while len(import_stuck) != 0:

        for instance in import_stuck:
            email_body = ''
            print(import_loc[instance])
            if import_stuck[instance][0][1] not in os.listdir(import_loc[instance]):
                for email_text in import_stuck[instance]:                    
                    email_body += email_text[0]
                import_stuck[instance] = 'PASS'
                subprocess.run([email_execut_link, email, instance, email_body], shell=True) #Send email if order is processed
                print('Email Sent')
                
        to_remove = [instance for instance in import_stuck if import_stuck[instance] == 'PASS'] 
        for key in to_remove: del import_stuck[key] #Removes completed orders from order dictionary

        time.sleep(60 * 60)
        

def send_email(user_data, json_content, import_stuck): #Send an email to the user that submited the order

    def custom_sort(string): #Custom sorts by the order number
        return int(re.search(r'[\d]+', string).group())
    
    email_body = ['_'.join(text) for instance in json_content for text in json_content[instance]['accref_email']]
    email_body_sort = '_'.join(sorted(email_body, key=custom_sort))
    email = user_data[1]
    instance_name = user_data[2][0].split('_')[0]
    email_execut_link = r'C:/Users/konygal/Desktop/Python notebook/Auto_case_Generator/Auto_case_gen_v3/OrderSendEmail_PASS.ahk'
    subprocess.run([email_execut_link, email, instance_name, email_body_sort], shell=True) #Sends email
    print('Email Sent')
    update_database(user_data, json_content) #Updates database for failed orders
    if len(import_stuck) != 0: #Dictionary not empty continue to monitor order
        stuck_in_import(user_data, import_stuck)
        
    print('ORDER DONE') #Thread Finished


def check_file_flow(user_data, json_content): #Starts monitoring

    start_time_find = time.time() #starts time
    imp_file_name = set()
    in_import_check = []
    import_stuck = {}
    total_checks = len(user_data[2]) #amount of orders to monitor

    while total_checks != len(in_import_check):
        
        for imp_instance in json_content: 
            import_folder = os.listdir(import_loc[imp_instance])
            for file in import_folder:
                for acc_ref in range(len(json_content[imp_instance]['accref'])): #Checks for orders that are not yet processed
                    if json_content[imp_instance]['accref'][acc_ref] is not True and json_content[imp_instance]['accref'][acc_ref] != 'PASS' and json_content[imp_instance]['accref'][acc_ref] is not None:
                        if '_DEBT_ITEMS_' in file:
                            with open(f'{import_loc[imp_instance]}\\{file}', 'r', encoding='utf16') as imp_file:
                                data = imp_file.read()
                                if data.find(json_content[imp_instance]['accref'][acc_ref]) != -1: #Finds the correct order and adds it to {in_import_check} set
                                    imp_file_name.add(file)
                                    json_content[imp_instance]['accref'][acc_ref] = file
                                    print(f'Registered: {file}')

            for file_name in range(len(json_content[imp_instance]['accref'])): #Checkes which orders are processed and marks them as completed 
                
                if json_content[imp_instance]['accref'][file_name] not in import_folder and '_DEBT_ITEMS_' in json_content[imp_instance]['accref'][file_name]:
                    
                    json_content[imp_instance]['accref'][file_name] = 'PASS'
                    in_import_check.append('PASS')
                    print(f'{file_name}: Uploaded')
                                                     
        time.sleep(10)
        cycle_time_end = time.time() - start_time_find
        if cycle_time_end >= 60 * 30: #if the monitoring exceeds 30mins breaks from while loop
            break
            
    for imp_instance in json_content: #Modifies the email text to represent results
        for status in range(len(json_content[imp_instance]['accref'])): 
            if json_content[imp_instance]['accref'][status] == 'PASS':
                continue
            elif '_DEBT_ITEMS_' in json_content[imp_instance]['accref'][status]: #If order is in import, but not yet processed, adds it to continuous monitoring
                email_copy_status = '_'.join(json_content[imp_instance]['accref_email'][status].copy())
                json_content[imp_instance]['accref_email'][status].insert(1, 'Stuck in Import, you will be notified when cases are uploaded')
                if imp_instance not in import_stuck:
                    import_stuck[imp_instance] = [[email_copy_status, json_content[imp_instance]['accref'][status]]]
                else: import_stuck[imp_instance].append([email_copy_status, json_content[imp_instance]['accref'][status]])                
            else:
                json_content[imp_instance]['accref'][status] = False
                json_content[imp_instance]['accref_email'][status] = [json_content[imp_instance]['accref_email'][status][0], 'Failed to upload, please check instance status and try again later_ _']
            
    send_email(user_data, json_content, import_stuck) #Send an email to the user that submited the order


def one_thread(data): #One Thread for one order

    try:
    
        user_data = data['user'] #Splits dictionary into users data and order data
        json_content = data['order']
        check_file_flow(user_data, json_content) #Monitoring procedure for each order

    except:
        
        traceback.print_exc()

    

    




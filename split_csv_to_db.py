import concurrent.futures
import math
import sys
import pyodbc
import glob
import os
import csv
from datetime import datetime

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def main():
    path = r'C:\temp\db create stuff'
    print(datetime.now())
    dest = split_files(path)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        all_files = glob.glob(os.path.join(dest, "*.csv"))
        for filepath in zip(all_files, executor.map(db_insert, all_files)):
            # print(filepath)
            pass
    print(datetime.now())

#------------------------------------------------

def apply_to_list(some_list, f):
    return [f(x) for x in some_list]

#------------------------------------------------

def split_files(init_path):
    folder = init_path + '\\splits\\'
    csvfile = open(init_path + '\\customers-2000000.csv', 'r').readlines()# customers-2000000.csv
    file_count = 1
    for i in range(len(csvfile)):
        if i % 20000 == 0:
            open(str(folder + r'file_' + str(file_count)) + '.csv', 'w+').writelines(csvfile[i:i+20000])
            file_count += 1
    return folder

#------------------------------------------------

def db_insert(filename):
    #print(filename)
    connectionString = r"DRIVER={SQL SERVER};Server=localhost\SQLEXPRESS;Database=JM_STUFF;Trusted_Connection=True;MultipleActiveResultSets=true;"
    conn = pyodbc.connect(connectionString)

    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        if (filename == r"C:\temp\db create stuff\splits\file_1.csv"):
            headers = next(csv_reader)
        line_count = 0
        for line in csv_reader:
            line_count = line_count + 1
            (index, customer_id, first_name, last_name, company, city, country, phone_1, phone_2, email, subscription_date, website) = line
            #apos = (first_name, last_name, company, city, country, email, website)
            (first_name, last_name, company, city, country, email, website) = apply_to_list((first_name, last_name, company, city, country, email, website), lambda x:  x.replace("'", "''"))
            insert_stmt = f"INSERT INTO [dbo].[CUSTOMERS_STAGING]([CUSTOMER ID],[FIRST NAME],[LAST NAME],[COMPANY],[CITY],[COUNTRY],[PHONE 1],[PHONE 2],[EMAIL],[SUBSCRIPTION DATE],[WEBSITE]) VALUES ('{customer_id}','{first_name}','{last_name}','{company}','{city}','{country}','{phone_1}','{phone_2}','{email}','{subscription_date}','{website}');"
            #print(insert_stmt)
            try:
                conn.execute(insert_stmt)
                conn.commit()
            except pyodbc.Error as e:
                print("an error occurred:" + str(e))
                print(insert_stmt)
                conn.close()
                sys.exit()
    conn.close()

#------------------------------------------------

if __name__ == '__main__':
    main()


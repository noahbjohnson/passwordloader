import hashlib
import re
import os
import pymysql
import argparse
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())



# rds settings
rds_host = os.getenv("RDS_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_table = os.getenv("DB_TABLE")


def isMd5(datahere):
    validHash = re.finditer(r'(?=(\b[A-Fa-f0-9]{32}\b))', datahere)
    result = [match.group(1) for match in validHash]
    if result:
        return True
    else:
        return False


def file_to_array(file):
    filestring = file.read()
    array = []
    for i in filestring.split():
        try:
            i = i.decode()
            array.append(i.replace("'",""))
        except AttributeError:
            array.append(i.replace("'",""))
            pass

    # print(array)
    return array


def file_loader(filename):
    try:
        file = open(filename, 'r')
        return file
    except:
        print("file not found")


def add_to_db(passwords,sourcedesc):
    conn = pymysql.connect(rds_host, user=db_user, passwd=db_password, db=db_name, connect_timeout=5)

    """
    This function fetches content from mysql RDS instance
    """

    item_count = 0

    with conn.cursor() as cur:
        for password in passwords:
            item_count += 1
            if isMd5(password):
                md5 = password
                passwordstr = ""
            else:
                md5 = hashlib.md5(password.encode('utf-8')).hexdigest()
                passwordstr = password
            cur.execute("insert into {} values('{}', '{}', '{}')".format(db_table, md5, passwordstr, sourcedesc))
            # print(item_count, len(passwords))
            if item_count % 1000 == 0:
                print(item_count)
                conn.commit()
            elif item_count % 100 == 0:
                print(item_count)

        conn.commit()

    return "Added {} items to {}".format(item_count, db_table)


def main(filename, source_desc):
    file = file_loader(filename)
    passwords = file_to_array(file)
    print(add_to_db(passwords, source_desc))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='load newline delimited passwords to mysql database and compute md5 for '
                                                'cleartext')
    parser.add_argument('password_file', type=str,
                        help='The file containing passwords to load (1 per line)')
    parser.add_argument('description', type=str,
                        help='message for the source column')
    args = parser.parse_args()

    main(args.password_file, args.description)

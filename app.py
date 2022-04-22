import mysql.connector
from flask import Flask
from flask import request
import json
import boto3
from tabulate import tabulate

app = Flask(__name__)

def connectRDS():
    session = boto3.Session().client('secretsmanager', region_name='us-east-1')
    rdsSecrets = session.get_secret_value(SecretId='A3_RDS_Database')
    secrets = json.loads(rdsSecrets['SecretString'])
    conn = mysql.connector.connect(host=secrets['host'], user=secrets['username'],
                                   passwd=secrets['password'], port=secrets['port'],
                                   database=secrets['dbname'])

    return conn


@app.route('/storestudents', methods=['GET', 'POST'])
def storeStudents():
    try:
        json_output = json.loads(request.get_data())
        data = (json_output['students'])
        result = data

        rdsConnection = connectRDS()
        cursor = rdsConnection.cursor()

        studentsArray = []
        for index in range(len(result)):
            first_name = result[index]['first_name']
            last_name = result[index]['last_name']
            banner = result[index]['banner']

            studentsArray.append([first_name, last_name, banner])
        sqlInsertQuery = "INSERT INTO students (first_name, last_name, banner) VALUES (%s, %s, %s)"
        cursor.executemany(sqlInsertQuery, studentsArray)
        rdsConnection.commit()

        output_response = {"Rows affected": str(cursor.rowcount)}
        return output_response, 200

    except Exception as e:
        error = {'error_message': str(e)}
        return error, 400


@app.route('/liststudents', methods=['GET', 'POST'])
def listStudents():
    rdsConnection = connectRDS()
    cursor = rdsConnection.cursor()
    studentsArray = []
    cursor.execute('SELECT * from students')
    available_students = cursor.fetchall()
    print(available_students)
    for item in available_students:
        studentsArray.append({'first_name':item[0],'last_name':item[1],'banner':item[2]})
    output = tabulate(studentsArray, headers={'first_name': 'First Name', 'last_name': 'Last Name', 'banner': 'Banner'}, tablefmt='html')

    return output


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)

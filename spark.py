from pyspark import SparkContext
import re
from csv import writer


def parse(line):
    try:
        format_json = {
            "host": re.compile(r"[^\s]+").match(line).group(),
            "timestamp": re.compile(r"(\d{2})/(\w{3})/(\d{4})(:\d{2})+ (-\d{4}|\d{4})").search(line).group(),
            "request": re.compile(r"(\"\w+\s)(([\S]+)|(\s)).*\"").search(line).group(2),
            "statusCode": re.compile(r"\"\s+(\d{3})").search(line).group(1),
            'bytes': re.compile(r"(\d+)|(-)$").search(line).group()
        }

    except Exception as error:
        # Catch an invalid format line so return a blanck schema
        # with the original line data
        format_json = {
            "host": '',
            "timestamp": '',
            "request": '',
            "statusCode": '',
            'bytes': '',
            'line': line
        }
    return format_json


if __name__ == "__main__":
    # setting default memory to solve outOfMemory problem
    # at version 2.4.x in Windows plataform
    SparkContext.setSystemProperty("spark.executor.memory", "4g")

    # Getting SparkContext
    sc = SparkContext("local", "NASA")

    # Getting data
    rddAug = sc.textFile("./files/access_log_Aug95")
    rddJul = sc.textFile("./files/access_log_Jul95")

    # Union a parse data
    rddData = rddJul + rddAug
    rddDataParsed = rddData.map(parse)


    # 1. Número de hosts únicos

    rddHosts = rddDataParsed.map(
        lambda line: line['host'].lower())

    with open('./results/result_1.csv', 'w+') as csvFile:

        writer(csvFile).writerow(
            [rddHosts.distinct().count()])


    # 2. O total de erro 404.

    rddTotal404 = rddDataParsed.map(
        lambda line: line['statusCode'])

    rddTotal404 = rddTotal404.filter(
        lambda status: status == "404")

    with open('./results/result_2.csv', 'w+') as csvFile:

        writer(csvFile).writerow([rddTotal404.count()])


    # 3. Top 5 urls erro 404.
    
    # Map our target data, if not 404 puts '' in request key
    rddTop_5_404 = rddDataParsed.map(
        lambda line: (line['request'], 1) \
        if line['statusCode'] == "404" else ('', 1))
    
    # Removing invalid data
    rddTop_5_404 = rddTop_5_404.filter(
        lambda url: url[0] != '')
    
    rddTop_5_404 = rddTop_5_404.reduceByKey(
        lambda x, y: x + y)

    with open('./results/result_3.csv', 'w+') as csvFile:

        writer(csvFile).writerows(
            # set key to order, ordering by value desc
            rddTop_5_404.takeOrdered(5, key = lambda url: -url[1]))


    # 4. Total de erros 404  por dia.

    # Map our target data, if not 404 puts '' in timestamp key
    rddDays404 = rddDataParsed.map(
        lambda line: (line['timestamp'][:11], 1) \
        if line['statusCode'] == "404" else ('', 1))

    # Removing invalid data
    rddDays404 = rddDays404.filter(
        lambda url: url[0] != '')

    rddDays404 = rddDays404.reduceByKey(
        lambda x, y: x + y)

    with open('./results/result_4.csv', 'w+') as csvFile:
        
        writer(csvFile).writerows(rddDays404.collect())


    # 5. Total de bytes retornado

    rddSumBytes = rddDataParsed.map(
        lambda line: int(line['bytes']) \
        if line['bytes'].isdigit() else 0)

    with open('./results/result_5.csv', 'w+') as csvFile:
        
        writer(csvFile).writerow([rddSumBytes.sum()])

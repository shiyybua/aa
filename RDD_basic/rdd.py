# -*- coding: utf-8 -*-
from pyspark import SparkContext

def convert_type(string):
    if type(string) == unicode:
        return str(string)
    return string

path = "/Users/mac/software/spark-2.1/README.md"
#创建sc
sc = SparkContext('local', 'app name')

def filter_t():
    lines = sc.textFile(path)
    # 计算总行数。
    print lines.count()
    '''
        调用count()、collect()这种"行动操作"时，整个RDD会从头开始计算。要回避这种抵消的行为，我们可以将中间结果持久化。
    '''
    # 用take，只取前10行。
    for i, line in enumerate(lines.take(10)):
        print i, line

    # collect()转换RDD为：
    print lines.collect()
    print lines.first()
    # 只取含有'Spark'的内容
    lines = lines.filter(lambda x: 'Spark' in x)
    print lines.collect()
    # print filter(convert_type(lines.first()), lambda x: 'Spark' not in x)

def union():
    first = sc.parallelize(['I','Love','Spark'])
    second = sc.parallelize(['how','about','you'])
    merge = first.union(second)
    print merge.collect()


def map_op():
    num = sc.parallelize([1,2,3,4])
    # 和python自带的map不同的是，num是个RDD，不是iterable的。 所以不能直接用自带的map.
    squared = num.map(lambda x: x ** x)
    print squared.collect()


def flatmap_op():
    '''
       flatMap(): 对输入的每个元素生成多个输出元素。
       从结果可以看出和map()的区别。
       flatMap 返回的都是一维的数组。相当于被"拍平"
    :return:
    '''
    line = sc.parallelize(['Hello hi','world ysc','! 55'])
    line = line.flatMap(lambda l: l.split(' '))
    print 'flatMap', line.collect()

    line = sc.parallelize(['Hello hi', 'world ysc', '! 55'])
    line = line.map(lambda l: l.split(' '))
    print 'map', line.collect()

def action_op():
    num = sc.parallelize(range(10))
    # 返回的不是一个RDD。
    sum = num.reduce(lambda x, y: x + y)
    print type(sum), sum

    # fold和reduce一样，不同的是结果要加一个初始值。
    sum = num.fold(100, lambda x, y: x + y)
    print sum



action_op()
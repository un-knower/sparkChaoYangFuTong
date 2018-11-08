#!/usr/bin/env python
# -*- coding: utf-8 -*-


#=============================================================2
"""gcs:\n
#在类外定义了一个函数。这个函数就是静态函数。
这个静态函数可以在这个str.py的文件，即namespace 中被调用。不需要跟着类或者对象，就可以直接地被调用
"""
def foo(x):
    print "executing foo(%s)"%(x)

class A(object):
    def foo(self,x):  #在类内定义的函数，是成员函数
        print "executing foo(%s,%s)"%(self,x)

    #=============================================================3
    """gcs:\n
    这是定义了一个类的成员函数。在调用的时候先需要创建出一个类的实例对象。之后用实例对象调用成员函数
    """
    @classmethod
    def class_foo(cls,x):
        print "executing class_foo(%s,%s)"%(cls,x)

    #=============================================================1
    """gcs:\n
    所谓的“类方法”，其实就是被类的对象所共有的方法。直接通过 类名.静态方法 的方式就可以调用
    静态方法的定义，只需要在成员方法的前面加上注解就可以了
    """
    @staticmethod
    def static_foo(x):
        print "executing static_foo(%s)"%x

if __name__ == '__main__':
    from urllib.request import urlopen

    html = urlopen("https://www.baidu.com/")
    print(html.read())
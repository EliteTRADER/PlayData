'''
Created on Jul 9, 2015

@author: shaunz
'''

from influxdb.influxdb08 import InfluxDBClient

class InfluxDB(object):
    
    def __init__(self, db_name=None):
        self.db = InfluxDBClient('localhost', 8086, 'root', 'root')
        if(db_name != None):
            self.db_name = db_name
            self.db.switch_database(db_name)
            
    def query(self,series_name,db_name=None):
        if(db_name != None):
            self.db.switch_database(db_name)
            return self.db.query('SELECT * FROM %s' % series_name)
        else:
            return self.db.query('SELECT * FROM %s' % series_name)
        
    def _break_expression(self, expression):
        '''
        Break the expression into logical components
        ------
        expression: str
            the expression to break down
        ------
        '''
        operators = ['^','+','-','*','/','(',')']
        results = []
        current_expression = ''
        for char in expression:
            if(char in operators):
                results.append(current_expression)
                results.append(char)
                current_expression = ''
            else:
                current_expression = current_expression + char
        results.append(current_expression)
        results[:] = [item for item in results if item != '']
        
        return results
    
    def interpret(self,expression,db_name=None):
        '''
        Interpret an expression
        '''
        results = self._break_expression(expression)
        print results
        return True    

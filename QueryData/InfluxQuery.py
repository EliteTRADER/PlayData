'''
Created on Jul 9, 2015

@author: shaunz
'''

from influxdb.influxdb08 import DataFrameClient

class InfluxDB(object):
    
    def __init__(self, db_name=None):
        self.db = DataFrameClient('localhost', 8086, 'root', 'root')
        if(db_name != None):
            self.db_name = db_name
            self.db.switch_database(db_name)
            
    def query(self,series_name,db_name=None):
        '''
        Query a particular series
        ------
        series_name: str
            name of the series, e.g. "CPI_US"
        ------
        return a pandas DataFrame
        ------
        '''
        if(db_name != None):
            self.db.switch_database(db_name)
            results = self.db.query('SELECT * FROM %s' % series_name)
        else:
            results = self.db.query('SELECT * FROM %s' % series_name)
        return results
    
    def _is_num(self, s):
        '''
        Determine if a string is a number
        '''
        try:
            float(s)
            return True
        except ValueError:
            return False
    
    def _include(self,expression_list,c):
        '''
        check whether expression_list include basic operator c, and give the index of first occurrence
        ------
        c: str
            the basic operator or parentheses c
        ------
        '''
        for index, item in enumerate(expression_list):
            if(type(item)==type('string')):
                if(item == c):
                    return index
        
        return -1
            
        
    def _get_index(self,expression_list, op_1, op_2):
        '''
        Get the index of first occurrence of op_1 OR op_2, assume expression_list includes op_1 OR op_2
        '''
        index_1 = self._include(expression_list, op_1)
        index_2 = self._include(expression_list, op_2)
        
        if(min(index_1,index_2)==-1):
            return(max(index_1,index_2))
        else:
            return(min(index_1,index_2))
        
    def _break_expression(self, expression):
        '''
        Break the expression into logical components
        '''
        operators = ['^','+','-','*','/','(',')']
        expression = expression.replace(' ','')
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
        
        converted_results = []
        for item in results:
            if item in operators:
                converted_results.append(item)
            elif self._is_num(item):
                converted_results.append(float(item))
            else:
                converted_results.append(self.query(item))
            
        return converted_results

    def _calculate(self, expression_list):
        '''
        Calculate a list of expression elements
        '''
        e_list = expression_list
        if(len(e_list)==1):
            return e_list
        
        if(self._include(e_list, '^') != -1):
            position = self._include(e_list, '^')
            eval_result = pow(e_list[position-1],e_list[position+1])
            return self._calculate(e_list[:position-1] + [eval_result] + e_list[position+2:])
        elif(self._include(e_list, '*') != -1 or self._include(e_list, '/') != -1):
            position = self._get_index(e_list, '*', '/')
            if(e_list[position]=='*'):
                eval_result = e_list[position-1] * e_list[position+1]
            else:
                eval_result = e_list[position-1] / e_list[position+1]
            return self._calculate(e_list[:position-1] + [eval_result] + e_list[position+2:])
        elif(self._include(e_list, '+') != -1 or self._include(e_list, '-') != -1):
            position = self._get_index(e_list, '+', '-')
            if(e_list[position]=='+'):
                eval_result = e_list[position-1] + e_list[position+1]
            else:
                eval_result = e_list[position-1] - e_list[position+1]
            return self._calculate(e_list[:position-1] + [eval_result] + e_list[position+2:])
        else:
            raise ValueError('cannot recognize operators in the expression')
       
    def _parentheses(self, expression_list):
        '''
        Iterate through parentheses, always interpret the first closing parentheses )
        '''
        e_list = expression_list  
        if(self._include(e_list, '(') != -1 and self._include(e_list, ')') != -1):
            close_index = self._include(e_list, ')')
            sub = e_list[:close_index][::-1]
            open_index = close_index - self._include(sub, '(') - 1
            eval_result = self._calculate(e_list[(open_index+1):close_index])
            new_list = e_list[:open_index] + eval_result + e_list[close_index+1:]
            return(self._parentheses(new_list))
        else:
            if(self._include(e_list, '(') != -1 or self._include(e_list, ')') != -1):
                raise ValueError('unmatched parentheses, check the expression')
            else:
                return(self._calculate(e_list))
    
    def interpret(self,expression):
        '''
        Interpret an expression
        '''
        interp_expression = self._break_expression(expression)
        results = self._parentheses(interp_expression)
        return results

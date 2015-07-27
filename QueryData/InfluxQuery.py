'''
Created on Jul 9, 2015

@author: shaunz
'''

from influxdb.influxdb08 import DataFrameClient
from pandas.core.common import isnull

class InfluxDB(object):
    '''
    Connect to influxdb and pull/write data
    '''
    def __init__(self, db_name=None):
        self.url = 'localhost'
        self.port = 8086
        self.user = 'root'
        self.password = 'root'
        self.db_list = [ 'FRED', 'Quandl', 'Econ' ]
        
        self.db = DataFrameClient(self.url, self.port, self.user, self.password)
        if(db_name != None):
            self.db_name = db_name
        self.db.switch_database(db_name)
        
    
    def _search_db(self,series_name):
        '''
        Search the db name for a series name
        '''
        for db in self.db_list:
            temp_db = DataFrameClient(self.url, self.port, self.user, self.password, db)
            if series_name in temp_db.get_list_series():
                return db
        
        return None
            
    def query(self,series_name,db_name=None):
        '''
        Query a particular series
        ------
        series_name: str
            name of the series, e.g. "CPI_US"
        ------
        return a pandas DataFrame with NaN representing missing values
        ------
        '''
        if(db_name != None):
            self.db.switch_database(db_name)
            results = self.db.query('SELECT * FROM %s' % series_name)
        else:
            db_name = self._search_db(series_name)
            self.db.switch_database(db_name)
            results = self.db.query('SELECT * FROM %s' % series_name)       
        
        if(results['value'].str.contains('.').isnull().sum()!=len(results)):
            results.loc[results['value']=='.','value'] = None
        
        return results.astype(float)
    
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
    
    def _close_parentheses(self,expression):
        '''
        Find the closing parentheses to the first opening (
        ------
        expression: str
            str contains an opening (
        ------
        '''
        layer = 0
        for i, char in enumerate(expression):
            if(char=='('):
                layer = layer + 1
            elif(char==')'):
                layer = layer - 1
                if(layer == 0):
                    return i
        return -1
            
    def _break_expression(self, expression, operators, functions):
        '''
        Break the expression into logical components
        '''
        expression = expression.replace(' ','')
        #empty string
        if(len(expression)==0):
            return [expression]
        
        # interpret functions
        for func in functions:
            if func in expression:
                func_start = expression.find(func)
                func_end = func_start + self._close_parentheses(expression[func_start:])
                if(expression[func_start-1] in operators):
                    return (self._break_expression(expression[:func_start], operators, functions)
                            +[expression[func_start:func_end+1]]
                            +self._break_expression(expression[func_end+1:], operators, functions))
                
        # then deal with the time series, operators and numbers
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
    
    def _eval_func(self, func):
        '''
        Evaluate the function
        '''
        function = func[:func.index('(')]
        if 'lag' == function: #lag the time series by a number of periods. lag(*series*,i) where i is number of period
            index_start = func.index('(')
            index_mid = len(func)-func[::-1].index(',')-1
            index_end = len(func)-func[::-1].index(')')-1
            try:
                series = self.query(func[index_start+1:index_mid])
            except:
                series = self.interpret(func[index_start+1:index_mid])
            periods = float(func[index_mid+1:index_end])
            return series.shift(periods)
        elif 'mlag' == function: # shift the time stamp by a number of months. mlag(*series*,i) where i is number of months
            index_start = func.index('(')
            index_mid = len(func)-func[::-1].index(',')-1
            index_end = len(func)-func[::-1].index(')')-1
            try:
                series = self.query(func[index_start+1:index_mid])
            except:
                series = self.interpret(func[index_start+1:index_mid])
            periods = float(func[index_mid+1:index_end])
            return series.tshift(periods,freq='M').tshift(1,freq='D')
        elif 'avg' == function: #taking the average of time series
            index_start = func.index('(')
            index_mid = len(func)-func[::-1].index(',')-1
            index_end = len(func)-func[::-1].index(')')-1
            try:
                series = self.query(func[index_start+1:index_mid])
            except:
                series = self.interpret(func[index_start+1:index_mid])
            freq = func[index_mid+1:index_end]
            series = series.resample(freq, how='mean')
            if 'M' in freq:
                return series.tshift(-1,freq='M').tshift(1,freq='D')
            else:
                return series
        else:
            message = '%s not defined' % func
            raise ValueError(message)
        
        
    def _convert_expressions(self, expression, operators):
        '''
        Convert expressions to data series after they are broken down
        '''
        expression[:] = [item for item in expression if item != '']
        converted_results = []
        for item in expression:
            if '(' in item and ')' in item:
                converted_results.append(self._eval_func(item))
            elif item in operators:
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
    
    def interpret(self, expression):
        '''
        Interpret an expression
        '''
        operators = ['^','+','-','*','/','(',')']
        functions = ['lag', 'mlag', 'avg']
        broken_expression = self._break_expression(expression, operators, functions)
        interp_expression = self._convert_expressions(broken_expression, operators)
        results = self._parentheses(interp_expression)[0]
        
        results = results.loc[isnull(results['value']) != True]
        return results
    
import MySQLdb

from src.settings import DATABASE

class MySQL:
    def __init__(self,):
        self.connection=MySQLdb.connect(user=DATABASE.get('USER'),passwd=DATABASE.get('PASSWORD'),
                                db=DATABASE.get('NAME'),host=DATABASE.get('HOST'),port=DATABASE.get('PORT')
                            )
        self.cursor=None

      
    def execute(self,sql,params=None,many=None):
        self.cursor=self.connection.cursor(MySQLdb.cursors.DictCursor)
        if many:
            self.cursor.executemany(sql,params)
        else:
            self.cursor.execute(sql,params)

        return self

   
    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()
            

    def fetchall(self):
        results=self.cursor.fetchall()
        self.cursor.close()
        return results

    def fetchone(self):
        result=self.cursor.fetchone()
        self.cursor.close()
        return result
    
    def close(self):
        #close db connection
        self.connection.close()
    

class DB:
    """Main class accessed and used by models , e.t.c """

    FILTER_COMMANDS={
        "eq":"=",
        "neq":"!=",
        "lt":"<",
        "lte":"<=",
        "gt":">",
        "gte":">=",
        "co":"LIKE",#like %var%
        "nco":"NOT LIKE",#
        "sw":"LIKE",#starts with . like %var
        "ew":"LIKE"# endswith like var%
    }

    def __init__(self,):
        #create mysql object
        self._mysql=MySQL()
        self._table_name=None
        self.columns=None
      
        self.filter_values=None
        self.query=None
       
    
    def table(self,table_name):
        #should be called first before others 
        self._table_name=table_name
        #reset previous records
        self.columns=None
       
        self.filter_values=None

        self.query=None
        return self

    
    def __select(self,columns):
        """
        Accepts:

        columns      :=     string comma separete of colums to select
        
        """
        self.columns=columns

        #form colum hodlers
        #col_holders=','.join(['%s'] *len(self.columns))
        self.query="SELECT {} FROM {} ".format(self.columns,self._table_name)

        return self



    def set_filter_values(self,filter_values):
        """ sets filter values directly """
        self.filter_values=filter_values
        return None

    def __get_query_condition(self,filter_command):
        fc=self.FILTER_COMMANDS.get(filter_command)

        
        return "{} %s ".format(fc)
        
       
        

    def __format_filter_val(self,filter_command,param):
        
        if filter_command in ['co','nco']:
            return " {} ".format(("%" + param + "%"))
        elif filter_command == 'sw':
            return " {} ".format(("%" + param))
        elif filter_command == 'ew':
            return " {} ".format(( param + "%"))
        else:
            return param


    def __where(self,filter_data):
        
        """ e.g {
            "and":[{"id":{"eq":20}},{"odds_status":{"gt":0}}],
            "or":[]
              }
        """

        if not filter_data:
            return self

        and_params=filter_data.get("and",[])
        or_params=filter_data.get("or",[])

        and_filters=[[],[]] #place holders and values
        or_filters=[[],[]]


        #process ands
        for a in and_params:
            for k,v in a.items():
                for ck,cv in v.items():
                    and_filters[0].append(" {} {} ".format(k,self.__get_query_condition(ck)))
                    and_filters[1].append(self.__format_filter_val(ck,cv))
        
        #process ors
        for o in or_params:
            for k,v in o.items():
                for ck,cv in v.items():
                    or_filters[0].append(" {} {} ".format(k,self.__get_query_condition(ck)))
                    or_filters[1].append(self.__format_filter_val(ck,cv))
        

        or_filters_sql=' OR '.join(v for v in or_filters[0])
        and_filters_sql=' AND '.join(v for v in and_filters[0])

        filters_sql=and_filters_sql + ' OR ' + or_filters_sql

        filter_vals=and_filters[1] + or_filters[1]

        print (filters_sql,filter_vals)



        

        self.set_filter_values(filter_vals)


        self.query= self.query +  " WHERE {} ".format(filters_sql)

        return self

    def __execute(self,query,values=None):
        #runs query
        return self._mysql.execute(sql=query,params=values)


    def __limit(self,total_rows):
        """ Sepecify rows to limit in the selection . """
      
        limit= " LIMIT {} ".format(total_rows)
        self.query=self.query + limit
        return self


    def commit(self):
        self._mysql.commit()
    
    def rollback(self):
        self._mysql.rollback()

    def close(self):
        self._mysql.close()


    def select_one(self,columns,filter_data=None):
        self.__select(columns)
        #if filter data append where clause
        
        self.__where(filter_data)
        #run query
        return self.__execute(self.query,self.filter_values).fetchone()

    
    def select_many(self,columns,filter_data=None,limit=None):
        self.__select(columns)
        self.__where(filter_data)

        if limit:
            self.__limit(limit)

        print (self.query,self.filter_values)

        #run query
        return self.__execute(self.query,self.filter_values).fetchall()



    def insert_one(self,data):
        """ put record to db. """

        col_holders=','.join(['%s'] * len(data.items()))
        col_names=','.join([k for k,v in  data.items()])

        col_values=[v for k,v in  data.items()]


        self.query="INSERT INTO {} ({}) VALUES({}) ".format(self._table_name,col_names,col_holders)

        print (col_values)
        print (self.query)

        result=self.__execute(self.query,col_values)
        if self._mysql.cursor.rowcount > 0:
            return self._mysql.cursor.lastrowid
        return None

    
    def update(self,update_data,filter_data):
       
        col_set=','.join([" {} = %s ".format(k) for k,v in  update_data.items()])

        col_values=[v for k,v in  update_data.items()]

        self.query="UPDATE {} SET {}  ".format(self._table_name,col_set)

        #append where clause to the query 
        self.__where(filter_data)

        #lets appedn filter values/params from update_data dict 

        col_values.extend(self.filter_values)


        print (self.query)
        print (col_values)

        #run
        result=self.__execute(self.query,col_values)
       
        return True


    def delete(self,filter_data):
        self.query="DELETE FROM {} ".format(self._table_name)
        self.__where(filter_data)

        print (self.query)
        print(self.filter_values)
    
        self.__execute(self.query,self.filter_values)

        return True

    def raw(self,sql,params=None,many=False):
        """ This is for executing raw query
        
        Returns mysl object directly. can cll commit,rollback,fetchone,fetchall methods directly

        """

        return self._mysql.execute(sql,params,many)

        


    
    
    
    
    
    
    

        

    


            






        



    




        

         

          
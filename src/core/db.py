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

    def make_filter_values(self,filter_data_list):
        """ makes filter values from query filter data list """
        """ this are params """


        filter_values=[]
      
        for d in filter_data_list:
            for k,v in d.items():
                for condition,value  in v.items():
                    filter_values.append(value)

        return filter_values


    def set_filter_values(self,filter_values):
        """ sets filter values directly """
        self.filter_values=filter_values
        return None


        
    def __where(self,filter_data_list):
        
        """ constructs the where clause of comparison 
        filter_data_list         :=    List of Query filter params dictionary . e.g [{"id":{"<=":251}}]
                            for where the list has more than one dict, they will be joined by a "OR" clause , but in dict data will be joined by "AND"
                            Filter key words are as per mysql filter keywords.
                            e.g < , > , >= ,<= , != , = , e.t.c
        If none of the fields is provided, empty select will be run.
        """
        if not filter_data_list:
            return self
            
        self.set_filter_values(self.make_filter_values(filter_data_list))

        filter_cols=[]
        for d in filter_data_list:
            for k,v in d.items():
                
                for condition,value  in v.items():
                    filter_cols.append("{} {} %s ".format(k,condition))
                   

        self.query= self.query +  " WHERE {} ".format(','.join(filter_cols))
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


    def select_one(self,columns,filter_data_list=None):
        self.__select(columns)
        #if filter data append where clause
        
        self.__where(filter_data_list)
        #run query
        return self.__execute(self.query,self.filter_values).fetchone()

    
    def select_many(self,columns,filter_data_list=None,limit=None):
        self.__select(columns)
        self.__where(filter_data_list)

        if limit:
            self.__limit(limit)


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

    
    def update(self,update_data,filter_data_list):
       
        col_set=','.join([" {} = %s ".format(k) for k,v in  update_data.items()])

        col_values=[v for k,v in  update_data.items()]

        self.query="UPDATE {} SET {}  ".format(self._table_name,col_set)

        #append where clause to the query 
        self.__where(filter_data_list)

        #lets appedn filter values/params from update_data dict 

        col_values.extend(self.filter_values)


        print (self.query)
        print (col_values)

        #run
        result=self.__execute(self.query,col_values)
       
        return True


    def delete(self,filter_data_list):
        self.query="DELETE FROM {} ".format(self._table_name)
        self.__where(filter_data_list)

        print (self.query)
        print(self.filter_values)
    
        self.__execute(self.query,self.filter_values)

        return True


    
    
    
    
    
    
    

        

    


            






        



    




        

         

          
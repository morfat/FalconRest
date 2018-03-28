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
        self.filter_data=None
        self.limit_rows=None
        self.query=None
        self.filter_values=None

        



    def table(self,table_name):
        #should be called first before others 
        self._table_name=table_name
        #reset previous records
        self.columns=None
        self.filter_data_list=None
        self.limit_rows=None
        self.filter_values=None
        


        return self

    
    def select(self,columns=None):

        """
        Accepts:

        columns      :=     string comma separete of colums to select
        
        """
        self.columns='*' if not columns else columns
        #form colum hodlers
        #col_holders=','.join(['%s'] *len(self.columns))
        self.query="SELECT {} FROM {} ".format(self.columns,self._table_name)
        return self

    def insert(self,data):
        """ put records to db. """

        col_holders=','.join(['%s'] * len(data.items()))
        col_names=','.join([k for k,v in  data.items()])

        col_values=[v for k,v in  data.items()]


        self.query="INSERT INTO {} ({}) VALUES({}) ".format(self._table_name,col_names,col_holders)

        print (col_values)
        print (self.query)


        result=self._mysql.execute(self.query,col_values)
        if self._mysql.cursor.rowcount > 0:
            return self._mysql.cursor.lastrowid
        return None
    

    def where(self,filter_data_list):
        
        """ constructs the where clause of comparison 
        filter_data_list         :=    List of Query filter params dictionary . e.g [{"id":{"<=":251}}]
                            for where the list has more than one dict, they will be joined by a "OR" clause , but in dict data will be joined by "AND"
                            Filter key words are as per mysql filter keywords.
                            e.g < , > , >= ,<= , != , = , e.t.c
        If none of the fields is provided, empty select will be run.
        """

        self.filter_values=[]
        self.filter_data_list=filter_data_list
        filter_cols=[]

        for d in filter_data_list:
            for k,v in d.items():
                
                for condition,value  in v.items():
                    filter_cols.append("{} {} %s ".format(k,condition))
                    self.filter_values.append(value)

        self.query= self.query +  " WHERE {} ".format(','.join(filter_cols))
        return self



    def limit(self,total_rows):
        """ Sepecify rows to limit in the selection . """
        self.limit_rows=total_rows
        limit= " LIMIT {} ".format(self.limit_rows)
        self.query=self.query + limit
        return self
    
    @property
    def result(self):
        """ Call this if you need to select one result . Last Called """
        return self._mysql.execute(self.query,(self.filter_values,)).fetchone()


    @property
    def results(self):
        """ Query this if you expect to return more than one row. Last called. """
        return self._mysql.execute(self.query,(self.filter_values,)).fetchall()


    
    
    

        

    


            






        



    




        

         

          
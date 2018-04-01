
from src.settings import PAGINATION 

from urllib import parse

#from collections import OrderedDict


class Pagination:
    def __init__(self,url=None,page_number=None,page_size=None,last_seen=None,last_seen_field_name=None,direction=None):
        self.page_number=int(page_number) if page_number else 1
        self.page_size=int(page_size) if page_size else PAGINATION.get('page_size')
        self.direction=direction
        self.last_seen=last_seen
        self.last_seen_field_name =  last_seen_field_name if last_seen_field_name else PAGINATION.get('last_seen_field_name')
        self.url=url

       
    
    def modify_order_by(self,order_by,pagination_order_by):
      
        #just update order by
        if order_by:
            pagination_order_by.extend(order_by)

        return pagination_order_by
            
    def paginate(self,order_by):
        
        """  Returns a sql appended with a where clause for comparison i.e < or >  and updated order_by query data 
             Should be called immediatedly after calling where .
        """
        
        where_clause=None

        if self.page_number == 1:
            #this is initial. access.
            order_by=self.modify_order_by(order_by,["-{}".format(self.last_seen_field_name)]) #order descending


        elif self.direction == 'prev':
            order_by=self.modify_order_by(order_by,["{}".format(self.last_seen_field_name)])   #order ascending
            where_clause="{} > {} ".format(self.last_seen_field_name,self.last_seen)

        elif self.direction == 'next':
            order_by=self.modify_order_by(order_by,["-{}".format(self.last_seen_field_name)])
            where_clause="{} < {} ".format(self.last_seen_field_name,self.last_seen)

        return (order_by,where_clause,)


  
    def get_next_link(self,results_list):
        page = self.page_number + 1
        url = self.url


        if len(results_list) < self.page_size:
            return None
            
        if self.direction == 'prev' and  page != 2:
            last_seen_dict = results_list[:-1][0]
        else:
            last_seen_dict = results_list[-1:][0]
            

        url=self.replace_query_param(url, 'page', page)
        url=self.replace_query_param(url, 'dir', 'next')
        url=self.replace_query_param(url, 'last_seen', last_seen_dict.get(self.last_seen_field_name))

        return url


    def get_previous_link(self,results_list):
        page=self.page_number - 1
        url=self.url

        if page == 0:
            return None

        elif len(results_list) == 0:
            #return home link
            url=self.remove_query_param(url, 'page')
            url=self.remove_query_param(url, 'dir')
            url=self.remove_query_param(url, 'last_seen')
            return url
        last_seen_dict = results_list[-1:][0]
        
    
        url=self.replace_query_param(url, 'page', page)
        url=self.replace_query_param(url, 'dir', 'prev')
        url=self.replace_query_param(url, 'last_seen', last_seen_dict.get(self.last_seen_field_name))

        
        return url

    

    def replace_query_param(self,url, key, val):
        """
        Given a URL and a key/val pair, set or replace an item in the query
        parameters of the URL, and return the new URL.
        """
        (scheme, netloc, path, query, fragment) = parse.urlsplit(url)
        query_dict = parse.parse_qs(query, keep_blank_values=True)
        query_dict[str(key)] = [val]
        query = parse.urlencode(sorted(list(query_dict.items())), doseq=True)
        return parse.urlunsplit((scheme, netloc, path, query, fragment))

    def remove_query_param(self,url, key):
        """
        Given a URL and a key/val pair, remove an item in the query
        parameters of the URL, and return the new URL.
        """
        (scheme, netloc, path, query, fragment) = parse.urlsplit(url)
        query_dict = parse.parse_qs(query, keep_blank_values=True)
        query_dict.pop(key, None)
        query = parse.urlencode(sorted(list(query_dict.items())), doseq=True)
        return parse.urlunsplit((scheme, netloc, path, query, fragment))

   
    def get_pagination_data(self,results_list):
        return {'page_size':self.page_size,
                'next_url': self.get_next_link(results_list),
                'previous_url': self.get_previous_link(results_list)
               }

                            
                

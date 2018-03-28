import falcon

from src.core.db import DB

class UserListCreateView:
   
    def on_post(self,req,resp):
        req_data=req.media

        db=DB()
        rowid=db.table('users').insert(req_data)
        print (rowid)
        



        resp.media={}

    
    def on_get(self,req,resp):
        #connect
        db=DB()

        results=db.table('users').select("id,odds_status,phone,country_iso_code").where([{"id":{"<=":50}}]).limit(20).results

        resp.media=[r for r in results]




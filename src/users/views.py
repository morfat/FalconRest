import falcon

from src.core.db import DB

class UserListCreateView:
   
    def on_post(self,req,resp):
        req_data=req.media

        db=DB()
        rowid=db.table('users').insert_one(req_data)
        print (rowid)

        #db.commit()
       


        #update
        updated=db.table('users').update({"odds_status":0,"country_iso_code":"mo"},filter_data_list=[
            {"id":{"=":rowid}}
            
            ])
        db.commit()


        resp.media={}

    
    def on_get(self,req,resp):
        #connect
        db=DB()

        results=db.table('users').select_many("id,odds_status,phone,country_iso_code",
        filter_data= {
            #"and":[{"id":{"eq":20}},{"odds_status":{"gt":0}}],
            "or":[{"id":{"lte":22}},{"country_iso_code":{"co":"k"}}]
        })

        #deleted=db.table('users').delete([{"id":{"=":41}}])

        #db.commit()

        resp.media=[r for r in results]



